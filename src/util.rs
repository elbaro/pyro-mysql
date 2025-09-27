use std::future::Future;

use futures::future::{AbortHandle, Abortable};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

pub fn mysql_error_to_pyerr(error: mysql_async::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyException, _>(format!("MySQL Error: {}", error))
}

pub fn url_error_to_pyerr(error: mysql_async::UrlError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyException, _>(format!("MySQL Error: {}", error))
}

/// A wrapper around a Python future that cancels the associated Rust future when dropped.
#[pyclass]
pub struct PyroFuture {
    py_future: Py<PyAny>,
    abort_handle: AbortHandle,
}

/// Iterator wrapper that keeps RaiiFuture alive during iteration
#[pyclass]
struct PyroFutureIterator {
    iterator: Py<PyAny>,
    _future: Py<PyroFuture>, // Keep the future alive
}

#[pymethods]
impl PyroFutureIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.iterator.bind(py).call_method0("__next__")
    }

    fn send<'py>(&self, py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        self.iterator.bind(py).call_method1("send", (value,))
    }

    #[pyo3(signature = (*args))]
    fn throw<'py>(
        &self,
        py: Python<'py>,
        args: &'py Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.iterator.bind(py).call_method1("throw", args)
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<()> {
        match self.iterator.bind(py).call_method0("close") {
            Ok(_) => Ok(()),
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl Drop for PyroFuture {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

#[pymethods]
impl PyroFuture {
    fn __await__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Py<PyroFutureIterator>> {
        // Get the iterator from the underlying future
        let awaitable = slf.py_future.bind(py);
        let iterator = awaitable.call_method0("__await__")?;

        // Create our wrapper iterator that keeps self alive
        Py::new(
            py,
            PyroFutureIterator {
                iterator: iterator.unbind(),
                _future: slf.into_pyobject(py)?.unbind(),
            },
        )
    }

    fn cancel<'py>(&mut self, py: Python<'py>) -> PyResult<bool> {
        // mark the Python future as cancelled, making await on __await__ raises
        let py_future = self.py_future.bind(py);
        let result = py_future.call_method0("cancel")?;

        // Also abort the Rust future
        self.abort_handle.abort();

        result.extract()
    }

    fn get_loop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let py_future = self.py_future.bind(py);
        py_future.call_method0("get_loop")
    }
}

/// Convert a Rust future into a Python future wrapped with RaiiFuture for automatic cancellation.
pub fn rust_future_into_py<F, T>(py: Python<'_>, fut: F) -> PyResult<Py<PyroFuture>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py>,
{
    // Get the event loop and create a future
    let event_loop = pyo3_async_runtimes::get_running_loop(py)?;
    let py_fut = event_loop.call_method0("create_future")?;
    let future_py = py_fut.clone().unbind();

    // Create an abortable future
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let abortable_fut = Abortable::new(fut, abort_registration);

    // Spawn the task
    let event_loop = event_loop.unbind();
    pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        let result = match abortable_fut.await {
            Ok(result) => result,
            Err(_) => Err(PyErr::new::<pyo3::exceptions::asyncio::CancelledError, _>(
                "Task was cancelled",
            )),
        };

        // Set the result on the Python future
        Python::attach(|py| {
            let py_fut = future_py.bind(py);

            // Check if user already cancelled the future
            // TODO: move this check inside call_soon_threadsafe
            let cancelled = py_fut
                .call_method0("cancelled")
                .map(|v| v.is_truthy().unwrap_or(false))
                .unwrap_or(false);

            if cancelled {
                return;
            }

            match result {
                Ok(value) => {
                    event_loop
                        .call_method1(
                            py,
                            "call_soon_threadsafe",
                            (
                                py_fut.getattr("set_result").unwrap(),
                                value.into_py_any(py).unwrap(),
                            ),
                        )
                        .unwrap();
                }
                Err(err) => {
                    event_loop
                        .call_method1(
                            py,
                            "call_soon_threadsafe",
                            (
                                py_fut.getattr("set_exception").unwrap(),
                                err.into_bound_py_any(py).unwrap(),
                            ),
                        )
                        .unwrap();
                }
            }
        });
    });

    // Wrap it in RaiiFuture
    let raii_future = Py::new(
        py,
        PyroFuture {
            py_future: py_fut.unbind(),
            abort_handle,
        },
    )?;

    Ok(raii_future)
}
