use std::future::Future;

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyTuple;
use tokio_util::task::AbortOnDropHandle;

use crate::error::PyroResult;

static CREATE_FUTURE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

pub fn mysql_error_to_pyerr(error: mysql_async::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyException, _>(format!("MySQL Error: {}", error))
}

pub fn url_error_to_pyerr(error: mysql_async::UrlError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyException, _>(format!("MySQL Error: {}", error))
}

pub type PyroFuture = PyAny;

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

pub fn tokio_spawn_as_abort_on_drop<F>(fut: F) -> AbortOnDropHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    AbortOnDropHandle::new(pyo3_async_runtimes::tokio::get_runtime().spawn(fut))
}

/// == Coroutine::new(AbortOnDropHandle::new(pyo3_async_runtimes::tokio::get_runtime().spawn(fut)))
pub fn rust_future_into_py<F, T>(py: Python<'_>, fut: F) -> PyResult<Py<PyroFuture>>
where
    F: Future<Output = PyroResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py>,
{
    let event_loop = pyo3_async_runtimes::get_running_loop(py)?;
    let bound_future = CREATE_FUTURE
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            let create_future = event_loop.getattr("create_future")?;
            Ok(create_future.unbind())
        })?
        .bind(py)
        .call0()?;
    let py_future = bound_future.clone().unbind();
    let event_loop = event_loop.unbind();

    pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        let result = fut.await;

        Python::attach(|py| {
            let bound_future = py_future.bind(py);

            // Check if user already cancelled the future
            // TODO: move this check inside call_soon_threadsafe
            let cancelled = bound_future
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
                                bound_future.getattr("set_result").unwrap(),
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
                                bound_future.getattr("set_exception").unwrap(),
                                pyo3::PyErr::from(err).into_bound_py_any(py).unwrap(),
                            ),
                        )
                        .unwrap();
                }
            }
        });
    });

    Ok(bound_future.unbind())
}
