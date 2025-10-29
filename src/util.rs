use std::future::Future;

use pyo3::IntoPyObjectExt;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyTuple;
use tokio_util::task::AbortOnDropHandle;

use crate::error::PyroResult;

struct Cache {
    create_future: Py<PyAny>,
    call_soon_threadsafe: Py<PyAny>,
}

static CACHE: PyOnceLock<Cache> = PyOnceLock::new();

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
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    let cache = CACHE.get_or_try_init(py, || {
        let event_loop = pyo3_async_runtimes::get_running_loop(py)?;
        let create_future = event_loop.getattr("create_future")?.unbind();
        let call_soon_threadsafe = event_loop.getattr("call_soon_threadsafe")?.unbind();

        PyResult::Ok(Cache {
            create_future,
            call_soon_threadsafe,
        })
    })?;
    let py_future = cache.create_future.call0(py)?;
    {
        let py_future = py_future.clone_ref(py);
        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            let result = fut.await;

            // TODO: spawn_blocking or not?
            //      - tokio::task::spawn_blocking slows down the microbench by 3~12%.
            //      - The default GIL switch interval is 5 milliseconds (5000 microseconds).
            //      - tokio considers 10~100us as a blocking operation
            //      - Python::attach() here runs in 30-50us on uncontended situation

            // tokio::task::spawn_blocking(move || {

            Python::attach(|py| {
                let bound_future = py_future.bind(py);
                match result {
                    Ok(value) => {
                        cache
                            .call_soon_threadsafe
                            .call1(
                                py,
                                (
                                    bound_future.getattr(intern!(py, "set_result")).unwrap(),
                                    value.into_py_any(py).unwrap(),
                                ),
                            )
                            .unwrap();
                    }
                    Err(err) => {
                        cache
                            .call_soon_threadsafe
                            .call1(
                                py,
                                (
                                    bound_future.getattr(intern!(py, "set_exception")).unwrap(),
                                    pyo3::PyErr::from(err).into_bound_py_any(py).unwrap(),
                                ),
                            )
                            .unwrap();
                    }
                }
            });
            // })
            // .await
            // .unwrap();
        });
    }

    Ok(py_future)
}
