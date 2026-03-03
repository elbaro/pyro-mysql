use std::future::Future;

use pyo3::IntoPyObjectExt;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use tokio_util::task::AbortOnDropHandle;

use crate::error::PyroResult;

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

pub fn tokio_spawn_as_abort_on_drop<F>(
    fut: F,
) -> Result<AbortOnDropHandle<F::Output>, crate::error::Error>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Ok(AbortOnDropHandle::new(
        crate::tokio_thread::get_tokio_thread()
            .map_err(crate::error::Error::IoError)?
            .spawn(fut),
    ))
}

/// == Coroutine::new(AbortOnDropHandle::new(pyo3_async_runtimes::tokio::get_runtime().spawn(fut)))
pub fn rust_future_into_py<F, T>(py: Python<'_>, fut: F) -> PyResult<Py<PyroFuture>>
where
    F: Future<Output = PyroResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    let event_loop = pyo3_async_runtimes::get_running_loop(py)?;

    // Because the event loop can be changed, these attributes are not cached.
    let create_future = event_loop.getattr(intern!(py, "create_future"))?.unbind();
    let call_soon_threadsafe = event_loop
        .getattr(intern!(py, "call_soon_threadsafe"))?
        .unbind();

    let py_future = create_future.call0(py)?;
    {
        let py_future = py_future.clone_ref(py);
        crate::tokio_thread::get_tokio_thread()
            .map_err(crate::error::Error::IoError)?
            .spawn(async move {
                let result = fut.await;

                // TODO: spawn_blocking or not?
                //      - tokio::task::spawn_blocking slows down the microbench by 3~12%.
                //      - The default GIL switch interval is 5 milliseconds (5000 microseconds).
                //      - tokio considers 10~100us as a blocking operation
                //      - Python::attach() here runs in 30-50us on uncontended situation

                // tokio::task::spawn_blocking(move || {

                Python::attach(|py2| {
                    let bound_future = py_future.bind(py2);
                    let r: PyResult<()> = (|| {
                        match result {
                            Ok(value) => {
                                call_soon_threadsafe.call1(
                                    py2,
                                    (
                                        bound_future.getattr(intern!(py2, "set_result"))?,
                                        value.into_py_any(py2)?,
                                    ),
                                )?;
                            }
                            Err(err) => {
                                call_soon_threadsafe.call1(
                                    py2,
                                    (
                                        bound_future.getattr(intern!(py2, "set_exception"))?,
                                        pyo3::PyErr::from(err).into_bound_py_any(py2)?,
                                    ),
                                )?;
                            }
                        }
                        Ok(())
                    })();
                    if let Err(e) = r {
                        log::error!("Failed to resolve Python future: {e}");
                    }
                });
                // })
                // .await
                // .unwrap();
            });
    }

    Ok(py_future)
}

pub struct PyTupleBuilder {
    ptr: *mut pyo3::ffi::PyObject,
}

impl PyTupleBuilder {
    pub fn new(_py: Python, len: usize) -> Self {
        // SAFETY: GIL is held (enforced by `_py: Python` token), `len` is a valid size.
        let ptr = unsafe { pyo3::ffi::PyTuple_New(len as isize) };
        Self { ptr }
    }

    pub fn set<'py>(&self, index: usize, value: Bound<'py, PyAny>) {
        // SAFETY: `self.ptr` is a valid PyTuple created by `PyTuple_New` in `new()`.
        // `into_ptr()` transfers ownership to the tuple (steals the reference).
        // Caller must ensure `index < len`.
        unsafe {
            pyo3::ffi::PyTuple_SetItem(self.ptr, index as pyo3::ffi::Py_ssize_t, value.into_ptr());
        }
    }

    pub fn build<'py>(self, py: Python<'py>) -> Bound<'py, PyTuple> {
        // SAFETY: `self.ptr` is a valid owned PyTuple created by `PyTuple_New`.
        let obj = unsafe { Bound::from_owned_ptr(py, self.ptr) };
        // SAFETY: `PyTuple_New` always returns a PyTuple object.
        unsafe { obj.cast_into_unchecked() }
    }
}
