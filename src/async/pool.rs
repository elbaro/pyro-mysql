use std::sync::Arc;

use crate::{
    r#async::conn::AsyncConn,
    r#async::opts::AsyncOpts,
    util::{mysql_error_to_pyerr, url_error_to_pyerr},
};
use either::Either;
use mysql_async::Opts;
use pyo3::prelude::*;
use tokio::sync::RwLock;

#[pyclass]
pub struct AsyncPool {
    pool: mysql_async::Pool, // This is clonable
}

#[pymethods]
impl AsyncPool {
    /// new() won't assert server availability.
    /// Can accept either a URL string or AsyncOpts object
    #[new]
    pub fn new(url_or_opts: Either<String, PyRef<AsyncOpts>>) -> PyResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url).map_err(url_error_to_pyerr)?,
            Either::Right(opts) => opts.opts.clone(),
        };

        let pool = mysql_async::Pool::new(opts);
        Ok(Self { pool })
    }

    // pub fn close_gracefully(self) {
    // This needs to be handled properly with async runtime
    // For now, we'll leave it as a placeholder
    // }

    fn get_conn<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?;
        pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals, async move {
            Ok(AsyncConn {
                inner: Arc::new(RwLock::new(Some(
                    pool.get_conn().await.map_err(mysql_error_to_pyerr)?,
                ))),
            })
        })
    }

    fn acquire<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.get_conn(py)
    }

    fn disconnect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?;
        pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals, async move {
            pool.disconnect().await.map_err(mysql_error_to_pyerr)?;
            Ok(())
        })
    }
}
