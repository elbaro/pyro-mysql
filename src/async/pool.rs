use std::sync::Arc;

use crate::{
    r#async::{conn::AsyncConn, multi_conn::MultiAsyncConn, opts::AsyncOpts},
    util::{PyroFuture, rust_future_into_py, url_error_to_pyerr},
};
use either::Either;
use mysql_async::Opts;
use pyo3::prelude::*;
use tokio::sync::RwLock;

#[pyclass(module = "pyro_mysql.async_", name = "Pool")]
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

    fn get<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let pool = self.pool.clone();
        rust_future_into_py(py, async move {
            let conn = pool.get_conn().await?;
            Ok(AsyncConn {
                inner: Arc::new(RwLock::new(Some(MultiAsyncConn::MysqlAsync(conn)))),
            })
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let pool = self.pool.clone();
        rust_future_into_py(py, async move {
            pool.disconnect().await?;
            Ok(())
        })
    }
}
