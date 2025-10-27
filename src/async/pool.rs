use std::sync::Arc;

use crate::{
    r#async::{conn::AsyncConn, wtx_types::{StatementCache, WtxConn}},
    error::Error,
    util::{PyroFuture, rust_future_into_py},
};
use pyo3::prelude::*;
use tokio::sync::RwLock;

#[pyclass(module = "pyro_mysql.async_", name = "Pool")]
pub struct AsyncPool {
    url: String,
}

#[pymethods]
impl AsyncPool {
    /// new() won't assert server availability.
    /// Can accept only a URL string (AsyncOpts not yet supported with wtx)
    #[new]
    pub fn new(url: String) -> PyResult<Self> {
        Ok(Self { url })
    }

    fn get<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let url = self.url.clone();
        rust_future_into_py(py, async move {
            let wtx_conn = WtxConn::connect(&url)
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            Ok(AsyncConn {
                inner: wtx_conn.executor,
                stmt_cache: Arc::new(RwLock::new(StatementCache::new())),
            })
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        // wtx connections are closed when dropped, no explicit close needed
        rust_future_into_py(py, async move {
            Ok(())
        })
    }
}
