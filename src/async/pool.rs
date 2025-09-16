use std::sync::Arc;

use crate::{
    r#async::conn::AsyncConn,
    util::{mysql_error_to_pyerr, url_error_to_pyerr},
};
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
    /// url example: mysql://root:password@127.0.0.1:3307/mysql
    #[new]
    pub fn new(url: &str) -> PyResult<Self> {
        Ok(Self {
            pool: mysql_async::Pool::new(Opts::try_from(url).map_err(url_error_to_pyerr)?),
        })
    }

    // pub fn close_gracefully(self) {
    // This needs to be handled properly with async runtime
    // For now, we'll leave it as a placeholder
    // }

    pub async fn acquire(&self) -> PyResult<AsyncConn> {
        // TODO: run this future in tokio
        self.pool
            .get_conn()
            .await
            .map(|conn| AsyncConn {
                inner: Arc::new(RwLock::new(Some(conn))),
            })
            .map_err(mysql_error_to_pyerr)
    }
}
