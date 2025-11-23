// PEP 249 – Python Database API Specification v2.0 (Async version)

use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    r#async::backend::ZeroMysqlConn,
    dbapi::{async_cursor::AsyncCursor, error::DbApiResult},
    error::{Error, PyroResult},
    params::Params,
    util::tokio_spawn_as_abort_on_drop,
};

#[pyclass(module = "pyro_mysql.dbapi_async", name = "Connection")]
pub struct AsyncDbApiConn(pub Arc<RwLock<Option<ZeroMysqlConn>>>);

impl AsyncDbApiConn {
    pub async fn exec_batch(&self, query: &str, params: Vec<Params>) -> DbApiResult<u64> {
        let mut guard = self.0.write().await;
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        log::debug!("execute {query}");

        let mut affected = 0;
        for p in params {
            conn.exec_drop(query.to_string(), p).await?;
            affected += conn.affected_rows();
        }
        Ok(affected)
    }
}

// Protocols:
// - sqlalchemy.connectors.AsyncAdapt_terminate
// - sqlalchemy.connectors.AsyncAdapt_dbapi_connection

#[pymethods]
impl AsyncDbApiConn {
    // ─── Pep249 ──────────────────────────────────────────────────────────

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            *arc.write().await = None;
            Ok(())
        })
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = arc.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            conn.query_drop("COMMIT".to_string()).await?;
            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = arc.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            conn.query_drop("ROLLBACK".to_string()).await?;
            Ok(())
        })
    }

    /// Cursor instances hold a reference to the python connection object.
    fn cursor(slf: Py<AsyncDbApiConn>) -> AsyncCursor {
        AsyncCursor::new(slf)
    }

    // ─── Helper ──────────────────────────────────────────────────────────

    pub async fn set_autocommit(&self, on: bool) -> PyroResult<()> {
        let arc = self.0.clone();
        let mut guard = arc.write().await;
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let query = if on {
            "SET autocommit=1"
        } else {
            "SET autocommit=0"
        };
        conn.query_drop(query.to_string()).await?;
        Ok(())
    }

    async fn ping(&self) -> DbApiResult<()> {
        let arc = self.0.clone();
        tokio_spawn_as_abort_on_drop(async move {
            let mut guard = arc.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            conn.ping().await?;
            PyroResult::Ok(())
        })
        .await??;
        Ok(())
    }

    /// Returns 0 if there was no last insert id.
    fn last_insert_id<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = arc.read().await;
            let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;

            let id = conn.last_insert_id();
            Ok(Some(id))
        })
    }

    fn is_closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = arc.read().await;
            Ok(guard.is_none()) // Fixed: is_none for closed, not is_some
        })
    }
}
