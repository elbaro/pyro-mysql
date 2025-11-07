// PEP 249 – Python Database API Specification v2.0 (Async version)

use mysql_async::prelude::*;
use pyo3::{prelude::*, types::PyList};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    dbapi::{async_cursor::AsyncCursor, error::DbApiResult},
    error::{Error, PyroResult},
    params::Params,
    r#async::multi_conn::MultiAsyncConn,
    row::Row,
    util::tokio_spawn_as_abort_on_drop,
};

#[pyclass(module = "pyro_mysql.dbapi_async", name = "Connection")]
pub struct AsyncDbApiConn(pub Arc<RwLock<Option<MultiAsyncConn>>>);

impl From<crate::r#async::conn::AsyncConn> for AsyncDbApiConn {
    fn from(value: crate::r#async::conn::AsyncConn) -> Self {
        Self(value.inner)
    }
}

pub enum AsyncDbApiExecResult {
    WithDescription {
        rows: Vec<Row>,
        description: Py<PyList>,
        affected_rows: u64,
    },
    NoDescription {
        affected_rows: u64,
        last_insert_id: Option<u64>,
    },
}

impl AsyncDbApiConn {
    // TODO: cleanup
    // async fn exec_drop(&self, query: &str, params: Params) -> DbApiResult<()> {
    //     let mut guard = self.0.write().await;
    //     let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
    //     log::debug!("execute {query}");
    //     Ok(conn.exec_drop(query, params).await.map_err(Error::from)?)
    // }

    pub async fn exec_batch(&self, query: &str, params: Vec<Params>) -> DbApiResult<u64> {
        let mut guard = self.0.write().await;
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        log::debug!("execute {query}");

        match multi_conn {
            MultiAsyncConn::MysqlAsync(conn) => {
                let mut affected = 0;
                let stmt = conn.prep(query).await.map_err(Error::from)?;
                for params in params {
                    conn.exec_drop(&stmt, params).await.map_err(Error::from)?;
                    affected += conn.affected_rows();
                }
                Ok(affected)
            }
            MultiAsyncConn::Wtx { .. } => {
                // wtx is not supported in DB-API, use the async API instead
                Err(Error::IncorrectApiUsageError(
                    "wtx connections are not supported with DB-API. Use the async API (pyro_mysql.AsyncConn) instead."
                ).into())
            }
        }
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
            let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            let conn = match multi_conn {
                MultiAsyncConn::MysqlAsync(c) => c,
                MultiAsyncConn::Wtx { executor, .. } => {
                    use wtx::database::Executor;
                    executor
                        .execute("COMMIT", |_: u64| Ok(()))
                        .await
                        .map_err(|e: wtx::Error| Error::WtxError(e.to_string()))?;
                    return Ok(());
                }
            };
            conn.exec_drop("COMMIT", Params::default())
                .await
                .map_err(Error::from)?;
            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = arc.write().await;
            let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            let conn = match multi_conn {
                MultiAsyncConn::MysqlAsync(c) => c,
                MultiAsyncConn::Wtx { executor, .. } => {
                    use wtx::database::Executor;
                    executor
                        .execute("ROLLBACK", |_: u64| Ok(()))
                        .await
                        .map_err(|e: wtx::Error| Error::WtxError(e.to_string()))?;
                    return Ok(());
                }
            };
            conn.exec_drop("ROLLBACK", Params::default())
                .await
                .map_err(Error::from)?;
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
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let conn = match multi_conn {
            MultiAsyncConn::MysqlAsync(c) => c,
            MultiAsyncConn::Wtx { executor, .. } => {
                use wtx::database::Executor;
                let query = if on {
                    "SET autocommit=1"
                } else {
                    "SET autocommit=0"
                };
                executor
                    .execute(query, |_: u64| Ok(()))
                    .await
                    .map_err(|e: wtx::Error| Error::WtxError(e.to_string()))?;
                return Ok(());
            }
        };
        let query = if on {
            "SET autocommit=1"
        } else {
            "SET autocommit=0"
        };
        conn.exec_drop(query, Params::default())
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    async fn ping(&self) -> DbApiResult<()> {
        let arc = self.0.clone();
        tokio_spawn_as_abort_on_drop(async move {
            let mut guard = arc.write().await;
            let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            let conn = match multi_conn {
                MultiAsyncConn::MysqlAsync(c) => c,
                MultiAsyncConn::Wtx { executor, .. } => {
                    use wtx::database::Executor;
                    executor
                        .execute("SELECT 1", |_: u64| Ok(()))
                        .await
                        .map_err(|e: wtx::Error| Error::WtxError(e.to_string()))?;
                    return Ok(());
                }
            };
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
            Ok(id)
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
