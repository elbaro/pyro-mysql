use std::sync::Arc;
use std::sync::atomic::Ordering;

use pyo3::prelude::*;
use tokio::sync::RwLock;
use zero_mysql::tokio::Conn;

use crate::r#async::conn::AsyncConn;
use crate::r#async::handler::DropHandler;
use crate::error::{Error, PyroResult};
use crate::util::{PyroFuture, rust_future_into_py};

async fn execute_query_drop(inner: &Arc<RwLock<Option<Conn>>>, query: &str) -> PyroResult<()> {
    let mut guard = inner.write().await;
    let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
    let mut handler = DropHandler::default();
    conn.query(query, &mut handler).await?;
    Ok(())
}

#[pyclass(module = "pyro_mysql.async_", name = "Transaction")]
pub struct AsyncTransaction {
    conn: Py<AsyncConn>,
    consistent_snapshot: bool,
    isolation_level: Option<String>,
    readonly: Option<bool>,
}

impl AsyncTransaction {
    pub fn new(
        conn: Py<AsyncConn>,
        consistent_snapshot: bool,
        isolation_level: Option<String>,
        readonly: Option<bool>,
    ) -> Self {
        AsyncTransaction {
            conn,
            consistent_snapshot,
            isolation_level,
            readonly,
        }
    }
}

#[pymethods]
impl AsyncTransaction {
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let conn = slf.conn.clone_ref(py);
        let consistent_snapshot = slf.consistent_snapshot;
        let isolation_level = slf.isolation_level.clone();
        let readonly = slf.readonly;
        let slf_py: Py<AsyncTransaction> = slf.into();

        rust_future_into_py(py, async move {
            Python::attach(|py| -> PyResult<()> {
                let conn_ref = conn.borrow(py);
                if conn_ref.in_transaction.load(Ordering::SeqCst) {
                    return Err(Error::IncorrectApiUsageError(
                        "Connection is already in a transaction",
                    )
                    .into());
                }
                conn_ref.in_transaction.store(true, Ordering::SeqCst);
                Ok(())
            })?;

            // Get the inner connection reference for async operations
            let inner = Python::attach(|py| conn.borrow(py).inner.clone());

            // Set isolation level if specified (must be done before START TRANSACTION)
            if let Some(level) = isolation_level {
                let query = format!("SET TRANSACTION ISOLATION LEVEL {}", level);
                execute_query_drop(&inner, &query).await?;
            }

            // Set access mode if specified
            if let Some(ro) = readonly {
                let mode = if ro { "READ ONLY" } else { "READ WRITE" };
                let query = format!("SET TRANSACTION {}", mode);
                execute_query_drop(&inner, &query).await?;
            }

            // Build START TRANSACTION statement
            let sql = if consistent_snapshot {
                "START TRANSACTION WITH CONSISTENT SNAPSHOT"
            } else {
                "START TRANSACTION"
            };

            // Execute START TRANSACTION
            execute_query_drop(&inner, sql).await?;

            Ok(slf_py)
        })
    }

    fn __aexit__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        _exc_type: &Bound<'py, PyAny>,
        _exc_value: &Bound<'py, PyAny>,
        _traceback: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyroFuture>> {
        let conn = slf.borrow().conn.clone_ref(py);

        rust_future_into_py(py, async move {
            let (should_rollback, inner) = Python::attach(|py| {
                let conn_ref = conn.borrow(py);
                (
                    conn_ref.in_transaction.load(Ordering::SeqCst),
                    conn_ref.inner.clone(),
                )
            });

            if should_rollback {
                log::warn!("commit() or rollback() was not called. Rolling back.");
                execute_query_drop(&inner, "ROLLBACK").await?;

                Python::attach(|py| {
                    conn.borrow(py)
                        .in_transaction
                        .store(false, Ordering::SeqCst);
                });
            }

            Ok(())
        })
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let conn = self.conn.clone_ref(py);

        rust_future_into_py(py, async move {
            let inner = Python::attach(|py| -> PyroResult<_> {
                let conn_ref = conn.borrow(py);
                if !conn_ref.in_transaction.load(Ordering::SeqCst) {
                    return Err(Error::TransactionClosedError);
                }
                Ok(conn_ref.inner.clone())
            })?;

            execute_query_drop(&inner, "COMMIT").await?;

            Python::attach(|py| {
                conn.borrow(py)
                    .in_transaction
                    .store(false, Ordering::SeqCst);
            });

            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let conn = self.conn.clone_ref(py);

        rust_future_into_py(py, async move {
            let inner = Python::attach(|py| -> PyroResult<_> {
                let conn_ref = conn.borrow(py);
                if !conn_ref.in_transaction.load(Ordering::SeqCst) {
                    return Err(Error::TransactionClosedError);
                }
                Ok(conn_ref.inner.clone())
            })?;

            execute_query_drop(&inner, "ROLLBACK").await?;

            Python::attach(|py| {
                conn.borrow(py)
                    .in_transaction
                    .store(false, Ordering::SeqCst);
            });

            Ok(())
        })
    }
}
