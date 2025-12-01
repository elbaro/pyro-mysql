use std::sync::Arc;
use std::sync::atomic::Ordering;

use pyo3::prelude::*;
use tokio::sync::RwLock;

use crate::r#async::conn::AsyncConn;
use crate::r#async::multi_conn::MultiAsyncConn;
use crate::error::{Error, PyroResult};
use crate::util::{PyroFuture, rust_future_into_py};

// Import the mysql_async Queryable trait for its methods
use mysql_async::prelude::Queryable as MysqlAsyncQueryable;

/// Transaction is a lightweight wrapper that only holds a reference to the Python Conn object.
/// It does not use the backend's Transaction API. Instead, it uses SQL commands:
/// - `__aenter__`: Executes `START TRANSACTION` (with options)
/// - `commit()`: Executes `COMMIT`
/// - `rollback()`: Executes `ROLLBACK`
///
/// The user should use `conn.exec*` and `conn.query*` methods directly while the transaction
/// is active. The Transaction object is only used for `commit()` and `rollback()`.
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

/// Helper function to execute a query_drop on a MultiAsyncConn
async fn multi_conn_query_drop(
    inner: &Arc<RwLock<Option<MultiAsyncConn>>>,
    query: String,
) -> PyroResult<()> {
    let mut guard = inner.write().await;
    let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
    match conn {
        MultiAsyncConn::MysqlAsync(mysql_conn) => {
            mysql_conn.query_drop(query).await?;
            Ok(())
        }
        MultiAsyncConn::Wtx(wtx_conn) => {
            use wtx::database::Executor;
            wtx_conn
                .executor
                .execute(&query, |_affected: u64| -> Result<(), wtx::Error> {
                    Ok(())
                })
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;
            Ok(())
        }
        MultiAsyncConn::ZeroMysql(zero_conn) => {
            zero_conn.query_drop(query).await?;
            Ok(())
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

        // Get the inner connection Arc before entering the async block
        let inner: Arc<RwLock<Option<MultiAsyncConn>>> =
            Python::attach(|py| conn.borrow(py).inner.clone());

        rust_future_into_py(py, async move {
            Python::attach(|py| -> PyResult<()> {
                let conn_ref = conn.borrow(py);
                // Check if already in transaction
                if conn_ref.in_transaction.load(Ordering::SeqCst) {
                    return Err(Error::IncorrectApiUsageError(
                        "Connection is already in a transaction",
                    )
                    .into());
                }
                // Set in_transaction flag
                conn_ref.in_transaction.store(true, Ordering::SeqCst);
                Ok(())
            })?;

            // Set isolation level if specified (must be done before START TRANSACTION)
            if let Some(&level) = isolation_level {
                multi_conn_query_drop(&inner, format!("SET TRANSACTION ISOLATION LEVEL {}", level))
                    .await?;
            }

            // Set access mode if specified
            if let Some(ro) = readonly {
                let mode = if ro { "READ ONLY" } else { "READ WRITE" };
                multi_conn_query_drop(&inner, format!("SET TRANSACTION {}", mode)).await?;
            }

            // Build START TRANSACTION statement
            let sql = if consistent_snapshot {
                "START TRANSACTION WITH CONSISTENT SNAPSHOT".to_string()
            } else {
                "START TRANSACTION".to_string()
            };

            // Execute START TRANSACTION
            multi_conn_query_drop(&inner, sql).await?;

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
        let inner: Arc<RwLock<Option<MultiAsyncConn>>> = conn.borrow(py).inner.clone();

        rust_future_into_py(py, async move {
            // Only rollback if transaction is still active (not yet committed/rolled back)
            let should_rollback =
                Python::attach(|py| conn.borrow(py).in_transaction.load(Ordering::SeqCst));

            if should_rollback {
                log::warn!("commit() or rollback() was not called. Rolling back.");
                multi_conn_query_drop(&inner, "ROLLBACK".to_string()).await?;

                // Clear in_transaction flag
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
        let inner: Arc<RwLock<Option<MultiAsyncConn>>> = conn.borrow(py).inner.clone();

        rust_future_into_py(py, async move {
            // Check if in transaction
            Python::attach(|py| -> PyroResult<()> {
                let conn_ref = conn.borrow(py);
                if !conn_ref.in_transaction.load(Ordering::SeqCst) {
                    return Err(Error::TransactionClosedError);
                }
                Ok(())
            })?;

            // Execute COMMIT
            multi_conn_query_drop(&inner, "COMMIT".to_string()).await?;

            // Clear in_transaction flag
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
        let inner: Arc<RwLock<Option<MultiAsyncConn>>> = conn.borrow(py).inner.clone();

        rust_future_into_py(py, async move {
            // Check if in transaction
            Python::attach(|py| -> PyroResult<()> {
                let conn_ref = conn.borrow(py);
                if !conn_ref.in_transaction.load(Ordering::SeqCst) {
                    return Err(Error::TransactionClosedError);
                }
                Ok(())
            })?;

            // Execute ROLLBACK
            multi_conn_query_drop(&inner, "ROLLBACK".to_string()).await?;

            // Clear in_transaction flag
            Python::attach(|py| {
                conn.borrow(py)
                    .in_transaction
                    .store(false, Ordering::SeqCst);
            });

            Ok(())
        })
    }
}
