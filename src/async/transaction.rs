use pyo3::{prelude::*, pybacked::PyBackedStr};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockWriteGuard};
use wtx::database::Executor;

use crate::{
    r#async::queryable::Queryable,
    r#async::wtx_types::{StatementCache, WtxExecutor},
    error::Error,
    isolation_level::IsolationLevel,
    params::Params,
    util::{PyroFuture, rust_future_into_py},
};

// struct fields are dropped in the same order as declared in the struct
#[pyclass(module = "pyro_mysql.async_", name = "Transaction")]
pub struct AsyncTransaction {
    consistent_snapshot: bool,
    isolation_level: Option<IsolationLevel>,
    readonly: Option<bool>,

    /// true when transaction has been started (after __aenter__)
    started: Arc<RwLock<bool>>,

    /// Holding this guard prevents other concurrent calls of Conn::some_method(&mut self).
    /// guard is initialized in __aenter__.
    /// It is reset on commit(), rollback(), or __aexit__.
    guard: Arc<RwLock<Option<RwLockWriteGuard<'static, Option<WtxExecutor>>>>>,

    conn: Arc<RwLock<Option<WtxExecutor>>>,
    stmt_cache: Arc<RwLock<StatementCache>>,
}

impl AsyncTransaction {
    pub fn new(
        conn: Arc<RwLock<Option<WtxExecutor>>>,
        stmt_cache: Arc<RwLock<StatementCache>>,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> Self {
        AsyncTransaction {
            consistent_snapshot,
            isolation_level,
            readonly,
            started: Arc::new(RwLock::new(false)),
            conn,
            stmt_cache,
            guard: Default::default(),
        }
    }
}

// Order or lock: conn -> conn guard -> inner
#[pymethods]
impl AsyncTransaction {
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let consistent_snapshot = slf.consistent_snapshot;
        let isolation_level = slf.isolation_level.clone();
        let readonly = slf.readonly;
        let conn = slf.conn.clone();
        let guard = slf.guard.clone();
        let started = slf.started.clone();
        let slf: Py<AsyncTransaction> = slf.into();

        rust_future_into_py(py, async move {
            let mut conn = conn.write().await;
            let mut guard_lock = guard.write().await;
            let mut started_lock = started.write().await;

            // check if transaction is already inflight
            if *started_lock {
                return Err(Error::IncorrectApiUsageError("Transaction already started").into());
            }

            let exec = conn.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

            // Build START TRANSACTION statement
            let mut stmt = String::from("START TRANSACTION");

            if readonly.is_some() {
                if readonly.unwrap() {
                    stmt.push_str(" READ ONLY");
                } else {
                    stmt.push_str(" READ WRITE");
                }
            }

            if consistent_snapshot {
                stmt.push_str(" WITH CONSISTENT SNAPSHOT");
            }

            // Set isolation level if specified
            if let Some(ref level) = isolation_level {
                let level_str = match level {
                    IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
                    IsolationLevel::ReadCommitted => "READ COMMITTED",
                    IsolationLevel::RepeatableRead => "REPEATABLE READ",
                    IsolationLevel::Serializable => "SERIALIZABLE",
                };
                let level_stmt = format!("SET TRANSACTION ISOLATION LEVEL {}", level_str);
                exec.execute(&level_stmt, |_: u64| -> Result<(), wtx::Error> { Ok(()) })
                    .await
                    .map_err(|e| Error::WtxError(e.to_string()))?;
            }

            // Start transaction
            exec.execute(&stmt, |_: u64| -> Result<(), wtx::Error> { Ok(()) })
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            *started_lock = true;

            // As long as we hold Arc<Conn>, RwLockWriteGuard is valid.
            // guard is declared before conn so that Arc<Guard> drops first.
            *guard_lock = Some(unsafe {
                std::mem::transmute::<
                    RwLockWriteGuard<'_, _>,
                    RwLockWriteGuard<'static, Option<WtxExecutor>>,
                >(conn)
            });

            Ok(slf)
        })
    }
    fn __aexit__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        _exc_type: &crate::Bound<'py, crate::PyAny>,
        _exc_value: &crate::Bound<'py, crate::PyAny>,
        _traceback: &crate::Bound<'py, crate::PyAny>,
    ) -> PyResult<Py<PyroFuture>> {
        // Check reference count of the transaction object
        let refcnt = slf.get_refcnt();
        if refcnt != 2 {
            log::error!(
                "AsyncTransaction reference count is {} (expected 2) in __aexit__. Transaction may be referenced elsewhere.",
                refcnt
            );
        }

        let guard = slf.borrow().guard.clone();
        let started = slf.borrow().started.clone();
        rust_future_into_py(py, async move {
            let mut guard_lock = guard.write().await;
            let mut started_lock = started.write().await;

            if *started_lock {
                log::warn!("commit() or rollback() is not called. rolling back.");

                if let Some(ref mut guard_val) = *guard_lock {
                    if let Some(exec) = guard_val.as_mut() {
                        let _ = exec.execute("ROLLBACK", |_: u64| -> Result<(), wtx::Error> { Ok(()) }).await;
                    }
                }
                *started_lock = false;
            }
            *guard_lock = None;
            Ok(())
        })
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let guard = self.guard.clone();
        let started = self.started.clone();
        rust_future_into_py(py, async move {
            let mut started_lock = started.write().await;
            if !*started_lock {
                return Err(Error::TransactionClosedError.into());
            }

            let mut guard_lock = guard.write().await;
            if let Some(ref mut guard_val) = *guard_lock {
                if let Some(exec) = guard_val.as_mut() {
                    exec.execute("COMMIT", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;
                }
            }

            *started_lock = false;
            *guard_lock = None;
            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let guard = self.guard.clone();
        let started = self.started.clone();
        rust_future_into_py(py, async move {
            let mut started_lock = started.write().await;
            if !*started_lock {
                return Err(Error::TransactionClosedError.into());
            }

            let mut guard_lock = guard.write().await;
            if let Some(ref mut guard_val) = *guard_lock {
                if let Some(exec) = guard_val.as_mut() {
                    exec.execute("ROLLBACK", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;
                }
            }

            *started_lock = false;
            *guard_lock = None;
            Ok(())
        })
    }

    fn affected_rows<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        // wtx doesn't track affected_rows at connection level
        rust_future_into_py(py, async move {
            Ok(0u64)
        })
    }

    // ─── Queryable ───────────────────────────────────────────────────────
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        self.conn.ping(py)
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.conn.query(py, query)
    }
    fn query_first<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.conn.query_first(py, query)
    }
    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.conn.query_drop(py, query)
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────
    #[pyo3(signature = (query, params=None))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.conn.exec(py, query, params, self.stmt_cache.clone())
    }
    #[pyo3(signature = (query, params=None))]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.conn.exec_first(py, query, params, self.stmt_cache.clone())
    }
    #[pyo3(signature = (query, params=None))]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.conn.exec_drop(py, query, params, self.stmt_cache.clone())
    }
    #[pyo3(signature = (query, params=vec![]))]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        self.conn.exec_batch(py, query, params, self.stmt_cache.clone())
    }
}
