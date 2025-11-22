use std::sync::atomic::Ordering;

use pyo3::prelude::*;

use crate::error::{Error, PyroResult};
use crate::sync::conn::SyncConn;

/// Transaction is a lightweight wrapper that only holds a reference to the Python Conn object.
/// It does not use the backend's Transaction API. Instead, it uses SQL commands:
/// - `__enter__`: Executes `START TRANSACTION` (with options)
/// - `commit()`: Executes `COMMIT`
/// - `rollback()`: Executes `ROLLBACK`
///
/// The user should use `conn.exec*` and `conn.query*` methods directly while the transaction
/// is active. The Transaction object is only used for `commit()` and `rollback()`.
#[pyclass(module = "pyro_mysql.sync", name = "Transaction")]
pub struct SyncTransaction {
    conn: Py<SyncConn>,
    consistent_snapshot: bool,
    isolation_level: Option<String>,
    readonly: Option<bool>,
}

impl SyncTransaction {
    pub fn new(
        conn: Py<SyncConn>,
        consistent_snapshot: bool,
        isolation_level: Option<String>,
        readonly: Option<bool>,
    ) -> Self {
        SyncTransaction {
            conn,
            consistent_snapshot,
            isolation_level,
            readonly,
        }
    }
}

#[pymethods]
impl SyncTransaction {
    pub fn __enter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyroResult<Bound<'py, Self>> {
        let slf_ref = slf.borrow();
        let conn = &slf_ref.conn;

        // Check if already in transaction
        {
            let conn_ref = conn.borrow(py);
            if conn_ref.in_transaction.load(Ordering::SeqCst) {
                return Err(Error::IncorrectApiUsageError(
                    "Connection is already in a transaction",
                ));
            }
            // Set in_transaction flag
            conn_ref.in_transaction.store(true, Ordering::SeqCst);
        }

        // Set isolation level if specified (must be done before START TRANSACTION)
        if let Some(ref level) = slf_ref.isolation_level {
            let conn_ref = conn.borrow(py);
            conn_ref.query_drop_internal(format!("SET TRANSACTION ISOLATION LEVEL {}", level))?;
        }

        // Set access mode if specified
        if let Some(ro) = slf_ref.readonly {
            let conn_ref = conn.borrow(py);
            let mode = if ro { "READ ONLY" } else { "READ WRITE" };
            conn_ref.query_drop_internal(format!("SET TRANSACTION {}", mode))?;
        }

        // Build START TRANSACTION statement
        let sql = if slf_ref.consistent_snapshot {
            "START TRANSACTION WITH CONSISTENT SNAPSHOT".to_string()
        } else {
            "START TRANSACTION".to_string()
        };

        // Execute START TRANSACTION
        {
            let conn_ref = conn.borrow(py);
            conn_ref.query_drop_internal(sql)?;
        }

        drop(slf_ref);
        Ok(slf)
    }

    pub fn __exit__(
        slf: &Bound<'_, Self>,
        py: Python,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyroResult<bool> {
        let slf_ref = slf.borrow();
        let conn = &slf_ref.conn;

        // Only rollback if transaction is still active (not yet committed/rolled back)
        let should_rollback = {
            let conn_ref = conn.borrow(py);
            conn_ref.in_transaction.load(Ordering::SeqCst)
        };

        if should_rollback {
            log::warn!("commit() or rollback() was not called. Rolling back.");
            let conn_ref = conn.borrow(py);
            conn_ref.query_drop_internal("ROLLBACK".to_string())?;
            conn_ref.in_transaction.store(false, Ordering::SeqCst);
        }

        Ok(false) // Don't suppress exceptions
    }

    fn commit(&self, py: Python) -> PyroResult<()> {
        let conn_ref = self.conn.borrow(py);

        // Check if in transaction
        if !conn_ref.in_transaction.load(Ordering::SeqCst) {
            return Err(Error::TransactionClosedError);
        }

        // Execute COMMIT
        conn_ref.query_drop_internal("COMMIT".to_string())?;

        // Clear in_transaction flag
        conn_ref.in_transaction.store(false, Ordering::SeqCst);

        Ok(())
    }

    fn rollback(&self, py: Python) -> PyroResult<()> {
        let conn_ref = self.conn.borrow(py);

        // Check if in transaction
        if !conn_ref.in_transaction.load(Ordering::SeqCst) {
            return Err(Error::TransactionClosedError);
        }

        // Execute ROLLBACK
        conn_ref.query_drop_internal("ROLLBACK".to_string())?;

        // Clear in_transaction flag
        conn_ref.in_transaction.store(false, Ordering::SeqCst);

        Ok(())
    }
}
