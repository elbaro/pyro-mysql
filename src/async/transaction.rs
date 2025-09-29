use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::{
    r#async::queryable::Queryable,
    error::Error,
    params::Params,
    util::{PyroFuture, rust_future_into_py},
};

// struct fields are dropped in the same order as declared in the struct
#[pyclass]
pub struct AsyncTransaction {
    opts: mysql_async::TxOpts,

    /// Option<Transaction> is initialized in __aenter__.
    /// It is reset on commit(), rollback(), or __aexit__.
    inner: Arc<RwLock<Option<mysql_async::Transaction<'static>>>>,

    /// Holding this guard prevents other concurrent calls of Conn::some_method(&mut self).
    /// guard is initialized in __aenter__.
    /// It is reset on commit(), rollback(), or __aexit__.
    guard: Arc<RwLock<Option<tokio::sync::RwLockWriteGuard<'static, Option<mysql_async::Conn>>>>>,

    conn: Arc<RwLock<Option<mysql_async::Conn>>>,
}

impl AsyncTransaction {
    pub fn new(conn: Arc<RwLock<Option<mysql_async::Conn>>>, opts: mysql_async::TxOpts) -> Self {
        AsyncTransaction {
            opts,
            conn,
            guard: Default::default(),
            inner: Default::default(),
        }
    }
}

// Order or lock: conn -> conn guard -> inner
#[pymethods]
impl AsyncTransaction {
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let opts = slf.opts.clone();
        let conn = slf.conn.clone();
        let guard = slf.guard.clone();
        let inner = slf.inner.clone();
        let slf: Py<AsyncTransaction> = slf.into();

        rust_future_into_py(py, async move {
            let mut conn = conn.write().await;
            let mut guard = guard.write().await;
            let mut inner = inner.write().await;

            // check if transaction is already inflight
            if inner.is_some() {
                panic!("panic");
            }

            let tx = conn
                .as_mut()
                .unwrap()
                .start_transaction(opts)
                .await
                .unwrap();

            // As long as we hold Arc<Conn>, mysql_async::Transaction is valid.
            // inner is declared before conn so that Arc<Transaction> drops first.
            *inner = Some(unsafe {
                std::mem::transmute::<mysql_async::Transaction<'_>, mysql_async::Transaction<'static>>(
                    tx,
                )
            });

            // As long as we hold Arc<Conn>, RwLockWriteGuard is valid.
            // guard is declared before conn so that Arc<Guard> drops first.
            *guard = Some(unsafe {
                std::mem::transmute::<
                    RwLockWriteGuard<'_, _>,
                    RwLockWriteGuard<'static, Option<mysql_async::Conn>>,
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
            eprintln!(
                "Warning: AsyncTransaction reference count is {} (expected 2) in __aexit__. Transaction may be referenced elsewhere.",
                refcnt
            );
        }

        let guard = slf.borrow().guard.clone();
        let inner = slf.borrow().inner.clone();
        rust_future_into_py(py, async move {
            // TODO: check if  is not called and normally exiting without exception

            let mut guard = guard.write().await;
            let mut inner = inner.write().await;

            if let Some(inner) = inner.take() {
                eprintln!("commit() or rollback() is not called. rolling back.");
                inner.rollback().await.unwrap(); // TODO: unwrap to error
                // Automatic rollback failed. The connection will rollback. Please close the current connection and start with new connection.
            }
            *guard = None;
            Ok(())
        })
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        let guard = self.guard.clone();
        rust_future_into_py(py, async move {
            let inner = inner
                .write()
                .await
                .take()
                .ok_or_else(|| Error::TransactionClosedError)?;
            // At this point, no new operation on Transaction can be started

            // TODO: wait for other concurrent ops
            // Transaction is not yet thread-safe due to this

            // Drop the RwLockGuard on conn
            guard.write().await.take();

            Ok(inner.commit().await?)
        })
    }
    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        let guard = self.guard.clone();
        rust_future_into_py(py, async move {
            let inner = inner
                .write()
                .await
                .take()
                .ok_or_else(|| Error::TransactionClosedError)?;

            // Drop the RwLockGuard on conn
            guard.write().await.take();

            Ok(inner.rollback().await?)
        })
    }

    fn affected_rows<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            Ok(inner
                .read()
                .await
                .as_ref()
                .ok_or_else(|| Error::TransactionClosedError)?
                .affected_rows())
        })
    }

    // ─── Queryable ───────────────────────────────────────────────────────
    fn close_prepared_statement<'py>(
        &self,
        _py: Python<'py>,
        _stmt: String,
    ) -> PyResult<Py<PyroFuture>> {
        todo!()
    }
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        self.inner.ping(py)
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.inner.query(py, query)
    }
    fn query_first<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.inner.query_first(py, query)
    }
    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.inner.query_drop(py, query)
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec(py, query, params)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_first(py, query, params)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_drop(py, query, params)
    }
    #[pyo3(signature = (query, params=vec![]))]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Vec<Params>,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_batch(py, query, params)
    }
}
