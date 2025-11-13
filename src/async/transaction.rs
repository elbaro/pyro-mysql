use pyo3::{prelude::*, pybacked::PyBackedStr};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::{
    r#async::multi_conn::MultiAsyncConn,
    r#async::queryable::Queryable,
    error::Error,
    util::{PyroFuture, rust_future_into_py},
};

// struct fields are dropped in the same order as declared in the struct
#[pyclass(module = "pyro_mysql.async_", name = "Transaction")]
pub struct AsyncTransaction {
    opts: mysql_async::TxOpts,

    /// `Option<Transaction>` is initialized in __aenter__.
    /// It is reset on commit(), rollback(), or __aexit__.
    inner: Arc<RwLock<Option<mysql_async::Transaction<'static>>>>,

    /// Holding this guard prevents other concurrent calls of Conn::some_method(&mutable self).
    /// guard is initialized in __aenter__.
    /// It is reset on commit(), rollback(), or __aexit__.
    guard: Arc<RwLock<Option<tokio::sync::RwLockWriteGuard<'static, Option<MultiAsyncConn>>>>>,

    conn: Arc<RwLock<Option<MultiAsyncConn>>>,
}

impl AsyncTransaction {
    pub fn new(conn: Arc<RwLock<Option<MultiAsyncConn>>>, opts: mysql_async::TxOpts) -> Self {
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

            // Check if connection is wtx and panic
            let conn_ref = conn.as_mut().unwrap(); // Conn is already non-None
            if matches!(conn_ref, MultiAsyncConn::Wtx(_)) {
                panic!("Transactions are not supported for wtx connections");
            }

            // Only mysql_async supports transactions
            let mysql_conn = match conn_ref {
                MultiAsyncConn::MysqlAsync(c) => c,
                MultiAsyncConn::Wtx(_) => unreachable!(),
                MultiAsyncConn::ZeroMysql(_) => todo!("zero_mysql does not support transactions yet"),
            };

            let tx = mysql_conn
                .start_transaction(opts)
                .await
                .map_err(Error::from)?;

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
                    RwLockWriteGuard<'static, Option<MultiAsyncConn>>,
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
        let inner = slf.borrow().inner.clone();
        rust_future_into_py(py, async move {
            // TODO: check if  is not called and normally exiting without exception

            let mut guard = guard.write().await;
            let mut inner = inner.write().await;

            if let Some(inner) = inner.take() {
                log::warn!("commit() or rollback() is not called. rolling back.");
                inner.rollback().await.map_err(Error::from)?;
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
    #[pyo3(signature = (query, *, as_dict=false))]
    fn query<'py>(&self, py: Python<'py>, query: String, as_dict: bool) -> PyResult<Py<PyroFuture>> {
        self.inner.query(py, query, as_dict)
    }
    #[pyo3(signature = (query, *, as_dict=false))]
    fn query_first<'py>(&self, py: Python<'py>, query: String, as_dict: bool) -> PyResult<Py<PyroFuture>> {
        self.inner.query_first(py, query, as_dict)
    }
    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.inner.query_drop(py, query)
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────
    #[pyo3(signature = (query, params=None, *, as_dict=false))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec(py, query, params, as_dict)
    }
    #[pyo3(signature = (query, params=None, *, as_dict=false))]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec_first(py, query, params, as_dict)
    }
    #[pyo3(signature = (query, params=None))]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec_drop(py, query, params)
    }
    #[pyo3(signature = (query, params=vec![]))]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_batch(py, query, params)
    }
}
