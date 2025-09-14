use color_eyre::{Result, eyre::ContextCompat};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::{params::Params, queryable::Queryable, row::Row};

// struct fields are dropped in the same order as declared in the struct
#[pyclass]
pub struct Transaction {
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

impl Transaction {
    pub fn new(conn: Arc<RwLock<Option<mysql_async::Conn>>>, opts: mysql_async::TxOpts) -> Self {
        Transaction {
            opts,
            conn,
            guard: Default::default(),
            inner: Default::default(),
        }
    }
}

// Order or lock: conn -> conn guard -> inner
#[pymethods]
impl Transaction {
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?;

        let opts = slf.opts.clone();
        let conn = slf.conn.clone();
        let guard = slf.guard.clone();
        let inner = slf.inner.clone();
        let slf: Py<Transaction> = slf.into();

        pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals, async move {
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
        &self,
        py: Python<'py>,
        _exc_type: &crate::Bound<'py, crate::PyAny>,
        _exc_value: &crate::Bound<'py, crate::PyAny>,
        _traceback: &crate::Bound<'py, crate::PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?;
        let guard = self.guard.clone();
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals, async move {
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

    async fn commit(&self) -> Result<()> {
        let inner = self.inner.clone();
        let guard = self.guard.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let inner = inner
                    .write()
                    .await
                    .take()
                    .context("transaction is already closed")?;
                // At this point, no new operation on Transaction can be started

                // TODO: wait for other concurrent ops
                // Transaction is not yet thread-safe due to this

                // Drop the RwLockGuard on conn
                guard.write().await.take();

                Ok(inner.commit().await?)
            })
            .await?
    }
    async fn rollback(&self) -> Result<()> {
        let inner = self.inner.clone();
        let guard = self.guard.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let inner = inner
                    .write()
                    .await
                    .take()
                    .context("transaction is already closed")?;

                // Drop the RwLockGuard on conn
                guard.write().await.take();

                Ok(inner.rollback().await?)
            })
            .await?
    }

    async fn affected_rows(&self) -> Result<u64> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .context("Conn is already closed")?
            .affected_rows())
    }

    // ─── Queryable ───────────────────────────────────────────────────────
    async fn close_prepared_statement(&self, _stmt: String) -> Result<()> {
        todo!()
    }
    async fn ping(&self) -> Result<()> {
        self.inner.ping().await
    }
    #[pyo3(signature = (query, params=Params::default()))]
    async fn exec(&self, query: String, params: Params) -> Result<Vec<Row>> {
        self.inner.exec(query, params).await
    }
    #[pyo3(signature = (query, params=Params::default()))]
    async fn exec_first(&self, query: String, params: Params) -> Result<Option<Row>> {
        self.inner.exec_first(query, params).await
    }
    #[pyo3(signature = (query, params=Params::default()))]
    async fn exec_drop(&self, query: String, params: Params) -> Result<()> {
        self.inner.exec_drop(query, params).await
    }
    #[pyo3(signature = (query, params=vec![]))]
    async fn exec_batch(&self, query: String, params: Vec<Params>) -> Result<()> {
        self.inner.exec_batch(query, params).await
    }
}
