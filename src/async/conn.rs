use either::Either;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use wtx::database::Executor;

use crate::r#async::opts::AsyncOpts;
use crate::r#async::queryable::Queryable;
use crate::r#async::transaction::AsyncTransaction;
use crate::r#async::wtx_types::{StatementCache, WtxConn, WtxExecutor};
use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::util::{PyroFuture, rust_future_into_py};

#[pyclass(module = "pyro_mysql.async_", name = "Conn")]
pub struct AsyncConn {
    pub inner: Arc<RwLock<Option<WtxExecutor>>>,
    pub stmt_cache: Arc<RwLock<StatementCache>>,
}

#[pymethods]
impl AsyncConn {
    // ─── Connection Management ───────────────────────────────────────────
    #[new]
    fn _new() -> PyroResult<Self> {
        Err(Error::IncorrectApiUsageError(
            "use `await Conn.new(url) instead of Conn()`.",
        ))
    }

    #[allow(clippy::new_ret_no_self)]
    #[staticmethod]
    pub fn new<'py>(
        py: Python<'py>,
        url_or_opts: Either<String, PyRef<AsyncOpts>>,
    ) -> PyResult<Py<PyroFuture>> {
        // For now, only support URL strings since wtx doesn't have OptsBuilder equivalent
        let url = match url_or_opts {
            Either::Left(url) => url,
            Either::Right(_opts) => {
                return Err(Error::IncorrectApiUsageError(
                    "AsyncOpts not yet supported with wtx backend, use URL string instead",
                ).into());
            }
        };

        rust_future_into_py(py, async move {
            let wtx_conn = WtxConn::connect(&url)
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            Ok(Self {
                inner: wtx_conn.executor,
                stmt_cache: Arc::new(RwLock::new(StatementCache::new())),
            })
        })
    }

    #[pyo3(signature = (consistent_snapshot=false, isolation_level=None, readonly=None))]
    fn start_transaction(
        &self,
        consistent_snapshot: bool,
        isolation_level: Option<PyRef<IsolationLevel>>,
        readonly: Option<bool>,
    ) -> AsyncTransaction {
        AsyncTransaction::new(
            self.inner.clone(),
            self.stmt_cache.clone(),
            consistent_snapshot,
            isolation_level.map(|l| l.clone()),
            readonly,
        )
    }

    async fn id(&self) -> PyResult<u32> {
        // wtx doesn't expose connection ID directly
        // Return 0 for now as a placeholder
        Ok(0)
    }

    async fn affected_rows(&self) -> PyResult<u64> {
        // wtx returns affected rows per query, not as connection state
        // Return 0 for now
        Ok(0)
    }

    async fn last_insert_id(&self) -> PyResult<Option<u64>> {
        // wtx doesn't expose last_insert_id as connection state
        // Return None for now
        Ok(None)
    }

    async fn close(&self) -> PyroResult<()> {
        let mut inner = self.inner.write().await;
        if inner.take().is_some() {
            // wtx executor drops the connection when dropped
        }
        Ok(())
    }

    async fn reset(&self) -> PyroResult<()> {
        let mut executor = self.inner.write().await;
        let exec = executor
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?;

        // Reset connection using MySQL RESET CONNECTION statement (MySQL 5.7.3+)
        // This clears temporary tables, user variables, prepared statements, etc.
        // For older versions, we could fall back to individual RESET commands
        exec.execute("RESET CONNECTION", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
            .await
            .map_err(|e| Error::WtxError(e.to_string()))?;

        // Clear the statement cache after reset since all prepared statements are invalidated
        let mut cache = self.stmt_cache.write().await;
        cache.clear();
        drop(cache);

        Ok(())
    }

    fn server_version<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            let _inner = inner.read().await;
            // wtx doesn't expose server_version directly
            // Return a tuple for now (0, 0, 0)
            Ok((0u16, 0u16, 0u16))
        })
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
    #[pyo3(signature = (query, params=None))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec(py, query, params, self.stmt_cache.clone())
    }

    #[pyo3(signature = (query, params=None))]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec_first(py, query, params, self.stmt_cache.clone())
    }

    #[pyo3(signature = (query, params=None))]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec_drop(py, query, params, self.stmt_cache.clone())
    }

    #[pyo3(signature = (query, params=vec![]))]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_batch(py, query, params, self.stmt_cache.clone())
    }
}
