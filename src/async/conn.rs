use std::sync::Arc;

use color_eyre::eyre::ContextCompat;
use either::Either;
use mysql_async::Opts;
use pyo3::prelude::*;
use tokio::sync::RwLock;

use crate::r#async::AsyncOpts;
use crate::r#async::queryable::Queryable;
use crate::r#async::transaction::AsyncTransaction;
use crate::isolation_level::IsolationLevel;
use crate::params::Params;
use crate::util::{PyroFuture, mysql_error_to_pyerr, rust_future_into_py, url_error_to_pyerr};
use color_eyre::Result;

#[pyclass]
/// ### Concurrency
/// The API is thread-safe. The underlying implementation is protected by RwLock.
/// Conn.exec_*() receives &mut self, so there is at most one statement being executed at any point.
pub struct AsyncConn {
    pub inner: Arc<RwLock<Option<mysql_async::Conn>>>, // Although mysql_async::Conn is already Send + Sync, the field can be only accessed via GIL if it's without Arc.
}

#[pymethods]
impl AsyncConn {
    #[new]
    fn _new() -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyException, _>(
            "Please use `await Conn.new(url) instead of Conn().`.",
        ))
    }

    #[staticmethod]
    fn new<'py>(
        py: Python<'py>,
        url_or_opts: Either<String, PyRef<AsyncOpts>>,
    ) -> PyResult<Py<PyroFuture>> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url).map_err(url_error_to_pyerr)?,
            Either::Right(opts) => opts.opts.clone(),
        };

        rust_future_into_py(py, async move {
            Ok(Self {
                inner: Arc::new(RwLock::new(Some(
                    mysql_async::Conn::new(opts)
                        .await
                        .map_err(mysql_error_to_pyerr)?,
                ))),
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
        let isolation_level: Option<mysql_async::IsolationLevel> =
            isolation_level.map(|l| mysql_async::IsolationLevel::from(&*l));
        let mut opts = mysql_async::TxOpts::new();
        opts.with_consistent_snapshot(consistent_snapshot)
            .with_isolation_level(isolation_level)
            .with_readonly(readonly);
        AsyncTransaction::new(self.inner.clone(), opts)
    }

    async fn id(&self) -> Result<u32> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .context("Conn is already closed")?
            .id())
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

    async fn last_insert_id(&self) -> Result<Option<u64>> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .context("Conn is already closed")?
            .last_insert_id())
    }

    // ─── Queryable ───────────────────────────────────────────────────────
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

    async fn disconnect(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        if let Some(conn) = inner.take() {
            conn.disconnect().await?;
        }
        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        if let Some(conn) = inner.as_mut() {
            conn.reset().await?;
        }
        Ok(())
    }

    fn server_version<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            Ok(inner
                .read()
                .await
                .as_ref()
                .context("Connection is not available")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .server_version())
        })
    }
}
