use either::Either;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::r#async::opts::AsyncOpts;
use crate::r#async::queryable::Queryable;
use crate::r#async::transaction::AsyncTransaction;
use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::params::Params;
use crate::util::{PyroFuture, rust_future_into_py, url_error_to_pyerr};

#[pyclass(module = "pyro_mysql.async_", name = "Conn")]
pub struct AsyncConn {
    pub inner: Arc<RwLock<Option<mysql_async::Conn>>>, // Although mysql_async::Conn is already Send + Sync, the field can be only accessed via GIL if it's without Arc.
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
        let opts = match url_or_opts {
            Either::Left(url) => mysql_async::Opts::from_url(&url).map_err(url_error_to_pyerr)?,
            Either::Right(opts) => opts.opts.clone(),
        };
        rust_future_into_py(py, async move {
            Ok(Self {
                inner: Arc::new(RwLock::new(Some(mysql_async::Conn::new(opts).await?))),
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

    async fn id(&self) -> PyResult<u32> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .id())
    }

    async fn affected_rows(&self) -> PyResult<u64> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .affected_rows())
    }

    async fn last_insert_id(&self) -> PyResult<Option<u64>> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .last_insert_id())
    }
    async fn close(&self) -> PyroResult<()> {
        let mut inner = self.inner.write().await;
        if let Some(conn) = inner.take() {
            conn.disconnect().await?;
        }
        Ok(())
    }
    async fn reset(&self) -> PyroResult<()> {
        let mut inner = self.inner.write().await;
        inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .reset()
            .await?;
        Ok(())
    }

    fn server_version<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            Ok(inner
                .read()
                .await
                .as_ref()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .server_version())
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
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec(py, query, params)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_first(py, query, params)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_drop(py, query, params)
    }
    #[pyo3(signature = (query, params=vec![]))]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Params>,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_batch(py, query, params)
    }
}
