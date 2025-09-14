use std::sync::Arc;

use color_eyre::eyre::ContextCompat;
use mysql_async::Opts;
use pyo3::prelude::*;
use tokio::sync::RwLock;

use crate::isolation_level::IsolationLevel;
use crate::params::Params;
use crate::queryable::Queryable;
use crate::row::Row;
use crate::transaction::Transaction;
use crate::util::{mysql_error_to_pyerr, url_error_to_pyerr};
use color_eyre::Result;

#[pyclass]
/// ### Concurrency
/// The API is thread-safe. The underlying implementation is protected by RwLock.
/// Conn.exec_*() receives &mut self, so there is at most one statement being executed at any point.
pub struct Conn {
    pub inner: Arc<RwLock<Option<mysql_async::Conn>>>, // Although mysql_async::Conn is already Send + Sync, the field can be only accessed via GIL if it's without Arc.
}

#[pymethods]
impl Conn {
    #[new]
    fn _new() -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyException, _>(
            "Please use `await Conn.new(url) instead of Conn().`.",
        ))
    }

    #[staticmethod]
    fn new<'py>(py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?;
        pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals, async move {
            Ok(Self {
                inner: Arc::new(RwLock::new(Some(
                    mysql_async::Conn::new(Opts::from_url(&url).map_err(url_error_to_pyerr)?)
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
    ) -> Transaction {
        let isolation_level: Option<mysql_async::IsolationLevel> =
            isolation_level.map(|l| mysql_async::IsolationLevel::from(&*l));
        let mut opts = mysql_async::TxOpts::new();
        opts.with_consistent_snapshot(consistent_snapshot)
            .with_isolation_level(isolation_level)
            .with_readonly(readonly);
        Transaction::new(self.inner.clone(), opts)
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
