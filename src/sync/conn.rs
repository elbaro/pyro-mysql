use color_eyre::Result;
use color_eyre::eyre::ContextCompat;
use mysql::{AccessMode, Conn as MysqlConn, Opts, prelude::*};
use pyo3::prelude::*;

use crate::isolation_level::IsolationLevel;
use crate::params::Params;
use crate::row::Row;
use crate::sync::transaction::SyncTransaction;

#[pyclass]
pub struct SyncConn {
    pub inner: Option<MysqlConn>,
}

#[pymethods]
impl SyncConn {
    #[new]
    fn new(url: String) -> PyResult<Self> {
        let opts = Opts::from_url(&url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let conn = MysqlConn::new(opts)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner: Some(conn) })
    }

    #[pyo3(signature=(callable, consistent_snapshot=false, isolation_level=None, readonly=None))]
    fn run_transaction(
        &mut self,
        callable: Py<PyAny>,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> Result<Py<PyAny>> {
        let isolation_level: Option<mysql::IsolationLevel> =
            isolation_level.map(|l| mysql::IsolationLevel::from(&l));
        let opts = mysql::TxOpts::default()
            .set_with_consistent_snapshot(consistent_snapshot)
            .set_isolation_level(isolation_level)
            .set_access_mode(readonly.map(|flag| {
                if flag {
                    AccessMode::ReadOnly
                } else {
                    AccessMode::ReadWrite
                }
            }));

        let inner = self.inner.as_mut().context("Connection is not available")?;
        let tx = inner.start_transaction(opts)?;

        Ok(Python::attach(|py| {
            callable.call1(
                py,
                (SyncTransaction {
                    inner: Some(unsafe {
                        std::mem::transmute::<mysql::Transaction<'_>, mysql::Transaction<'static>>(
                            tx,
                        )
                    }),
                },),
            )
        })?)
    }

    fn affected_rows(&self) -> PyResult<u64> {
        let conn = self.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Connection is not available")
        })?;
        Ok(conn.affected_rows())
    }

    fn ping(&mut self) -> Result<()> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .ping()?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec(&mut self, query: String, params: Params) -> Result<Vec<Row>> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .exec(query, params)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first(&mut self, query: String, params: Params) -> Result<Option<Row>> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .exec_first(query, params)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&mut self, query: String, params: Params) -> Result<()> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .exec_drop(query, params)?)
    }

    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&mut self, query: String, params_list: Vec<Params>) -> Result<()> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .exec_batch(query, params_list)?)
    }

    fn close(&mut self) -> PyResult<()> {
        self.inner.take();
        Ok(())
    }
}
