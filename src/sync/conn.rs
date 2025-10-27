use either::Either;
use mysql::{AccessMode, Opts, prelude::*};
use parking_lot::RwLock;
use pyo3::prelude::*;

use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::params::Params;
use crate::row::Row;
use crate::sync::iterator::ResultSetIterator;
use crate::sync::opts::SyncOpts;
use crate::sync::transaction::SyncTransaction;

#[pyclass(module = "pyro_mysql.sync", name = "Conn")]
pub struct SyncConn {
    pub inner: RwLock<Option<mysql::Conn>>,
}

#[pymethods]
impl SyncConn {
    #[new]
    pub fn new(url_or_opts: Either<String, PyRef<SyncOpts>>) -> PyroResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url)?,
            Either::Right(opts) => opts.opts.clone(),
        };
        let conn = mysql::Conn::new(opts)?;

        Ok(Self {
            inner: RwLock::new(Some(conn)),
        })
    }

    #[pyo3(signature=(consistent_snapshot=false, isolation_level=None, readonly=None))]
    fn start_transaction(
        slf: Py<Self>,
        py: Python,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> PyroResult<SyncTransaction> {
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

        Ok(SyncTransaction::new(Either::Left(slf.clone_ref(py)), opts))
    }

    fn id(&self) -> PyroResult<u32> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.connection_id())
    }

    fn affected_rows(&self) -> PyResult<u64> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Connection is not available")
        })?;
        Ok(conn.affected_rows())
    }

    fn last_insert_id(&self) -> PyroResult<Option<u64>> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
        match conn.last_insert_id() {
            0 => Ok(None),
            x => Ok(Some(x)),
        }
    }

    fn ping(&self) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.ping()?)
    }

    // ─── Text Protocol ───────────────────────────────────────────────────

    #[pyo3(signature = (query))]
    fn query(&self, query: String) -> PyroResult<Vec<Row>> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.query(query)?)
    }

    #[pyo3(signature = (query))]
    fn query_first(&self, query: String) -> PyroResult<Option<Row>> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.query_first(query)?)
    }

    #[pyo3(signature = (query))]
    fn query_drop(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.query_drop(query)?)
    }
    #[pyo3(signature = (query))]
    fn query_iter(slf: Py<Self>, py: Python, query: String) -> PyroResult<ResultSetIterator> {
        let slf_ref = slf.borrow(py);
        let mut guard = slf_ref.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let query_result = conn.query_iter(query)?;

        Ok(ResultSetIterator {
            owner: slf.clone_ref(py).into_any(),
            inner: Either::Left(unsafe {
                std::mem::transmute::<
                    mysql::QueryResult<'_, '_, '_, mysql::Text>,
                    mysql::QueryResult<'_, '_, '_, mysql::Text>,
                >(query_result)
            }),
        })
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec(&self, query: String, params: Params) -> PyroResult<Vec<Row>> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("exec {query}");
        Ok(conn.exec(query, params)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first(&self, query: String, params: Params) -> PyroResult<Option<Row>> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("exec_first {query}");
        Ok(conn.exec_first(query, params)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&self, query: String, params: Params) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("exec_drop {query}");
        Ok(conn.exec_drop(query, params)?)
    }

    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&self, query: String, params_list: Vec<Params>) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("exec_batch {query}");
        Ok(conn.exec_batch(query, params_list)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_iter(
        slf: Py<Self>,
        py: Python,
        query: String,
        params: Params,
    ) -> PyroResult<ResultSetIterator> {
        let slf_ref = slf.borrow(py);
        let mut guard = slf_ref.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        log::debug!("exec_iter {query}");
        let query_result = conn.exec_iter(query, params)?;
        Ok(ResultSetIterator {
            owner: slf.clone_ref(py).into_any(),
            inner: Either::Right(unsafe {
                std::mem::transmute::<
                    mysql::QueryResult<'_, '_, '_, mysql::Binary>,
                    mysql::QueryResult<'_, '_, '_, mysql::Binary>,
                >(query_result)
            }),
        })
    }

    pub fn close(&self) {
        *self.inner.write() = None;
    }

    fn reset(&self) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.reset()?)
    }

    fn server_version(&self) -> PyroResult<(u16, u16, u16)> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.server_version())
    }
}
