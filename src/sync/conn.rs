use either::Either;
use mysql::{AccessMode, Opts, prelude::*};
use pyo3::prelude::*;

use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::params::Params;
use crate::row::Row;
use crate::sync::iterator::ResultSetIterator;
use crate::sync::opts::SyncOpts;
use crate::sync::transaction::SyncTransaction;

#[pyclass]
pub struct SyncConn {
    pub inner: Option<mysql::Conn>,
}

#[pymethods]
impl SyncConn {
    #[new]
    fn new(url_or_opts: Either<String, PyRef<SyncOpts>>) -> PyResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?,
            Either::Right(opts) => opts.opts.clone(),
        };
        let conn = mysql::Conn::new(opts)
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
    ) -> PyResult<Py<PyAny>> {
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

        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?;
        let tx = inner.start_transaction(opts).map_err(Error::from)?;

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

    fn id(&self) -> PyroResult<u32> {
        Ok(self
            .inner
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .connection_id())
    }

    fn affected_rows(&self) -> PyResult<u64> {
        let conn = self.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Connection is not available")
        })?;
        Ok(conn.affected_rows())
    }

    fn last_insert_id(&self) -> PyroResult<Option<u64>> {
        match self
            .inner
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .last_insert_id()
        {
            0 => Ok(None),
            x => Ok(Some(x)),
        }
    }

    fn ping(&mut self) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .ping()?)
    }

    // ─── Text Protocol ───────────────────────────────────────────────────

    #[pyo3(signature = (query))]
    fn query(&mut self, query: String) -> PyroResult<Vec<Row>> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .query(query)?)
    }

    #[pyo3(signature = (query))]
    fn query_first(&mut self, query: String) -> PyroResult<Option<Row>> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .query_first(query)?)
    }

    #[pyo3(signature = (query))]
    fn query_drop(&mut self, query: String) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .query_drop(query)?)
    }
    #[pyo3(signature = (query))]
    fn query_iter(slf: Py<Self>, query: String) -> PyroResult<ResultSetIterator> {
        Python::attach(|py| {
            let mut slf_ref = slf.borrow_mut(py);
            let query_result = slf_ref
                .inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .query_iter(query)?;

            Ok(ResultSetIterator {
                owner: slf.clone_ref(py).into_any(),
                inner: Either::Left(unsafe { std::mem::transmute(query_result) }),
            })
        })
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec(&mut self, query: String, params: Params) -> PyroResult<Vec<Row>> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .exec(query, params)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first(&mut self, query: String, params: Params) -> PyroResult<Option<Row>> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .exec_first(query, params)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&mut self, query: String, params: Params) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .exec_drop(query, params)?)
    }

    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&mut self, query: String, params_list: Vec<Params>) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .exec_batch(query, params_list)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_iter(slf: Py<Self>, query: String, params: Params) -> PyroResult<ResultSetIterator> {
        Python::attach(|py| {
            let mut slf_ref = slf.borrow_mut(py);
            let query_result = slf_ref
                .inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .exec_iter(query, params)?;

            Ok(ResultSetIterator {
                owner: slf.clone_ref(py).into_any(),
                inner: Either::Right(unsafe { std::mem::transmute(query_result) }),
            })
        })
    }

    fn close(&mut self) -> PyroResult<()> {
        self.inner.take();
        Ok(())
    }

    fn disconnect(&mut self) -> PyroResult<()> {
        self.inner.take();
        Ok(())
    }

    fn reset(&mut self) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .reset()?)
    }

    fn server_version(&self) -> PyroResult<(u16, u16, u16)> {
        Ok(self
            .inner
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .server_version())
    }
}
