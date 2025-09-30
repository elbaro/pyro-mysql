use either::Either;
use mysql::{AccessMode, prelude::*};
use parking_lot::RwLock;
use pyo3::prelude::*;

use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::params::Params;
use crate::row::Row;
use crate::sync::iterator::ResultSetIterator;
use crate::sync::transaction::SyncTransaction;

#[pyclass]
pub struct SyncPooledConn {
    pub inner: RwLock<Option<mysql::PooledConn>>,
}

#[pymethods]
impl SyncPooledConn {
    #[new]
    fn __init__() -> PyResult<Self> {
        Err(PyErr::new::<pyo3::exceptions::PyException, _>(
            "SyncPooledConn cannot be instantiated directly. Use SyncPool.get_conn() or SyncPool.acquire().",
        ))
    }

    #[pyo3(signature=(consistent_snapshot=false, isolation_level=None, readonly=None))]
    fn start_transaction(
        slf: Py<Self>,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> PyroResult<SyncTransaction> {
        Python::attach(|py| {
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
            Ok(SyncTransaction::new(Either::Right(slf.clone_ref(py)), opts))
        })
    }

    #[pyo3(signature=(callable, consistent_snapshot=false, isolation_level=None, readonly=None))]
    fn run_transaction(
        slf: Py<Self>,
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

        Ok(Python::attach(|py| {
            callable.call1(
                py,
                (SyncTransaction::new(Either::Right(slf.clone_ref(py)), opts),),
            )
        })?)
    }

    fn affected_rows(&self) -> PyResult<u64> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Connection is not available")
        })?;
        Ok(conn.affected_rows())
    }

    fn ping(&mut self) -> PyroResult<()> {
        Ok(self
            .inner
            .write()
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .as_mut()
            .ping()?)
    }

    // ─── Text Protocol ───────────────────────────────────────────────────

    fn query(&mut self, query: String) -> PyroResult<Vec<Row>> {
        Ok(self
            .inner
            .write()
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .query(query)?)
    }

    fn query_first(&mut self, query: String) -> PyroResult<Option<Row>> {
        Ok(self
            .inner
            .write()
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .query_first(query)?)
    }

    fn query_drop(&mut self, query: String) -> PyroResult<()> {
        Ok(self
            .inner
            .write()
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .query_drop(query)?)
    }

    fn query_iter(slf: Py<Self>, query: String) -> PyroResult<ResultSetIterator> {
        Python::attach(|py| {
            let slf_ref = slf.borrow(py);
            let mut guard = slf_ref.inner.write();
            let query_result = guard
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
            .write()
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .exec(query, params)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first(&mut self, query: String, params: Params) -> PyroResult<Option<Row>> {
        Ok(self
            .inner
            .write()
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .exec_first(query, params)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&mut self, query: String, params: Params) -> PyroResult<()> {
        Ok(self
            .inner
            .write()
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .exec_drop(query, params)?)
    }

    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&mut self, query: String, params_list: Vec<Params>) -> PyroResult<()> {
        Ok(self
            .inner
            .write()
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .exec_batch(query, params_list)?)
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
        let query_result = guard
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .exec_iter(query, params)?;

        Ok(ResultSetIterator {
            owner: slf.clone_ref(py).into_any(),
            inner: Either::Right(unsafe { std::mem::transmute(query_result) }),
        })
    }

    fn close(&mut self) -> PyResult<()> {
        self.inner.write().take();
        Ok(())
    }
}
