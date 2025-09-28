use either::Either;
use mysql::prelude::Queryable;
use pyo3::prelude::*;

use crate::error::{Error, PyroResult};
use crate::sync::iterator::ResultSetIterator;
use crate::{params::Params, row::Row};

#[pyclass]
pub struct SyncTransaction {
    pub inner: Option<mysql::Transaction<'static>>,
}

#[pymethods]
impl SyncTransaction {
    fn commit(&mut self) -> PyroResult<()> {
        let inner = self
            .inner
            .take()
            .ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(inner.commit()?)
    }
    fn rollback(&mut self) -> PyroResult<()> {
        let inner = self
            .inner
            .take()
            .ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(inner.rollback()?)
    }
    fn affected_rows(&self) -> PyResult<u64> {
        let inner = self.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Connection is not available")
        })?;
        Ok(inner.affected_rows())
    }

    // ─── Text Protocol ───────────────────────────────────────────────────

    fn query(&mut self, query: String) -> PyroResult<Vec<Row>> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query(query)?)
    }

    fn query_first(&mut self, query: String) -> PyroResult<Option<Row>> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query_first(query)?)
    }

    fn query_drop(&mut self, query: String) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query_drop(query)?)
    }

    fn query_iter(slf: Py<Self>, query: String) -> PyroResult<ResultSetIterator> {
        Python::attach(|py| {
            let mut slf_ref = slf.borrow_mut(py);
            let query_result = slf_ref
                .inner
                .as_mut()
                .ok_or_else(|| Error::TransactionClosedError)?
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
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec(query, params.inner)?)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first(&mut self, query: String, params: Params) -> PyroResult<Option<Row>> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec_first(query, params.inner)?)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&mut self, query: String, params: Params) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec_drop(query, params.inner)?)
    }
    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&mut self, query: String, params_list: Vec<Params>) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec_batch(query, params_list)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_iter(slf: Py<Self>, query: String, params: Params) -> PyroResult<ResultSetIterator> {
        Python::attach(|py| {
            let mut slf_ref = slf.borrow_mut(py);
            let query_result = slf_ref
                .inner
                .as_mut()
                .ok_or_else(|| Error::TransactionClosedError)?
                .exec_iter(query, params)?;

            Ok(ResultSetIterator {
                owner: slf.clone_ref(py).into_any(),
                inner: Either::Right(unsafe { std::mem::transmute(query_result) }),
            })
        })
    }
}
