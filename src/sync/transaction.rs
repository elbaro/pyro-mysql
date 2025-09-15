use color_eyre::{Result, eyre::ContextCompat};
use mysql::prelude::Queryable;
use pyo3::prelude::*;

use crate::{params::Params, row::Row};
use crate::sync::iterator::ResultSetIterator;

#[pyclass]
pub struct SyncTransaction {
    pub inner: Option<mysql::Transaction<'static>>,
}

#[pymethods]
impl SyncTransaction {
    fn commit(&mut self) -> Result<()> {
        let inner = self.inner.take().context("transaction is already closed")?;
        Ok(inner.commit()?)
    }
    fn rollback(&mut self) -> Result<()> {
        let inner = self.inner.take().context("transaction is already closed")?;
        Ok(inner.rollback()?)
    }
    fn affected_rows(&self) -> PyResult<u64> {
        let inner = self.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Connection is not available")
        })?;
        Ok(inner.affected_rows())
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    
    fn query(&mut self, query: String) -> Result<Vec<Row>> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .query(query)?)
    }
    
    fn query_first(&mut self, query: String) -> Result<Option<Row>> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .query_first(query)?)
    }
    
    fn query_drop(&mut self, query: String) -> Result<()> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .query_drop(query)?)
    }
    
    fn query_iter(slf: Py<Self>, query: String) -> Result<ResultSetIterator> {
        Python::attach(|py| {
            let mut slf_ref = slf.borrow_mut(py);
            let query_result = slf_ref
                .inner
                .as_mut()
                .context("Connection is not available")?
                .query_iter(query)?;
            
            Ok(ResultSetIterator {
                owner: slf.clone_ref(py).into_any(),
                inner: unsafe { std::mem::transmute(query_result) },
            })
        })
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec(&mut self, query: String, params: Params) -> Result<Vec<Row>> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .exec(query, params.inner)?)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first(&mut self, query: String, params: Params) -> Result<Option<Row>> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .exec_first(query, params.inner)?)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&mut self, query: String, params: Params) -> Result<()> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .exec_drop(query, params.inner)?)
    }
    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&mut self, query: String, params_list: Vec<Params>) -> Result<()> {
        Ok(self
            .inner
            .as_mut()
            .context("Connection is not available")?
            .exec_batch(query, params_list)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_iter(slf: Py<Self>, query: String, params: Params) -> Result<ResultSetIterator> {
        Python::attach(|py| {
            let mut slf_ref = slf.borrow_mut(py);
            let query_result = slf_ref
                .inner
                .as_mut()
                .context("Connection is not available")?
                .exec_iter(query, params)?;
            
            Ok(ResultSetIterator {
                owner: slf.clone_ref(py).into_any(),
                inner: unsafe { std::mem::transmute(query_result) },
            })
        })
    }
}
