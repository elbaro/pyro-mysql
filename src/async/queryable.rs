use color_eyre::eyre::ContextCompat;
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    params::Params,
    row::Row,
    util::{PyroFuture, rust_future_into_py},
};

/// This trait implements the common methods between Conn, Connection, Transaction.
///
/// All methods return RaiiFuture for Python async integration

pub trait Queryable {
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>>;
    // fn prep(&self, query: String) -> PyResult<Py<RaiiFuture>>; // TODO
    fn close_prepared_statement<'py>(
        &self,
        py: Python<'py>,
        stmt: mysql_async::Statement,
    ) -> PyResult<Py<PyroFuture>>;

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>>;
    fn query_first<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>>;
    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>>;

    // ─── Binary Protocol ─────────────────────────────────────────────────
    fn exec<'py>(&self, py: Python<'py>, query: String, params: Params)
    -> PyResult<Py<PyroFuture>>;
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyResult<Py<PyroFuture>>;
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyResult<Py<PyroFuture>>;
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Vec<Params>,
    ) -> PyResult<Py<PyroFuture>>;
    // fn exec_iter<'py>(&self, py: Python<'py>, query: String, params: Params) -> PyResult<Py<RaiiFuture>>;
}

impl<T: mysql_async::prelude::Queryable + Send + Sync + 'static> Queryable
    for Arc<RwLock<Option<T>>>
{
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py(py, async move {
            let mut inner = inner.write().await;
            inner
                .as_mut()
                .context("connection is already closed")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .ping()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
        })
    }

    fn close_prepared_statement<'py>(
        &self,
        py: Python<'py>,
        stmt: mysql_async::Statement,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py(py, async move {
            let mut inner = inner.write().await;
            inner
                .as_mut()
                .context("connection is already closed")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .close(stmt)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
        })
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Vec<Row>>(py, async move {
            let mut inner = inner.write().await;
            inner
                .as_mut()
                .context("connection is already closed")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .query(query)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
        })
    }

    fn query_first<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Option<Row>>(py, async move {
            let mut inner = inner.write().await;
            inner
                .as_mut()
                .context("connection is already closed")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .query_first(query)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
        })
    }

    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            inner
                .as_mut()
                .context("connection is already closed")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .query_drop(query)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
        })
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Vec<Row>>(py, async move {
            let mut inner = inner.write().await;
            inner
                .as_mut()
                .context("connection is already closed")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .exec(query, params)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
        })
    }

    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Option<Row>>(py, async move {
            let mut inner = inner.write().await;
            inner
                .as_mut()
                .context("connection is already closed")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .exec_first(query, params)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
        })
    }

    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            inner
                .as_mut()
                .context("connection is already closed")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .exec_drop(query, params)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
        })
    }

    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Vec<Params>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            inner
                .as_mut()
                .context("connection is already closed")
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
                .exec_batch(query, params)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))
        })
    }

    // fn exec_iter<'py>(&self, py: Python<'py>, query: String, params: Params) -> PyResult<Py<RaiiFuture>> {
    //     let inner = self.clone();
    //     rust_future_into_py(py, async move {
    //         let mut inner = inner.write().await;
    //         Ok(RowStream::new(inner
    //             .as_mut()
    //             .context("connection is already closed")
    //             .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?
    //             .exec_iter(query, params)
    //             .await
    //             .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?))
    //     })
    // }
}
