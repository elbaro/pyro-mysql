use pyo3::{prelude::*, pybacked::PyBackedStr};
use std::sync::Arc;
use tokio::sync::RwLock;
use wtx::database::{Executor, Records};

use crate::{
    error::Error,
    util::{PyroFuture, rust_future_into_py},
    r#async::{
        row::Row,
        wtx_types::{StatementCache, WtxExecutor},
    },
};

/// This trait implements the common methods between Conn and Transaction.
pub trait Queryable {
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>>;

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>>;
    fn query_first<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>>;
    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>>;

    // ─── Binary Protocol ─────────────────────────────────────────────────
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        stmt_cache: Arc<RwLock<StatementCache>>,
    ) -> PyResult<Py<PyroFuture>>;
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        stmt_cache: Arc<RwLock<StatementCache>>,
    ) -> PyResult<Py<PyroFuture>>;
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        stmt_cache: Arc<RwLock<StatementCache>>,
    ) -> PyResult<Py<PyroFuture>>;
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
        stmt_cache: Arc<RwLock<StatementCache>>,
    ) -> PyResult<Py<PyroFuture>>;
}

impl Queryable for Arc<RwLock<Option<WtxExecutor>>> {
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py(py, async move {
            let mut executor = inner.write().await;
            let exec = executor
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            // Execute a simple ping query using text protocol
            exec.execute("SELECT 1", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            Ok(())
        })
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Vec<Row>>(py, async move {
            let mut executor = inner.write().await;
            let exec = executor
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            // wtx text protocol: execute() is for non-SELECT, we need to prepare even for text queries
            // For now, use prepare + fetch_many_with_stmt
            let stmt_id = exec.prepare(&query)
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            // Fetch all records - wtx returns a Records object that we need to iterate
            let records = exec.fetch_many_with_stmt(stmt_id, (), |_record| {
                // Just collect, don't convert yet (no Python context here)
                Ok(())
            })
            .await
            .map_err(|e| Error::WtxError(e.to_string()))?;

            // Now convert to Rows with Python context
            let mut rows = Vec::new();
            Python::attach(|py| {
                for i in 0..records.len() {
                    if let Some(record) = records.get(i) {
                        let row = crate::r#async::wtx_types::wtx_record_to_row(py, &record)
                            .map_err(|e| Error::WtxError(e.to_string()))?;
                        rows.push(row);
                    }
                }
                Ok::<_, Error>(())
            })?;

            Ok(rows)
        })
    }

    fn query_first<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Option<Row>>(py, async move {
            let mut executor = inner.write().await;
            let exec = executor
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            // Prepare and fetch first record
            let stmt_id = exec.prepare(&query)
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            let record = exec.fetch_with_stmt(stmt_id, ())
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            // Convert to Row with Python context
            let row = Python::attach(|py| {
                crate::r#async::wtx_types::wtx_record_to_row(py, &record)
                    .map_err(|e| Error::WtxError(e.to_string()))
            })?;

            Ok(Some(row))
        })
    }

    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, ()>(py, async move {
            let mut executor = inner.write().await;
            let exec = executor
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            // Use wtx execute() for non-SELECT queries (text protocol)
            exec.execute(&query, |_affected: u64| -> Result<(), wtx::Error> { Ok(()) })
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            Ok(())
        })
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        stmt_cache: Arc<RwLock<StatementCache>>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();

        // Convert params to WtxParams in the Python context
        let params_bound = params.bind(py);
        let wtx_params = crate::r#async::wtx_types::WtxParams::from_py(py, params_bound)?;

        rust_future_into_py::<_, Vec<Row>>(py, async move {
            let mut executor = inner.write().await;
            let exec = executor
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            let query_str: &str = query.as_ref();

            // Check if statement is cached
            let mut cache = stmt_cache.write().await;
            let stmt_id = if let Some(id) = cache.get(query_str) {
                id
            } else {
                // Prepare the statement
                let id = exec.prepare(query_str)
                    .await
                    .map_err(|e| Error::WtxError(e.to_string()))?;
                cache.insert(query_str.to_string(), id);
                id
            };
            drop(cache);

            // Fetch all records with parameters
            let records = exec.fetch_many_with_stmt(stmt_id, wtx_params, |_record| {
                // Just collect, don't convert yet (no Python context here)
                Ok(())
            })
            .await
            .map_err(|e| Error::WtxError(e.to_string()))?;

            // Now convert to Rows with Python context
            let mut rows = Vec::new();
            Python::attach(|py| {
                for i in 0..records.len() {
                    if let Some(record) = records.get(i) {
                        let row = crate::r#async::wtx_types::wtx_record_to_row(py, &record)
                            .map_err(|e| Error::WtxError(e.to_string()))?;
                        rows.push(row);
                    }
                }
                Ok::<_, Error>(())
            })?;

            Ok(rows)
        })
    }

    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        stmt_cache: Arc<RwLock<StatementCache>>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();

        // Convert params to WtxParams in the Python context
        let params_bound = params.bind(py);
        let wtx_params = crate::r#async::wtx_types::WtxParams::from_py(py, params_bound)?;

        rust_future_into_py::<_, Option<Row>>(py, async move {
            let mut executor = inner.write().await;
            let exec = executor
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            let query_str: &str = query.as_ref();

            // Check if statement is cached
            let mut cache = stmt_cache.write().await;
            let stmt_id = if let Some(id) = cache.get(query_str) {
                id
            } else {
                // Prepare the statement
                let id = exec.prepare(query_str)
                    .await
                    .map_err(|e| Error::WtxError(e.to_string()))?;
                cache.insert(query_str.to_string(), id);
                id
            };
            drop(cache);

            // Fetch first record with parameters
            let record = exec.fetch_with_stmt(stmt_id, wtx_params)
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            // Convert to Row with Python context
            let row = Python::attach(|py| {
                crate::r#async::wtx_types::wtx_record_to_row(py, &record)
                    .map_err(|e| Error::WtxError(e.to_string()))
            })?;

            Ok(Some(row))
        })
    }

    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        stmt_cache: Arc<RwLock<StatementCache>>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();

        // Convert params to WtxParams in the Python context
        let params_bound = params.bind(py);
        let wtx_params = crate::r#async::wtx_types::WtxParams::from_py(py, params_bound)?;

        rust_future_into_py::<_, ()>(py, async move {
            let mut executor = inner.write().await;
            let exec = executor
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            let query_str: &str = query.as_ref();

            // Check if statement is cached
            let mut cache = stmt_cache.write().await;
            let stmt_id = if let Some(id) = cache.get(query_str) {
                id
            } else {
                // Prepare the statement
                let id = exec.prepare(query_str)
                    .await
                    .map_err(|e| Error::WtxError(e.to_string()))?;
                cache.insert(query_str.to_string(), id);
                id
            };
            drop(cache);

            // Execute with statement (drop results)
            let _affected = exec.execute_with_stmt(stmt_id, wtx_params)
                .await
                .map_err(|e| Error::WtxError(e.to_string()))?;

            Ok(())
        })
    }

    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
        stmt_cache: Arc<RwLock<StatementCache>>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();

        // Convert all params to WtxParams in the Python context
        let mut wtx_params_vec = Vec::new();
        for param in params {
            let params_bound = param.bind(py);
            let wtx_params = crate::r#async::wtx_types::WtxParams::from_py(py, params_bound)?;
            wtx_params_vec.push(wtx_params);
        }

        rust_future_into_py::<_, ()>(py, async move {
            let mut executor = inner.write().await;
            let exec = executor
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            let query_str: &str = query.as_ref();

            // Check if statement is cached
            let mut cache = stmt_cache.write().await;
            let stmt_id = if let Some(id) = cache.get(query_str) {
                id
            } else {
                // Prepare the statement
                let id = exec.prepare(query_str)
                    .await
                    .map_err(|e| Error::WtxError(e.to_string()))?;
                cache.insert(query_str.to_string(), id);
                id
            };
            drop(cache);

            // Execute batch - loop through params and execute each
            for wtx_params in wtx_params_vec {
                let _affected = exec.execute_with_stmt(stmt_id, wtx_params)
                    .await
                    .map_err(|e| Error::WtxError(e.to_string()))?;
            }

            Ok(())
        })
    }
}
