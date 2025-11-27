use std::collections::VecDeque;

use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};
use tokio::sync::RwLock;

use crate::{
    r#async::backend::ZeroMysqlConn,
    dbapi::{
        async_conn::AsyncDbApiConn,
        async_zero_handler::AsyncDbApiHandler,
        error::{DbApiError, DbApiResult},
    },
    error::{Error, PyroResult},
    params::Params,
    util::tokio_spawn_as_abort_on_drop,
    zero_params_adapter::ParamsAdapter,
};

#[pyclass(module = "pyro_mysql.dbapi_async", name = "Cursor")]
pub struct AsyncCursor(pub RwLock<AsyncCursorImpl>);

impl AsyncCursor {
    pub fn new(conn: Py<AsyncDbApiConn>) -> Self {
        Self(RwLock::new(AsyncCursorImpl::new(conn)))
    }
}

pub struct AsyncCursorImpl {
    conn: Option<Py<AsyncDbApiConn>>,
    result: Option<VecDeque<Py<PyTuple>>>,
    arraysize: usize,
    description: Option<Py<PyList>>,
    rowcount: i64,
    lastrowid: Option<u64>,
}

impl AsyncCursorImpl {
    pub fn new(conn: Py<AsyncDbApiConn>) -> Self {
        Self {
            conn: Some(conn),
            result: None,
            arraysize: 1,
            description: None,
            rowcount: -1,
            lastrowid: None,
        }
    }
}

#[pymethods]
impl AsyncCursor {
    #[getter]
    fn arraysize(&self) -> usize {
        futures::executor::block_on(async { self.0.read().await.arraysize })
    }

    #[setter]
    fn set_arraysize(&self, value: usize) {
        futures::executor::block_on(async { self.0.write().await.arraysize = value })
    }

    #[getter]
    fn description(&self, py: Python) -> Option<Py<PyList>> {
        futures::executor::block_on(async {
            self.0
                .read()
                .await
                .description
                .as_ref()
                .map(|d| d.clone_ref(py))
        })
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        futures::executor::block_on(async { self.0.read().await.rowcount })
    }

    #[getter]
    fn lastrowid(&self) -> Option<u64> {
        futures::executor::block_on(async { self.0.read().await.lastrowid })
    }

    /// Closes the cursor. The connection is still alive
    async fn close(&self) -> PyResult<()> {
        let mut cursor = self.0.write().await;
        cursor.conn = None;
        cursor.rowcount = -1;
        cursor.result = None;
        cursor.description = None;
        Ok(())
    }

    #[pyo3(signature = (query, params=None))]
    async fn execute(&self, query: String, params: Option<Py<PyAny>>) -> DbApiResult<()> {
        let mut cursor = self.0.write().await;

        let conn = {
            let conn = cursor
                .conn
                .as_ref()
                .ok_or_else(|| Error::ConnectionClosedError)?;
            Python::attach(|py| conn.borrow(py).0.clone())
        };

        // Extract params while we have the GIL
        let params = Python::attach(|py| {
            if let Some(p) = params {
                p.extract::<Params>(py)
            } else {
                Ok(Params::default())
            }
        })?;

        // Execute the query in a spawned task
        let handler = tokio_spawn_as_abort_on_drop(async move {
            let mut conn_guard = conn.write().await;
            let zero_conn = conn_guard
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            // Execute with our handler
            execute_with_handler(zero_conn, &query, params).await
        })
        .await
        .unwrap()?;

        // Convert the handler results to Python objects (with GIL)
        Python::attach(|py| {
            if handler.has_result_set() {
                cursor.description = Some(handler.build_description(py)?);
                let rows = handler.rows_to_python(py)?;
                cursor.rowcount = rows.len() as i64;
                cursor.result = Some(VecDeque::from(rows));
                cursor.lastrowid = None;
            } else {
                cursor.description = None;
                cursor.rowcount = handler.affected_rows() as i64;
                cursor.result = None;
                let id = handler.last_insert_id();
                cursor.lastrowid = if id == 0 { None } else { Some(id) };
            }
            Ok::<_, PyErr>(())
        })?;

        Ok(())
    }

    async fn executemany(&self, query: String, params: Vec<Py<PyAny>>) -> DbApiResult<()> {
        let mut cursor = self.0.write().await;
        let conn = {
            let conn = cursor
                .conn
                .as_ref()
                .ok_or_else(|| Error::ConnectionClosedError)?;
            Python::attach(|py| conn.borrow(py).0.clone())
        };

        // Extract all params while we have the GIL
        let params_list: Vec<Params> = Python::attach(|py| {
            params
                .into_iter()
                .map(|p| p.extract::<Params>(py))
                .collect::<PyResult<Vec<_>>>()
        })?;

        let affected = tokio_spawn_as_abort_on_drop(async move {
            let mut conn_guard = conn.write().await;
            let zero_conn = conn_guard
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            let mut total_affected = 0u64;
            for p in params_list {
                zero_conn.exec_drop(query.clone(), p).await?;
                total_affected += zero_conn.affected_rows();
            }
            PyroResult::Ok(total_affected)
        })
        .await
        .unwrap()?;

        cursor.description = None;
        cursor.rowcount = affected as i64;
        cursor.result = None;
        cursor.lastrowid = None;
        Ok(())
    }

    async fn fetchone(&self) -> DbApiResult<Option<Py<PyTuple>>> {
        let mut cursor = self.0.write().await;
        if let Some(result) = &mut cursor.result {
            Ok(result.pop_front())
        } else {
            Err(DbApiError::no_result_set())
        }
    }

    #[pyo3(signature=(size=None))]
    async fn fetchmany(&self, size: Option<usize>) -> DbApiResult<Vec<Py<PyTuple>>> {
        let mut cursor = self.0.write().await;
        let size = size.unwrap_or(cursor.arraysize);
        if let Some(result) = &mut cursor.result {
            let drain_count = size.min(result.len());
            Ok(result.drain(..drain_count).collect())
        } else {
            Err(DbApiError::no_result_set())
        }
    }

    async fn fetchall(&self) -> DbApiResult<Vec<Py<PyTuple>>> {
        let mut cursor = self.0.write().await;
        if let Some(result) = cursor.result.take() {
            cursor.result = Some(VecDeque::new());
            Ok(result.into_iter().collect())
        } else {
            Err(DbApiError::no_result_set())
        }
    }

    fn setinputsizes(&self) {}

    fn setoutputsize(&self) {}

    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
}

/// Execute a query with the AsyncDbApiHandler
async fn execute_with_handler(
    conn: &mut ZeroMysqlConn,
    query: &str,
    params: Params,
) -> PyroResult<AsyncDbApiHandler> {
    let mut handler = AsyncDbApiHandler::default();

    if params.is_empty() {
        // Use text protocol for queries without parameters
        conn.inner.query(query, &mut handler).await?;
    } else {
        // Use binary protocol with prepared statement
        let stmt_id = if let Some(&cached_id) = conn.stmt_cache.get(query) {
            cached_id
        } else {
            let stmt_id = conn.inner.prepare(query).await?;
            conn.stmt_cache.insert(query.to_string(), stmt_id);
            stmt_id
        };

        let params_adapter = ParamsAdapter::new(&params);
        conn.inner
            .exec(stmt_id, params_adapter, &mut handler)
            .await?;
    }

    Ok(handler)
}
