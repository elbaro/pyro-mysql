use std::collections::VecDeque;

use mysql_async::prelude::Queryable;
use mysql_async::{BinaryProtocol, QueryResult};
use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};
use tokio::sync::RwLock;

use crate::{
    dbapi::{
        async_conn::AsyncDbApiConn,
        error::{DbApiError, DbApiResult},
    },
    error::{Error, PyroResult},
    params::Params,
    r#async::{multi_conn::MultiAsyncConn, row::Row},
    util::tokio_spawn_as_abort_on_drop,
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
    result: Option<VecDeque<Row>>,
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

        let (description, rowcount, result, lastrowid) = tokio_spawn_as_abort_on_drop(async move {
            let mut conn_guard = conn.write().await;
            let multi_conn = conn_guard
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            match multi_conn {
                MultiAsyncConn::MysqlAsync(conn) => {
                    // Convert Py<PyAny> to Params for mysql_async
                    let params = Python::attach(|py| {
                        if let Some(p) = params {
                            p.extract::<Params>(py)
                        } else {
                            Ok(Params::default())
                        }
                    })?;

                    let query_result = conn
                        .exec_iter(query, params)
                        .await?;

                    let affected_rows = query_result.affected_rows();
                    let last_insert_id = query_result.last_insert_id();

                    if let Some(columns) = query_result.columns().as_ref() && !columns.is_empty(){
                        let description = Python::attach(|py| {
                            PyList::new(
                                py,
                                columns.iter().map(|col|
                                    // tuple of 7 items
                                    (
                                        col.name_str(),          // name
                                        col.column_type() as u8, // type_code
                                        col.column_length(),     // display_size
                                        None::<Option<()>>,      // internal_size
                                        None::<Option<()>>,      // precision
                                        None::<Option<()>>,      // scale
                                        None::<Option<()>>,      // null_ok
                                    )
                                    .into_pyobject(py).unwrap()),
                            )
                            .map(|bound| bound.unbind())
                        })?;

                        let rows = query_result.collect_and_drop().await.map_err(Error::from)?;

                        Result::<_, Error>::Ok((
                            Some(description),
                            affected_rows as i64,
                            Some(rows.into()),
                            None,
                        ))
                    } else {
                        // no result set (different from empty set)
                        Ok((None, affected_rows as i64, None, last_insert_id))
                    }
                }
                MultiAsyncConn::Wtx(wtx_conn) => {
                    use wtx::database::{Executor, Records, Record};
                    use crate::r#async::backend::wtx::{queryable::get_or_prepare_stmt, row::wtx_record_to_row};
                    use crate::r#async::backend::wtx::WtxParams;

                    // Convert Py<PyAny> to WtxParams for wtx
                    let wtx_params = Python::attach(|py| {
                        if let Some(p) = params {
                            WtxParams::from_py(py, &p)
                        } else {
                            // Create empty params by calling from_py with None
                            WtxParams::from_py(py, &py.None())
                        }
                    })?;

                    // Get or prepare statement with client-side caching
                    let stmt_id = get_or_prepare_stmt(&mut wtx_conn.executor, &mut wtx_conn.stmt_cache, &query).await?;

                    // Fetch all records
                    let records = wtx_conn.executor
                        .fetch_many_with_stmt(stmt_id, wtx_params, |_| Ok(()))
                        .await
                        .map_err(|e: wtx::Error| Error::WtxError(e.to_string()))?;

                    // Check if we got any records (indicating a SELECT query)
                    // Note: wtx always returns a records object, even for non-SELECT queries
                    // We detect SELECT queries by checking if we have column metadata
                    if records.len() > 0 {
                        // We have a result set - convert records to rows
                        let mut rows = Vec::with_capacity(records.len());
                        Python::attach(|py| {
                            for i in 0..records.len() {
                                let record = records.get(i).unwrap();
                                let row = wtx_record_to_row(py, &record)
                                    .map_err(|e| Error::WtxError(e.to_string()))?;
                                rows.push(row);
                            }
                            Ok::<_, Error>(())
                        })?;

                        // Build description from first record if available
                        let description = if let Some(first_record) = records.get(0) {
                            Python::attach(|py| {
                                use pyo3::types::PyList;
                                // Collect values into a Vec first
                                let items: Vec<_> = first_record.values().flatten().map(|val| {
                                    // tuple of 7 items matching DB-API spec
                                    (
                                        val.name(),                    // name
                                        val.ty().ty() as u8,          // type_code
                                        None::<Option<()>>,            // display_size
                                        None::<Option<()>>,            // internal_size
                                        None::<Option<()>>,            // precision
                                        None::<Option<()>>,            // scale
                                        None::<Option<()>>,            // null_ok
                                    )
                                    .into_pyobject(py).unwrap()
                                }).collect();

                                PyList::new(py, &items).map(|bound| bound.unbind())
                            })?
                        } else {
                            Python::attach(|py| {
                                use pyo3::types::PyList;
                                Ok::<_, Error>(PyList::empty(py).unbind())
                            })?
                        };

                        Ok((Some(description), rows.len() as i64, Some(VecDeque::from(rows)), None))
                    } else {
                        // No result set (e.g., INSERT, UPDATE, DELETE)
                        // wtx doesn't expose affected_rows easily, return 0
                        Ok((None, 0, None, None))
                    }
                }
                MultiAsyncConn::ZeroMysql(_) => {
                    Err(Error::IncorrectApiUsageError(
                        "zero_mysql connections are not supported with DB-API. Use the async API (pyro_mysql.AsyncConn) instead."
                    ))
                }
            }
        })
        .await
        .unwrap() // TODO: handle join error
        ?;

        cursor.description = description;
        cursor.rowcount = rowcount;
        cursor.result = result;
        cursor.lastrowid = lastrowid;
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

        let affected = tokio_spawn_as_abort_on_drop(async move {
            let mut conn_guard = conn.write().await;
            let multi_conn = conn_guard
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            match multi_conn {
                MultiAsyncConn::MysqlAsync(conn) => {
                    let mut affected = 0;
                    let stmt = conn.prep(query).await.map_err(Error::from)?;
                    for params_item in params {
                        // Convert Py<PyAny> to Params for mysql_async
                        let params = Python::attach(|py| params_item.extract::<Params>(py))?;
                        conn.execute_statement(&stmt, params).await?;
                        QueryResult::<BinaryProtocol>::new(&mut *conn)
                            .drop_result()
                            .await?;
                        affected += conn.affected_rows();
                    }
                    PyroResult::Ok(affected)
                }
                MultiAsyncConn::Wtx(wtx_conn) => {
                    use wtx::database::Executor;
                    use crate::r#async::backend::wtx::queryable::get_or_prepare_stmt;
                    use crate::r#async::backend::wtx::WtxParams;

                    // Get or prepare statement with client-side caching
                    let stmt_id = get_or_prepare_stmt(&mut wtx_conn.executor, &mut wtx_conn.stmt_cache, &query).await?;

                    // Execute for each set of params
                    for params_item in params {
                        // Convert Py<PyAny> to WtxParams for wtx
                        let wtx_params = Python::attach(|py| WtxParams::from_py(py, &params_item))?;

                        wtx_conn.executor
                            .execute_with_stmt(stmt_id, wtx_params)
                            .await
                            .map_err(|e: wtx::Error| Error::WtxError(e.to_string()))?;
                    }

                    // wtx doesn't expose affected_rows easily, return 0
                    PyroResult::Ok(0)
                }
                MultiAsyncConn::ZeroMysql(_) => {
                    Err(Error::IncorrectApiUsageError(
                        "zero_mysql connections are not supported with DB-API. Use the async API (pyro_mysql.AsyncConn) instead."
                    ))
                }
            }
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
            if let Some(row) = result.pop_front() {
                Ok(Some(Python::attach(|py| {
                    row.to_tuple(py).map(|bound| bound.unbind())
                })?))
            } else {
                Ok(None)
            }
        } else {
            Err(DbApiError::no_result_set())
        }
    }

    #[pyo3(signature=(size=None))]
    async fn fetchmany(&self, size: Option<usize>) -> DbApiResult<Vec<Py<PyTuple>>> {
        let mut cursor = self.0.write().await;
        let size = size.unwrap_or(cursor.arraysize);
        if let Some(result) = &mut cursor.result {
            let mut vec = vec![];
            for row in result.drain(..size.min(result.len())) {
                vec.push(Python::attach(|py| {
                    row.to_tuple(py).map(|bound| bound.unbind())
                })?);
            }
            Ok(vec)
        } else {
            Err(DbApiError::no_result_set())
        }
    }

    async fn fetchall(&self) -> DbApiResult<Vec<Py<PyTuple>>> {
        let mut cursor = self.0.write().await;
        if let Some(result) = cursor.result.take() {
            cursor.result = Some(VecDeque::new());
            let mut vec = vec![];
            for row in result.into_iter() {
                vec.push(Python::attach(|py| {
                    row.to_tuple(py).map(|bound| bound.unbind())
                })?);
            }
            Ok(vec)
        } else {
            Err(DbApiError::no_result_set())
        }
    }

    fn setinputsizes(&self) {}

    fn setoutputsize(&self) {}

    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    // async fn __anext__(&self) -> Option<Py<PyTuple>> {
    //     //-> DbApiResult<Option<Py<PyTuple>>> {
    //     match self.fetchone().await {
    //         Ok(x) => x,
    //         Err(x) => None,
    //     }
    // }
}
