use std::collections::VecDeque;

use mysql_async::prelude::{Queryable, StatementLike};
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
    row::Row,
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
        pyo3_async_runtimes::tokio::get_runtime().block_on(async { self.0.read().await.arraysize })
    }

    #[setter]
    fn set_arraysize(&self, value: usize) {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { self.0.write().await.arraysize = value })
    }

    #[getter]
    fn description(&self) -> Option<Py<PyList>> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            self.0
                .read()
                .await
                .description
                .as_ref()
                .map(|d| Python::attach(|py| d.clone_ref(py)))
        })
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async { self.0.read().await.rowcount })
    }

    #[getter]
    fn lastrowid(&self) -> Option<u64> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async { self.0.read().await.lastrowid })
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

    #[pyo3(signature = (query, params=Params::default()))]
    async fn execute(&self, query: String, params: Params) -> DbApiResult<()> {
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
            let conn = conn_guard
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

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

    async fn executemany(&self, query: String, params: Vec<Params>) -> DbApiResult<()> {
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
            let conn = conn_guard
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?;

            let mut affected = 0;
            let stmt = conn.prep(query).await.map_err(Error::from)?;
            for params in params {
                conn.execute_statement(&stmt, params).await?;
                QueryResult::<BinaryProtocol>::new(&mut *conn)
                    .drop_result()
                    .await?;
                affected += conn.affected_rows();
            }
            PyroResult::Ok(affected)
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
