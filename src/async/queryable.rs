use pyo3::{
    prelude::*,
    pybacked::PyBackedStr,
    types::{PyBytes, PyFloat, PyInt, PyString},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use wtx::database::Records;

use crate::{
    r#async::{conn::MultiAsyncConn, row::Row, wtx_param::WtxParams},
    error::Error,
    params::Params,
    util::{PyroFuture, rust_future_into_py},
};

// Import the mysql_async Queryable trait for its methods
use mysql_async::prelude::Queryable as MysqlAsyncQueryable;

// ─── WTX Helper Functions ────────────────────────────────────────────────────

/// Helper function to get or prepare a statement with client-side caching
pub(crate) async fn get_or_prepare_stmt(
    executor: &mut crate::r#async::conn::WtxMysqlExecutor,
    stmt_cache: &mut std::collections::HashMap<String, u64>,
    query: &str,
) -> Result<u64, crate::error::Error> {
    use wtx::database::Executor;

    // Check cache first
    if let Some(&stmt_id) = stmt_cache.get(query) {
        return Ok(stmt_id);
    }

    // Not in cache, prepare and cache it
    let stmt_id = executor
        .prepare(query)
        .await
        .map_err(|e| crate::error::Error::WtxError(e.to_string()))?;

    stmt_cache.insert(query.to_string(), stmt_id);
    Ok(stmt_id)
}

/// Convert wtx MysqlRecord to async Row with Python objects
pub(crate) fn wtx_record_to_row(
    py: Python<'_>,
    record: &wtx::database::client::mysql::MysqlRecord<wtx::Error>,
) -> Result<crate::r#async::row::Row, wtx::Error> {
    use wtx::database::Record;
    use wtx::database::client::mysql::Ty;

    let mut column_names = Vec::new();
    let mut py_values = Vec::new();

    for value_wrapper in record.values().flatten() {
        column_names.push(value_wrapper.name().to_string());

        let bytes = value_wrapper.bytes();
        let ty_params = value_wrapper.ty();
        let column_type = ty_params.ty();

        let py_obj = if bytes.is_empty() {
            py.None()
        } else {
            match column_type {
                Ty::Tiny => PyInt::new(py, bytes[0] as i8 as i64).into(),
                Ty::Short => {
                    let val = i16::from_le_bytes([bytes[0], bytes[1]]);
                    PyInt::new(py, val as i64).into()
                }
                Ty::Long | Ty::Int24 => {
                    let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    PyInt::new(py, val as i64).into()
                }
                Ty::LongLong => {
                    let val = i64::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    PyInt::new(py, val).into()
                }
                Ty::Float => {
                    let val = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    PyFloat::new(py, val as f64).into()
                }
                Ty::Double => {
                    let val = f64::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    PyFloat::new(py, val).into()
                }
                Ty::VarChar | Ty::VarString | Ty::String => match std::str::from_utf8(bytes) {
                    Ok(s) => {
                        if let Ok(val) = s.parse::<i64>() {
                            PyInt::new(py, val).into()
                        } else if let Ok(val) = s.parse::<f64>() {
                            PyFloat::new(py, val).into()
                        } else {
                            PyString::new(py, s).into()
                        }
                    }
                    Err(_) => PyBytes::new(py, bytes).into(),
                },
                Ty::TinyBlob | Ty::MediumBlob | Ty::LongBlob | Ty::Blob => {
                    PyBytes::new(py, bytes).into()
                }
                Ty::Date | Ty::Datetime | Ty::Timestamp | Ty::Time => {
                    match std::str::from_utf8(bytes) {
                        Ok(s) => PyString::new(py, s).into(),
                        Err(_) => PyBytes::new(py, bytes).into(),
                    }
                }
                Ty::Decimal | Ty::NewDecimal => match std::str::from_utf8(bytes) {
                    Ok(s) => PyString::new(py, s).into(),
                    Err(_) => PyBytes::new(py, bytes).into(),
                },
                Ty::Year => {
                    let val = u16::from_le_bytes([bytes[0], bytes[1]]);
                    PyInt::new(py, val as i64).into()
                }
                _ => match bytes.len() {
                    1 => PyInt::new(py, bytes[0] as i8 as i64).into(),
                    2 => {
                        let val = i16::from_le_bytes([bytes[0], bytes[1]]);
                        PyInt::new(py, val as i64).into()
                    }
                    4 => {
                        let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                        PyInt::new(py, val as i64).into()
                    }
                    8 => {
                        let val = i64::from_le_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7],
                        ]);
                        PyInt::new(py, val).into()
                    }
                    _ => match std::str::from_utf8(bytes) {
                        Ok(s) => {
                            if let Ok(val) = s.parse::<i64>() {
                                PyInt::new(py, val).into()
                            } else if let Ok(val) = s.parse::<f64>() {
                                PyFloat::new(py, val).into()
                            } else {
                                PyString::new(py, s).into()
                            }
                        }
                        Err(_) => PyBytes::new(py, bytes).into(),
                    },
                },
            }
        };

        py_values.push(py_obj);
    }

    Ok(crate::r#async::row::Row::new(py_values, column_names))
}

/// This trait implements the common methods between Conn and Transaction.
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
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>>;
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>>;
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>>;
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>>;
    // fn exec_iter<'py>(&self, py: Python<'py>, query: String, params: Params) -> PyResult<Py<RaiiFuture>>;) -> PyResult<Py<PyroFuture>>;
}

impl<T: mysql_async::prelude::Queryable + Send + Sync + 'static> Queryable
    for Arc<RwLock<Option<T>>>
{
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py(py, async move {
            let mut inner = inner.write().await;
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .ping()
                .await?)
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
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .close(stmt)
                .await?)
        })
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Vec<Row>>(py, async move {
            let mut inner = inner.write().await;
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .query(query)
                .await?)
        })
    }

    fn query_first<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Option<Row>>(py, async move {
            let mut inner = inner.write().await;
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .query_first(query)
                .await?)
        })
    }

    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .query_drop(query)
                .await?)
        })
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────
    #[inline]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        // Convert Py<PyAny> to Params for mysql_async
        let params_obj: Params = params.extract(py)?;
        rust_future_into_py::<_, Vec<Row>>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .exec(query, params_obj)
                .await?)
        })
    }

    #[inline]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        let params_obj: Params = params.extract(py)?;
        rust_future_into_py::<_, Option<Row>>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .exec_first(query, params_obj)
                .await?)
        })
    }

    #[inline]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        let params_obj: Params = params.extract(py)?;
        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .exec_drop(query, params_obj)
                .await?)
        })
    }

    #[inline]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        // Convert Vec<Py<PyAny>> to Vec<Params> for mysql_async
        let mut params_vec = Vec::new();
        for p in params {
            params_vec.push(p.extract::<Params>(py)?);
        }
        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .exec_batch(query, params_vec)
                .await?)
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

// Specific implementation for MultiAsyncConn that dispatches to the appropriate backend
impl Queryable for Arc<RwLock<Option<MultiAsyncConn>>> {
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py(py, async move {
            let mut inner = inner.write().await;
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => {
                    mysql_conn.ping().await?;
                    Ok(())
                }
                MultiAsyncConn::Wtx { executor, .. } => {
                    use wtx::database::Executor;
                    // Use COM_PING or just a simple query
                    executor
                        .execute("SELECT 1", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;
                    Ok(())
                }
            }
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
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => {
                    mysql_conn.close(stmt).await?;
                    Ok(())
                }
                MultiAsyncConn::Wtx { .. } => {
                    panic!("close_prepared_statement() is not supported for wtx connections")
                }
            }
        })
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Vec<Row>>(py, async move {
            let mut inner = inner.write().await;
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => Ok(mysql_conn.query(query).await?),
                MultiAsyncConn::Wtx {
                    executor,
                    stmt_cache,
                } => {
                    use wtx::database::Executor;

                    // Get or prepare statement with caching
                    let stmt_id = get_or_prepare_stmt(executor, stmt_cache, &query).await?;

                    // Fetch all records with empty params for text query
                    let records = executor
                        .fetch_many_with_stmt(stmt_id, (), |_| Ok(()))
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    // Convert to Rows with Python context
                    let mut rows = Vec::new();
                    Python::attach(|py| {
                        for i in 0..records.len() {
                            if let Some(record) = records.get(i) {
                                let row = wtx_record_to_row(py, &record)
                                    .map_err(|e| Error::WtxError(e.to_string()))?;
                                rows.push(row);
                            }
                        }
                        Ok::<_, Error>(())
                    })?;

                    Ok(rows)
                }
            }
        })
    }

    fn query_first<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Option<Row>>(py, async move {
            let mut inner = inner.write().await;
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => Ok(mysql_conn.query_first(query).await?),
                MultiAsyncConn::Wtx {
                    executor,
                    stmt_cache,
                } => {
                    use wtx::database::Executor;

                    // Get or prepare statement with caching
                    let stmt_id = get_or_prepare_stmt(executor, stmt_cache, &query).await?;

                    let record = executor
                        .fetch_with_stmt(stmt_id, ())
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    // Convert to Row with Python context
                    let row = Python::attach(|py| {
                        wtx_record_to_row(py, &record).map_err(|e| Error::WtxError(e.to_string()))
                    })?;

                    Ok(Some(row))
                }
            }
        })
    }

    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => {
                    mysql_conn.query_drop(query).await?;
                    Ok(())
                }
                MultiAsyncConn::Wtx { executor, .. } => {
                    use wtx::database::Executor;

                    // Use wtx execute() for non-SELECT queries (text protocol)
                    executor
                        .execute(&query, |_affected: u64| -> Result<(), wtx::Error> {
                            Ok(())
                        })
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    Ok(())
                }
            }
        })
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────
    #[inline]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();

        rust_future_into_py::<_, Vec<Row>>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => {
                    // Convert to Params for mysql_async
                    let params_mysql = Python::attach(|py| params.extract::<Params>(py))?;
                    Ok(mysql_conn.exec(query, params_mysql).await?)
                }
                MultiAsyncConn::Wtx {
                    executor,
                    stmt_cache,
                } => {
                    use wtx::database::Executor;

                    // Convert to WtxParams for wtx
                    let wtx_params = Python::attach(|py| WtxParams::from_py(py, &params))?;

                    // Get or prepare statement with client-side caching
                    let stmt_id = get_or_prepare_stmt(executor, stmt_cache, query).await?;

                    // Execute and fetch results
                    let records = executor
                        .fetch_many_with_stmt(stmt_id, wtx_params, |_| Ok(()))
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    // Convert wtx records to Rows with Python context

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

                    Ok(rows)
                }
            }
        })
    }

    #[inline]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();

        rust_future_into_py::<_, Option<Row>>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => {
                    let params_mysql = Python::attach(|py| params.extract::<Params>(py))?;
                    Ok(mysql_conn.exec_first(query, params_mysql).await?)
                }
                MultiAsyncConn::Wtx {
                    executor,
                    stmt_cache,
                } => {
                    use wtx::database::Executor;

                    let wtx_params = Python::attach(|py| WtxParams::from_py(py, &params))?;

                    // Get or prepare statement with client-side caching
                    let stmt_id = get_or_prepare_stmt(executor, stmt_cache, query).await?;

                    // Fetch first record
                    let record = executor
                        .fetch_with_stmt(stmt_id, wtx_params)
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    // Convert to Row with Python context
                    let row = Python::attach(|py| {
                        wtx_record_to_row(py, &record).map_err(|e| Error::WtxError(e.to_string()))
                    })?;

                    Ok(Some(row))
                }
            }
        })
    }

    #[inline]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        let wtx_params = WtxParams::from_py(py, &params)?;

        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => {
                    let params_mysql = Python::attach(|py| params.extract::<Params>(py))?;
                    mysql_conn.exec_drop(query, params_mysql).await?;
                    Ok(())
                }
                MultiAsyncConn::Wtx {
                    executor,
                    stmt_cache,
                } => {
                    use wtx::database::Executor;

                    // Get or prepare statement with client-side caching
                    let stmt_id = get_or_prepare_stmt(executor, stmt_cache, query).await?;

                    // Execute and drop results (don't fetch)
                    executor
                        .execute_with_stmt(stmt_id, wtx_params)
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    Ok(())
                }
            }
        })
    }

    #[inline]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => {
                    // Convert to Vec<Params> for mysql_async
                    let mut params_vec = Vec::new();
                    Python::attach(|py| {
                        for p in params {
                            params_vec.push(p.extract::<Params>(py)?);
                        }
                        Ok::<_, PyErr>(())
                    })?;
                    mysql_conn.exec_batch(query, params_vec).await?;
                    Ok(())
                }
                MultiAsyncConn::Wtx {
                    executor,
                    stmt_cache,
                } => {
                    use wtx::database::Executor;

                    // Convert to Vec<WtxParams> for wtx
                    let mut wtx_params_vec = Vec::new();
                    Python::attach(|py| {
                        for p in params {
                            wtx_params_vec.push(WtxParams::from_py(py, &p)?);
                        }
                        Ok::<_, PyErr>(())
                    })?;

                    // Get or prepare statement with client-side caching
                    let stmt_id = get_or_prepare_stmt(executor, stmt_cache, query).await?;

                    // Execute for each set of params
                    for wtx_params in wtx_params_vec {
                        executor
                            .execute_with_stmt(stmt_id, wtx_params)
                            .await
                            .map_err(|e| Error::WtxError(e.to_string()))?;
                    }

                    Ok(())
                }
            }
        })
    }
}
