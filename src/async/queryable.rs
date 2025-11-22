use pyo3::{prelude::*, pybacked::PyBackedStr};
use std::sync::Arc;
use tokio::sync::RwLock;
use wtx::database::Records;

use crate::{
    r#async::{
        backend::wtx::{WtxParams, queryable::get_or_prepare_stmt, row::wtx_record_to_row},
        multi_conn::MultiAsyncConn,
        row::Row,
    },
    error::Error,
    params::Params,
    util::{PyroFuture, rust_future_into_py},
};

// Import the mysql_async Queryable trait for its methods
use mysql_async::prelude::Queryable as MysqlAsyncQueryable;

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
    fn query<'py>(&self, py: Python<'py>, query: String, as_dict: bool)
    -> PyResult<Py<PyroFuture>>;
    fn query_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>>;
    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>>;

    // ─── Binary Protocol ─────────────────────────────────────────────────
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>>;
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        as_dict: bool,
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
    fn query<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut inner = inner.write().await;
            let rows: Vec<Row> = inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .query(query)
                .await?;

            // Convert rows to either tuples or dicts
            Python::attach(|py| {
                let result: Vec<Py<PyAny>> = if as_dict {
                    rows.iter()
                        .map(|row| row.to_dict(py).map(|d| d.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                } else {
                    rows.iter()
                        .map(|row| row.to_tuple(py).map(|t| t.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                };
                Ok(result)
            })
        })
    }

    fn query_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut inner = inner.write().await;
            let row: Option<Row> = inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .query_first(query)
                .await?;

            // Convert row to either tuple or dict
            Python::attach(|py| match row {
                Some(r) => {
                    let result: Py<PyAny> = if as_dict {
                        r.to_dict(py)?.into_any().unbind()
                    } else {
                        r.to_tuple(py)?.into_any().unbind()
                    };
                    Ok(Some(result))
                }
                None => Ok(None),
            })
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
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        // Convert Py<PyAny> to Params for mysql_async
        let params_obj: Params = params.extract(py)?;
        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            let rows: Vec<Row> = inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .exec(query, params_obj)
                .await?;

            // Convert rows to either tuples or dicts
            Python::attach(|py| {
                let result: Vec<Py<PyAny>> = if as_dict {
                    rows.iter()
                        .map(|row| row.to_dict(py).map(|d| d.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                } else {
                    rows.iter()
                        .map(|row| row.to_tuple(py).map(|t| t.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                };
                Ok(result)
            })
        })
    }

    #[inline]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        let params_obj: Params = params.extract(py)?;
        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            let row: Option<Row> = inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .exec_first(query, params_obj)
                .await?;

            // Convert row to either tuple or dict
            Python::attach(|py| match row {
                Some(r) => {
                    let result: Py<PyAny> = if as_dict {
                        r.to_dict(py)?.into_any().unbind()
                    } else {
                        r.to_tuple(py)?.into_any().unbind()
                    };
                    Ok(Some(result))
                }
                None => Ok(None),
            })
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
                MultiAsyncConn::Wtx(wtx_conn) => {
                    use wtx::database::Executor;
                    // Use COM_PING or just a simple query
                    wtx_conn
                        .executor
                        .execute("SELECT 1", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;
                    Ok(())
                }
                MultiAsyncConn::ZeroMysql(zero_conn) => {
                    zero_conn.ping().await?;
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
                MultiAsyncConn::Wtx(_) => {
                    panic!("close_prepared_statement() is not supported for wtx connections")
                }
                MultiAsyncConn::ZeroMysql(_) => {
                    panic!("close_prepared_statement() is not supported for zero_mysql connections")
                }
            }
        })
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut inner = inner.write().await;
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            let rows = match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => mysql_conn.query(query).await?,
                MultiAsyncConn::Wtx(wtx_conn) => {
                    use wtx::database::Executor;

                    // Get or prepare statement with caching
                    let stmt_id = get_or_prepare_stmt(
                        &mut wtx_conn.executor,
                        &mut wtx_conn.stmt_cache,
                        &query,
                    )
                    .await?;

                    // Fetch all records with empty params for text query
                    let records = wtx_conn
                        .executor
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

                    rows
                }
                MultiAsyncConn::ZeroMysql(zero_conn) => {
                    // zero_mysql now handles as_dict internally via DictHandler
                    let py_rows = zero_conn.query(query, as_dict).await?;
                    return Python::attach(|py| {
                        Ok(py_rows.bind(py).extract::<Vec<Py<PyAny>>>()?)
                    });
                }
            };

            // Convert rows to either tuples or dicts
            Python::attach(|py| {
                let result: Vec<Py<PyAny>> = if as_dict {
                    rows.iter()
                        .map(|row| row.to_dict(py).map(|d| d.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                } else {
                    rows.iter()
                        .map(|row| row.to_tuple(py).map(|t| t.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                };
                Ok(result)
            })
        })
    }

    fn query_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();
        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut inner = inner.write().await;
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            let row = match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => mysql_conn.query_first(query).await?,
                MultiAsyncConn::Wtx(wtx_conn) => {
                    use wtx::database::Executor;

                    // Get or prepare statement with caching
                    let stmt_id = get_or_prepare_stmt(
                        &mut wtx_conn.executor,
                        &mut wtx_conn.stmt_cache,
                        &query,
                    )
                    .await?;

                    let record = wtx_conn
                        .executor
                        .fetch_with_stmt(stmt_id, ())
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    // Convert to Row with Python context
                    let row = Python::attach(|py| {
                        wtx_record_to_row(py, &record).map_err(|e| Error::WtxError(e.to_string()))
                    })?;

                    Some(row)
                }
                MultiAsyncConn::ZeroMysql(zero_conn) => {
                    // zero_mysql now handles as_dict internally via DictHandler
                    let py_rows = zero_conn.query(query, as_dict).await?;
                    return Python::attach(|py| {
                        let list = py_rows.bind(py);
                        if list.len()? > 0 {
                            Ok(Some(list.get_item(0)?.unbind()))
                        } else {
                            Ok(None)
                        }
                    });
                }
            };

            // Convert row to either tuple or dict
            Python::attach(|py| match row {
                Some(r) => {
                    let result: Py<PyAny> = if as_dict {
                        r.to_dict(py)?.into_any().unbind()
                    } else {
                        r.to_tuple(py)?.into_any().unbind()
                    };
                    Ok(Some(result))
                }
                None => Ok(None),
            })
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
                MultiAsyncConn::Wtx(wtx_conn) => {
                    use wtx::database::Executor;

                    // Use wtx execute() for non-SELECT queries (text protocol)
                    wtx_conn
                        .executor
                        .execute(&query, |_affected: u64| -> Result<(), wtx::Error> {
                            Ok(())
                        })
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    Ok(())
                }
                MultiAsyncConn::ZeroMysql(zero_conn) => {
                    // zero_mysql uses prepared statements for all queries, drop the results
                    let _ = zero_conn.query_drop(query).await?;
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
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();

        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            let rows = match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => {
                    // Convert to Params for mysql_async
                    let params_mysql = Python::attach(|py| params.extract::<Params>(py))?;
                    mysql_conn.exec(query, params_mysql).await?
                }
                MultiAsyncConn::Wtx(wtx_conn) => {
                    use wtx::database::Executor;

                    // Convert to WtxParams for wtx
                    let wtx_params = Python::attach(|py| WtxParams::from_py(py, &params))?;

                    // Get or prepare statement with client-side caching
                    let stmt_id = get_or_prepare_stmt(
                        &mut wtx_conn.executor,
                        &mut wtx_conn.stmt_cache,
                        query,
                    )
                    .await?;

                    // Execute and fetch results
                    let records = wtx_conn
                        .executor
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

                    rows
                }
                MultiAsyncConn::ZeroMysql(zero_conn) => {
                    // zero_mysql now handles as_dict internally via DictHandler
                    let pyro_params =
                        Python::attach(|py| params.extract::<crate::params::Params>(py))?;
                    let py_rows = zero_conn
                        .exec(query.to_string(), pyro_params, as_dict)
                        .await
                        .map_err(Error::from)?;

                    return Python::attach(|py| {
                        Ok(py_rows.bind(py).extract::<Vec<Py<PyAny>>>()?)
                    });
                }
            };

            // Convert rows to either tuples or dicts
            Python::attach(|py| {
                let result: Vec<Py<PyAny>> = if as_dict {
                    rows.iter()
                        .map(|row| row.to_dict(py).map(|d| d.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                } else {
                    rows.iter()
                        .map(|row| row.to_tuple(py).map(|t| t.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                };
                Ok(result)
            })
        })
    }

    #[inline]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Py<PyAny>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.clone();

        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut inner = inner.write().await;
            let query: &str = query.as_ref();
            let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            let row = match conn {
                MultiAsyncConn::MysqlAsync(mysql_conn) => {
                    let params_mysql = Python::attach(|py| params.extract::<Params>(py))?;
                    mysql_conn.exec_first(query, params_mysql).await?
                }
                MultiAsyncConn::Wtx(wtx_conn) => {
                    use wtx::database::Executor;

                    let wtx_params = Python::attach(|py| WtxParams::from_py(py, &params))?;

                    // Get or prepare statement with client-side caching
                    let stmt_id = get_or_prepare_stmt(
                        &mut wtx_conn.executor,
                        &mut wtx_conn.stmt_cache,
                        query,
                    )
                    .await?;

                    // Fetch first record
                    let record = wtx_conn
                        .executor
                        .fetch_with_stmt(stmt_id, wtx_params)
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    // Convert to Row with Python context
                    let row = Python::attach(|py| {
                        wtx_record_to_row(py, &record).map_err(|e| Error::WtxError(e.to_string()))
                    })?;

                    Some(row)
                }
                MultiAsyncConn::ZeroMysql(zero_conn) => {
                    // zero_mysql now handles as_dict internally via DictHandler
                    let pyro_params =
                        Python::attach(|py| params.extract::<crate::params::Params>(py))?;
                    let first_row = zero_conn
                        .exec_first(query.to_string(), pyro_params, as_dict)
                        .await
                        .map_err(Error::from)?;

                    return Ok(first_row);
                }
            };

            // Convert row to either tuple or dict
            Python::attach(|py| match row {
                Some(r) => {
                    let result: Py<PyAny> = if as_dict {
                        r.to_dict(py)?.into_any().unbind()
                    } else {
                        r.to_tuple(py)?.into_any().unbind()
                    };
                    Ok(Some(result))
                }
                None => Ok(None),
            })
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
                MultiAsyncConn::Wtx(wtx_conn) => {
                    use wtx::database::Executor;

                    // Get or prepare statement with client-side caching
                    let stmt_id = get_or_prepare_stmt(
                        &mut wtx_conn.executor,
                        &mut wtx_conn.stmt_cache,
                        query,
                    )
                    .await?;

                    // Execute and drop results (don't fetch)
                    wtx_conn
                        .executor
                        .execute_with_stmt(stmt_id, wtx_params)
                        .await
                        .map_err(|e| Error::WtxError(e.to_string()))?;

                    Ok(())
                }
                MultiAsyncConn::ZeroMysql(zero_conn) => {
                    // Use dedicated exec_drop method
                    let pyro_params =
                        Python::attach(|py| params.extract::<crate::params::Params>(py))?;
                    zero_conn
                        .exec_drop(query.to_string(), pyro_params)
                        .await
                        .map_err(Error::from)?;
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
                MultiAsyncConn::Wtx(wtx_conn) => {
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
                    let stmt_id = get_or_prepare_stmt(
                        &mut wtx_conn.executor,
                        &mut wtx_conn.stmt_cache,
                        query,
                    )
                    .await?;

                    // Execute for each set of params
                    for wtx_params in wtx_params_vec {
                        wtx_conn
                            .executor
                            .execute_with_stmt(stmt_id, wtx_params)
                            .await
                            .map_err(|e| Error::WtxError(e.to_string()))?;
                    }

                    Ok(())
                }
                MultiAsyncConn::ZeroMysql(zero_conn) => {
                    // Execute batch with zero_mysql using exec_drop
                    for params_item in params {
                        let pyro_params =
                            Python::attach(|py| params_item.extract::<crate::params::Params>(py))?;
                        zero_conn
                            .exec_drop(query.to_string(), pyro_params)
                            .await
                            .map_err(Error::from)?;
                    }
                    Ok(())
                }
            }
        })
    }
}
