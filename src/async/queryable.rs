use pyo3::{
    prelude::*,
    pybacked::PyBackedStr,
    types::{PyBytes, PyFloat, PyInt, PyString},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use wtx::database::Records;
use wtx::misc::Lease;

use crate::{
    r#async::{conn::MultiAsyncConn, row::Row},
    error::Error,
    params::Params,
    util::{PyroFuture, rust_future_into_py},
};

// Import the mysql_async Queryable trait for its methods
use mysql_async::prelude::Queryable as MysqlAsyncQueryable;

// ─── WTX Helper Types and Functions ──────────────────────────────────────────

/// Lightweight parameter wrapper for wtx - holds Python objects directly
struct WtxParams {
    values: Vec<Py<PyAny>>,
}

impl WtxParams {
    /// Create from Py<PyAny> (Python tuple/list/None)
    fn from_py(py: Python, params: &Py<PyAny>) -> PyResult<Self> {
        let params_bound = params.bind(py);

        // Handle None case
        if params_bound.is_none() {
            return Ok(Self { values: Vec::new() });
        }

        // Handle tuple case
        if let Ok(tuple) = params_bound.cast::<pyo3::types::PyTuple>() {
            let values: Vec<_> = tuple.iter().map(|b| b.unbind()).collect();
            return Ok(Self { values });
        }

        // Handle list case
        if let Ok(list) = params_bound.cast::<pyo3::types::PyList>() {
            let values: Vec<_> = list.iter().map(|b| b.unbind()).collect();
            return Ok(Self { values });
        }

        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Expected None, tuple, or list for params (wtx backend doesn't support named params yet)",
        ))
    }
}

// Implement RecordValues for WtxParams - encode Python objects directly
impl wtx::database::RecordValues<wtx::database::client::mysql::Mysql<wtx::Error>> for WtxParams {
    fn encode_values<'inner, 'outer, 'rem, A>(
        &self,
        aux: &mut A,
        ew: &mut <wtx::database::client::mysql::Mysql<wtx::Error> as wtx::de::DEController>::EncodeWrapper<'inner, 'outer, 'rem>,
        mut prefix_cb: impl FnMut(
            &mut A,
            &mut <wtx::database::client::mysql::Mysql<wtx::Error> as wtx::de::DEController>::EncodeWrapper<'inner, 'outer, 'rem>,
        ) -> usize,
        mut suffix_cb: impl FnMut(
            &mut A,
            &mut <wtx::database::client::mysql::Mysql<wtx::Error> as wtx::de::DEController>::EncodeWrapper<'inner, 'outer, 'rem>,
            bool,
            usize,
        ) -> usize,
    ) -> Result<usize, wtx::Error>
    where
        'inner: 'outer,
    {
        Python::attach(|py| {
            let mut n: usize = 0;
            for value in &self.values {
                n = n.wrapping_add(prefix_cb(aux, ew));
                let before_len = ew.lease().len();

                // Encode Python object directly to buffer
                let is_null = encode_py_to_wtx(&value.bind(py), ew)?;

                let value_len = ew.lease().len().wrapping_sub(before_len);
                n = n.wrapping_add(value_len);
                n = n.wrapping_add(suffix_cb(aux, ew, is_null, value_len));
            }
            Ok(n)
        })
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn walk(
        &self,
        mut cb: impl FnMut(
            bool,
            Option<wtx::database::client::mysql::TyParams>,
        ) -> Result<(), wtx::Error>,
    ) -> Result<(), wtx::Error> {
        Python::attach(|py| {
            for value in &self.values {
                let (is_null, ty_params) = get_py_type_info(&value.bind(py))?;
                cb(is_null, ty_params)?;
            }
            Ok(())
        })
    }
}

/// Encode a Python object directly to wtx buffer using wtx's primitive encoders
/// Returns true if the value is NULL
fn encode_py_to_wtx(
    value: &Bound<'_, PyAny>,
    ew: &mut wtx::database::client::mysql::MysqlEncodeWrapper<'_>,
) -> Result<bool, wtx::Error> {
    use wtx::de::Encode;
    type Mysql = wtx::database::client::mysql::Mysql<wtx::Error>;

    // Get type name
    let type_obj = value.get_type();
    let type_name = type_obj
        .name()
        .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
    let type_str = type_name
        .to_str()
        .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;

    match type_str {
        "NoneType" => Ok(true), // NULL
        "bool" => {
            let val: bool = value
                .extract()
                .map_err(|e: PyErr| wtx::Error::Generic(Box::new(e.to_string())))?;
            <bool as Encode<Mysql>>::encode(&val, &mut (), ew)?;
            Ok(false)
        }
        "int" => {
            // Try i64 first
            if let Ok(val) = value.extract::<i64>() {
                <i64 as Encode<Mysql>>::encode(&val, &mut (), ew)?;
            } else {
                // Too large, encode as string
                let s = value
                    .str()
                    .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
                let string = s
                    .to_str()
                    .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
                <&str as Encode<Mysql>>::encode(&string, &mut (), ew)?;
            }
            Ok(false)
        }
        "float" => {
            let val: f64 = value
                .extract()
                .map_err(|e: PyErr| wtx::Error::Generic(Box::new(e.to_string())))?;
            <f64 as Encode<Mysql>>::encode(&val, &mut (), ew)?;
            Ok(false)
        }
        "str" => {
            let s: &str = value
                .extract()
                .map_err(|e: PyErr| wtx::Error::Generic(Box::new(e.to_string())))?;
            <&str as Encode<Mysql>>::encode(&s, &mut (), ew)?;
            Ok(false)
        }
        "bytes" => {
            use pyo3::types::PyBytes;
            let py_bytes = value
                .cast::<PyBytes>()
                .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
            let b: &[u8] = py_bytes.as_bytes();
            <&[u8] as Encode<Mysql>>::encode(&b, &mut (), ew)?;
            Ok(false)
        }
        _ => {
            // Default: convert to string
            let s = value
                .str()
                .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
            let string = s
                .to_str()
                .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
            <&str as Encode<Mysql>>::encode(&string, &mut (), ew)?;
            Ok(false)
        }
    }
}

/// Get type information for a Python object
fn get_py_type_info(
    value: &Bound<'_, PyAny>,
) -> Result<(bool, Option<wtx::database::client::mysql::TyParams>), wtx::Error> {
    use wtx::database::client::mysql::{Ty, TyParams};
    const BINARY: u16 = 128; // Flag::Binary

    let type_obj = value.get_type();
    let type_name = type_obj
        .name()
        .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
    let type_str = type_name
        .to_str()
        .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;

    match type_str {
        "NoneType" => Ok((true, None)),
        "bool" => Ok((false, Some(TyParams::new(BINARY, Ty::Tiny)))),
        "int" => Ok((false, Some(TyParams::new(BINARY, Ty::LongLong)))),
        "float" => Ok((false, Some(TyParams::new(BINARY, Ty::Double)))),
        "str" => Ok((false, Some(TyParams::new(0, Ty::VarString)))),
        "bytes" => Ok((false, Some(TyParams::new(BINARY, Ty::Blob)))),
        _ => Ok((false, Some(TyParams::new(0, Ty::VarString)))),
    }
}

/// Helper function to get or prepare a statement with client-side caching
async fn get_or_prepare_stmt(
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
fn wtx_record_to_row(
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
                    // Ok(mysql_conn.exec(query, params_mysql).await?)

                    let mut query_result = mysql_conn.exec_iter(query, params_mysql).await?;
                    let rows = query_result
                        .reduce(Vec::new(), |mut acc, row| {
                            acc.push(mysql::prelude::FromRow::from_row(row));
                            acc
                        })
                        .await?;
                    query_result.drop_result().await?;

                    Ok(rows)
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

                    let wtx_params = Python::attach(|py| WtxParams::from_py(py, &params))?;

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
