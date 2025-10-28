// WTX-specific types and conversions

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyFloat, PyInt, PyString};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use wtx::database::RecordValues;
use wtx::database::client::mysql::{
    Config, ExecutorBuffer, Mysql, MysqlEncodeWrapper, MysqlExecutor, MysqlRecord, Ty, TyParams,
};
use wtx::misc::Lease;
use wtx::misc::Uri;

use crate::error::Error;

/// Type alias for the WTX MySQL executor we use
/// - Error type: wtx::Error
/// - ExecutorBuffer: stores query buffers
/// - TcpStream: async network connection
pub type WtxExecutor = MysqlExecutor<wtx::Error, ExecutorBuffer, TcpStream>;

/// Wrapper around WTX executor with connection state
pub struct WtxConn {
    pub executor: Arc<RwLock<Option<WtxExecutor>>>,
}

/// Convert wtx MysqlRecord to async Row with Python objects
pub fn wtx_record_to_row(
    py: Python<'_>,
    record: &wtx::database::client::mysql::MysqlRecord<wtx::Error>,
) -> Result<crate::r#async::row::Row, wtx::Error> {
    use wtx::database::Record;

    // Extract column names and values from the record
    let mut column_names = Vec::new();
    let mut py_values = Vec::new();

    for value_wrapper in record.values().flatten() {
        // Extract column name
        column_names.push(value_wrapper.name().to_string());

        let bytes = value_wrapper.bytes();
        let ty_params = value_wrapper.ty();
        let column_type = ty_params.ty();

        let py_obj = if bytes.is_empty() {
            py.None()
        } else {
            match column_type {
                // Integer types
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

                // Float types
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

                // String/Binary types
                Ty::VarChar | Ty::VarString | Ty::String => {
                    match std::str::from_utf8(bytes) {
                        Ok(s) => {
                            // Try to parse as integer or float first (for numeric strings)
                            if let Ok(val) = s.parse::<i64>() {
                                PyInt::new(py, val).into()
                            } else if let Ok(val) = s.parse::<f64>() {
                                PyFloat::new(py, val).into()
                            } else {
                                PyString::new(py, s).into()
                            }
                        }
                        Err(_) => PyBytes::new(py, bytes).into(),
                    }
                }

                Ty::TinyBlob | Ty::MediumBlob | Ty::LongBlob | Ty::Blob => {
                    PyBytes::new(py, bytes).into()
                }

                // TODO: Date/Time types
                Ty::Date | Ty::Datetime | Ty::Timestamp | Ty::Time => {
                    // For now, try to decode as string
                    match std::str::from_utf8(bytes) {
                        Ok(s) => PyString::new(py, s).into(),
                        Err(_) => PyBytes::new(py, bytes).into(),
                    }
                }

                // Decimal/Numeric - decode as string for now
                Ty::Decimal | Ty::NewDecimal => match std::str::from_utf8(bytes) {
                    Ok(s) => PyString::new(py, s).into(),
                    Err(_) => PyBytes::new(py, bytes).into(),
                },

                // Year
                Ty::Year => {
                    let val = u16::from_le_bytes([bytes[0], bytes[1]]);
                    PyInt::new(py, val as i64).into()
                }

                // Default fallback
                _ => {
                    // Try to detect binary-encoded integers by byte patterns
                    match bytes.len() {
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
                                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                                bytes[6], bytes[7],
                            ]);
                            PyInt::new(py, val).into()
                        }
                        _ => {
                            // Try as string first, then bytes
                            match std::str::from_utf8(bytes) {
                                Ok(s) => {
                                    // Try to parse as number
                                    if let Ok(val) = s.parse::<i64>() {
                                        PyInt::new(py, val).into()
                                    } else if let Ok(val) = s.parse::<f64>() {
                                        PyFloat::new(py, val).into()
                                    } else {
                                        PyString::new(py, s).into()
                                    }
                                }
                                Err(_) => PyBytes::new(py, bytes).into(),
                            }
                        }
                    }
                }
            }
        };

        py_values.push(py_obj);
    }

    Ok(crate::r#async::row::Row::new(py_values, column_names))
}

/// Decode MySQL binary protocol bytes to Python object
fn decode_wtx_bytes_to_py(py: Python<'_>, bytes: &[u8]) -> Result<Py<PyAny>, wtx::Error> {
    use pyo3::types::{PyBytes, PyString};

    if bytes.is_empty() {
        return Ok(py.None());
    }

    // Try to decode as UTF-8 string first (most common case)
    match std::str::from_utf8(bytes) {
        Ok(s) => {
            // Valid UTF-8, return as string
            Ok(PyString::new(py, s).into_any().unbind())
        }
        Err(_) => {
            // Not valid UTF-8, return as bytes
            Ok(PyBytes::new(py, bytes).into_any().unbind())
        }
    }
}

/// Decode MySQL binary protocol value to Python object with type info
/// This is for more precise decoding when we have column type information
#[allow(dead_code)]
fn decode_wtx_value_typed(
    py: Python<'_>,
    bytes: &[u8],
    column_type: Ty,
) -> Result<Py<PyAny>, wtx::Error> {
    use pyo3::types::{PyBytes, PyFloat, PyInt, PyString};

    if bytes.is_empty() {
        return Ok(py.None());
    }

    match column_type {
        // Integer types
        Ty::Tiny => {
            if bytes.len() == 1 {
                Ok(PyInt::new(py, bytes[0] as i64).into_any().unbind())
            } else {
                decode_wtx_bytes_to_py(py, bytes)
            }
        }
        Ty::Short => {
            if bytes.len() >= 2 {
                let val = i16::from_le_bytes([bytes[0], bytes[1]]);
                Ok(PyInt::new(py, val as i64).into_any().unbind())
            } else {
                decode_wtx_bytes_to_py(py, bytes)
            }
        }
        Ty::Long | Ty::Int24 => {
            if bytes.len() >= 4 {
                let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Ok(PyInt::new(py, val as i64).into_any().unbind())
            } else {
                decode_wtx_bytes_to_py(py, bytes)
            }
        }
        Ty::LongLong => {
            if bytes.len() >= 8 {
                let val = i64::from_le_bytes(bytes[0..8].try_into().unwrap());
                Ok(PyInt::new(py, val).into_any().unbind())
            } else {
                decode_wtx_bytes_to_py(py, bytes)
            }
        }

        // Float types
        Ty::Float => {
            if bytes.len() >= 4 {
                let val = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Ok(PyFloat::new(py, val as f64).into_any().unbind())
            } else {
                decode_wtx_bytes_to_py(py, bytes)
            }
        }
        Ty::Double => {
            if bytes.len() >= 8 {
                let val = f64::from_le_bytes(bytes[0..8].try_into().unwrap());
                Ok(PyFloat::new(py, val).into_any().unbind())
            } else {
                decode_wtx_bytes_to_py(py, bytes)
            }
        }

        // String/text types
        Ty::VarChar | Ty::VarString | Ty::String => match std::str::from_utf8(bytes) {
            Ok(s) => Ok(PyString::new(py, s).into_any().unbind()),
            Err(_) => Ok(PyBytes::new(py, bytes).into_any().unbind()),
        },

        // Binary types
        Ty::TinyBlob | Ty::Blob | Ty::MediumBlob | Ty::LongBlob => {
            Ok(PyBytes::new(py, bytes).into_any().unbind())
        }

        // TODO: Implement datetime, date, time, decimal types

        // Default: try as string
        _ => decode_wtx_bytes_to_py(py, bytes),
    }
}

impl WtxConn {
    pub async fn connect(url: &str) -> Result<Self, wtx::Error> {
        Self::connect_with_buffer(url, None).await
    }

    pub async fn connect_with_buffer(
        url: &str,
        executor_buffer: Option<ExecutorBuffer>,
    ) -> Result<Self, wtx::Error> {
        // Parse the URL
        let uri = Uri::new(url);

        // Create RNG for connection and buffer
        use wtx::rng::SeedableRng;
        let mut rng = wtx::rng::ChaCha20::from_os()?;

        // Parse config from URI
        let config = Config::from_uri(&uri)?;

        // Use provided buffer or create new one
        let executor_buffer =
            executor_buffer.unwrap_or_else(|| ExecutorBuffer::new(usize::MAX, &mut rng));

        // Connect to the server
        let addr = uri.hostname_with_implied_port();
        let tcp_stream = TcpStream::connect(addr)
            .await
            .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;

        // Create the executor
        let executor =
            MysqlExecutor::connect(&config, executor_buffer, &mut rng, tcp_stream).await?;

        Ok(Self {
            executor: Arc::new(RwLock::new(Some(executor))),
        })
    }
}

/// Python wrapper for wtx ExecutorBuffer
/// This allows reusing buffers across connections for better performance
#[pyclass(module = "pyro_mysql.async_", name = "BufferObj")]
pub struct BufferObj {
    pub(crate) inner: Arc<RwLock<Option<ExecutorBuffer>>>,
}

#[pymethods]
impl BufferObj {
    #[new]
    fn _new() -> crate::error::PyroResult<Self> {
        Err(Error::IncorrectApiUsageError(
            "use `BufferObj.new()` instead of BufferObj()",
        ))
    }

    #[staticmethod]
    #[pyo3(signature = (max_capacity=usize::MAX))]
    fn new(max_capacity: usize) -> crate::error::PyroResult<Self> {
        use wtx::rng::SeedableRng;
        let mut rng = wtx::rng::ChaCha20::from_os()
            .map_err(|e| Error::WtxError(e.to_string()))?;

        let buffer = ExecutorBuffer::new(max_capacity, &mut rng);

        Ok(Self {
            inner: Arc::new(RwLock::new(Some(buffer))),
        })
    }
}

impl BufferObj {
    /// Extract the buffer for use in connection creation
    /// Returns None if the buffer has already been taken
    pub(crate) async fn take(&self) -> Option<ExecutorBuffer> {
        let mut inner = self.inner.write().await;
        inner.take()
    }
}

/// Wrapper for wtx MysqlRecord that we can convert to our Row type
pub struct WtxRow<'exec> {
    pub record: MysqlRecord<'exec, wtx::Error>,
}

/// Client-side prepared statement cache
pub struct StatementCache {
    cache: HashMap<String, u64>,
}

impl StatementCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn get(&self, query: &str) -> Option<u64> {
        self.cache.get(query).copied()
    }

    pub fn insert(&mut self, query: String, stmt_id: u64) {
        self.cache.insert(query, stmt_id);
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

/// Lightweight parameter wrapper for wtx - holds Python objects directly
/// We store Py<PyAny> (not Bound) so this can be moved into async blocks
pub struct WtxParams {
    values: Vec<Py<PyAny>>,
}

impl WtxParams {
    /// Create from Python tuple/list/dict
    pub fn from_py(_py: Python, params: &Bound<PyAny>) -> PyResult<Self> {
        // Handle None case
        if params.is_none() {
            return Ok(Self { values: Vec::new() });
        }

        // Handle tuple case
        if let Ok(tuple) = params.cast::<pyo3::types::PyTuple>() {
            let values: Vec<_> = tuple.iter().map(|b| b.unbind()).collect();
            return Ok(Self { values });
        }

        // Handle list case
        if let Ok(list) = params.cast::<pyo3::types::PyList>() {
            let values: Vec<_> = list.iter().map(|b| b.unbind()).collect();
            return Ok(Self { values });
        }

        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Expected None, tuple, or list for params",
        ))
    }
}

// Implement RecordValues for WtxParams - encode Python objects directly
impl RecordValues<Mysql<wtx::Error>> for WtxParams {
    fn encode_values<'inner, 'outer, 'rem, A>(
        &self,
        aux: &mut A,
        ew: &mut <Mysql<wtx::Error> as wtx::de::DEController>::EncodeWrapper<'inner, 'outer, 'rem>,
        mut prefix_cb: impl FnMut(
            &mut A,
            &mut <Mysql<wtx::Error> as wtx::de::DEController>::EncodeWrapper<'inner, 'outer, 'rem>,
        ) -> usize,
        mut suffix_cb: impl FnMut(
            &mut A,
            &mut <Mysql<wtx::Error> as wtx::de::DEController>::EncodeWrapper<'inner, 'outer, 'rem>,
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
        mut cb: impl FnMut(bool, Option<TyParams>) -> Result<(), wtx::Error>,
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
    ew: &mut MysqlEncodeWrapper<'_>,
) -> Result<bool, wtx::Error> {
    use wtx::de::Encode;

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
            <bool as Encode<Mysql<wtx::Error>>>::encode(&val, &mut (), ew)?;
            Ok(false)
        }
        "int" => {
            // Try i64 first
            if let Ok(val) = value.extract::<i64>() {
                <i64 as Encode<Mysql<wtx::Error>>>::encode(&val, &mut (), ew)?;
            } else {
                // Too large, encode as string with length prefix
                let s = value
                    .str()
                    .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
                let string = s
                    .to_str()
                    .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
                <&str as Encode<Mysql<wtx::Error>>>::encode(&string, &mut (), ew)?;
            }
            Ok(false)
        }
        "float" => {
            let val: f64 = value
                .extract()
                .map_err(|e: PyErr| wtx::Error::Generic(Box::new(e.to_string())))?;
            <f64 as Encode<Mysql<wtx::Error>>>::encode(&val, &mut (), ew)?;
            Ok(false)
        }
        "str" => {
            let s: &str = value
                .extract()
                .map_err(|e: PyErr| wtx::Error::Generic(Box::new(e.to_string())))?;
            <&str as Encode<Mysql<wtx::Error>>>::encode(&s, &mut (), ew)?;
            Ok(false)
        }
        "bytes" => {
            use pyo3::types::PyBytes;
            let py_bytes = value
                .cast::<PyBytes>()
                .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
            let b: &[u8] = py_bytes.as_bytes();
            <&[u8] as Encode<Mysql<wtx::Error>>>::encode(&b, &mut (), ew)?;
            Ok(false)
        }
        // TODO: Handle datetime, date, time, timedelta, Decimal, etc.
        _ => {
            // Default: convert to string
            let s = value
                .str()
                .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
            let string = s
                .to_str()
                .map_err(|e| wtx::Error::Generic(Box::new(e.to_string())))?;
            <&str as Encode<Mysql<wtx::Error>>>::encode(&string, &mut (), ew)?;
            Ok(false)
        }
    }
}

/// Get type information for a Python object
fn get_py_type_info(value: &Bound<'_, PyAny>) -> Result<(bool, Option<TyParams>), wtx::Error> {
    const BINARY: u16 = 128; // Flag::Binary

    // Get type name
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
        // TODO: Handle datetime, date, time, timedelta, Decimal, etc.
        _ => {
            // Default to string
            Ok((false, Some(TyParams::new(0, Ty::VarString))))
        }
    }
}
