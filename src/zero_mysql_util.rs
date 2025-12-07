/// Utilities for converting zero-mysql values to Python objects
use std::hint::unlikely;

use crate::py_imports::{
    get_date_class, get_datetime_class, get_decimal_class, get_timedelta_class,
};
use pyo3::{IntoPyObjectExt, prelude::*};
use zero_mysql::constant::{ColumnFlags, ColumnType};
use zero_mysql::protocol::command::ColumnDefinitionTail;

/// MySQL binary charset number - indicates binary/non-text data
const BINARY_CHARSET: u16 = 63;

/// Parse MySQL server version string into (major, minor, patch) tuple
///
/// Handles version strings like:
/// - MySQL: "8.0.33", "8.0.33-0ubuntu0.22.04.4", "5.7.44-log"
/// - MariaDB 10.x: "5.5.5-10.11.6-MariaDB-0ubuntu0.23.10.2" (note the 5.5.5- prefix)
/// - MariaDB 11.x+: "11.2.2-MariaDB" (no prefix)
pub fn parse_server_version(version_str: &str) -> (u16, u16, u16) {
    // MariaDB Server 10.X versions are prefixed with "5.5.5-" by default
    // Strip this prefix if present
    let version_str = version_str.strip_prefix("5.5.5-").unwrap_or(version_str);

    let parts: Vec<&str> = version_str.split('.').collect();

    let major = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let patch = parts
        .get(2)
        .and_then(|s| {
            // Handle versions like "8.0.33-0ubuntu0.22.04.4" or "5.7.44-log"
            // or MariaDB versions like "6-MariaDB-0ubuntu0.23.10.2"
            // Extract just the patch number before any hyphen
            s.split('-').next()?.parse().ok()
        })
        .unwrap_or(0);

    (major, minor, patch)
}

/// Decode text protocol value to Python object based on column type and flags
///
/// Returns the Python object.
pub fn decode_text_value_to_python<'py>(
    py: Python<'py>,
    col: &ColumnDefinitionTail,
    text_value: &[u8],
) -> PyResult<Bound<'py, PyAny>> {
    let column_type = col.column_type().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyException, _>("Failed to get column_type")
    })?;
    let flags = col.flags().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyException, _>("Failed to get flags")
    })?;
    let is_unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
    let is_binary_charset = col.charset() == BINARY_CHARSET;

    match column_type {
        ColumnType::MYSQL_TYPE_NULL => Ok(py.None().into_bound(py)),

        ColumnType::MYSQL_TYPE_TINY
        | ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_INT24
        | ColumnType::MYSQL_TYPE_LONG
        | ColumnType::MYSQL_TYPE_LONGLONG
        | ColumnType::MYSQL_TYPE_YEAR => {
            // Convert bytes to str for parsing
            let text_str = std::str::from_utf8(text_value).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid UTF-8 in integer value")
            })?;

            if is_unsigned {
                let val: u64 = text_str.parse().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid unsigned integer")
                })?;
                Ok(val.into_bound_py_any(py)?)
            } else {
                let val: i64 = text_str.parse().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid integer")
                })?;
                Ok(val.into_bound_py_any(py)?)
            }
        }

        ColumnType::MYSQL_TYPE_FLOAT => {
            let text_str = std::str::from_utf8(text_value).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid UTF-8 in float value")
            })?;
            let val: f32 = text_str
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid float"))?;
            // Convert f32 to f64 via string to maintain precision
            let mut buffer = ryu::Buffer::new();
            let f64_val = buffer.format(val).parse::<f64>().unwrap();
            Ok(f64_val.into_bound_py_any(py)?)
        }

        ColumnType::MYSQL_TYPE_DOUBLE => {
            let text_str = std::str::from_utf8(text_value).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid UTF-8 in double value")
            })?;
            let val: f64 = text_str
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid double"))?;
            Ok(val.into_bound_py_any(py)?)
        }

        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
            // Format: YYYY-MM-DD
            let text_str = std::str::from_utf8(text_value).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid UTF-8 in date value")
            })?;

            if unlikely(text_str == "0000-00-00") {
                return Ok(py.None().into_bound(py));
            }
            let parts: Vec<&str> = text_str.split('-').collect();
            if parts.len() != 3 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid date format",
                ));
            }
            let year: u16 = parts[0]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid year"))?;
            let month: u8 = parts[1]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid month"))?;
            let day: u8 = parts[2]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid day"))?;
            let date_class = get_date_class(py)?;
            date_class.call1((year, month, day))
        }

        ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            // Format: YYYY-MM-DD HH:MM:SS[.ffffff]
            let text_str = std::str::from_utf8(text_value).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid UTF-8 in datetime value")
            })?;

            if unlikely(text_str.starts_with("0000-00-00")) {
                return Ok(py.None().into_bound(py));
            }

            let parts: Vec<&str> = text_str.split(' ').collect();
            if parts.len() < 2 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid datetime format",
                ));
            }

            // Parse date part
            let date_parts: Vec<&str> = parts[0].split('-').collect();
            if date_parts.len() != 3 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid date format",
                ));
            }
            let year: u16 = date_parts[0]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid year"))?;
            let month: u8 = date_parts[1]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid month"))?;
            let day: u8 = date_parts[2]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid day"))?;

            // Parse time part
            let time_parts: Vec<&str> = parts[1].split(':').collect();
            if time_parts.len() != 3 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid time format",
                ));
            }
            let hour: u8 = time_parts[0]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid hour"))?;
            let minute: u8 = time_parts[1]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid minute"))?;

            // Handle seconds with optional microseconds
            let second_parts: Vec<&str> = time_parts[2].split('.').collect();
            let second: u8 = second_parts[0]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid second"))?;

            let datetime_class = get_datetime_class(py)?;
            if second_parts.len() > 1 {
                let microsecond: u32 = second_parts[1].parse().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid microsecond")
                })?;
                datetime_class.call1((year, month, day, hour, minute, second, microsecond))
            } else {
                datetime_class.call1((year, month, day, hour, minute, second))
            }
        }

        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            // Format: [-][H]HH:MM:SS[.ffffff]
            let text_str = std::str::from_utf8(text_value).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid UTF-8 in time value")
            })?;

            if unlikely(text_str == "00:00:00") {
                let timedelta_class = get_timedelta_class(py)?;
                return timedelta_class.call1((0,));
            }

            let is_negative = unlikely(text_str.starts_with('-'));
            let time_str = if is_negative {
                &text_str[1..]
            } else {
                text_str
            };

            let parts: Vec<&str> = time_str.split(':').collect();
            if parts.len() != 3 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Invalid time format",
                ));
            }

            let hours: u32 = parts[0]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid hour"))?;
            let minutes: u8 = parts[1]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid minute"))?;

            let second_parts: Vec<&str> = parts[2].split('.').collect();
            let seconds: u8 = second_parts[0]
                .parse()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid second"))?;

            let timedelta_class = get_timedelta_class(py)?;
            let timedelta = if second_parts.len() > 1 {
                let microsecond: u32 = second_parts[1].parse().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid microsecond")
                })?;
                timedelta_class.call1((
                    0,
                    (hours * 3600 + minutes as u32 * 60 + seconds as u32) as i32,
                    microsecond,
                    0,
                    0,
                    0,
                ))?
            } else {
                timedelta_class.call1((
                    0,
                    (hours * 3600 + minutes as u32 * 60 + seconds as u32) as i32,
                    0,
                    0,
                    0,
                    0,
                ))?
            };

            if is_negative {
                timedelta.call_method0("__neg__")
            } else {
                Ok(timedelta)
            }
        }

        // Decimal types - parse with Decimal class
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let decimal_class = get_decimal_class(py)?;
            let py_str = pyo3::types::PyString::from_bytes(py, text_value).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid UTF-8 in decimal value")
            })?;
            decimal_class.call1((py_str,))
        }

        // TEXT_TYPES: charset 63 = binary -> bytes, otherwise decode as str
        ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_GEOMETRY => {
            if is_binary_charset {
                Ok(pyo3::types::PyBytes::new(py, text_value).into_any())
            } else {
                Ok(pyo3::types::PyString::from_bytes(py, text_value)?.into_any())
            }
        }

        // JSON: always decode as str (connection encoding, regardless of charset)
        ColumnType::MYSQL_TYPE_JSON => {
            Ok(pyo3::types::PyString::from_bytes(py, text_value)?.into_any())
        }

        // ENUM, SET, TYPED_ARRAY: always decode as str (ascii)
        ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET
        | ColumnType::MYSQL_TYPE_TYPED_ARRAY => {
            Ok(pyo3::types::PyString::from_bytes(py, text_value)?.into_any())
        }
    }
}
