/// Utilities for converting zero-mysql values to Python objects
use std::hint::unlikely;

use crate::py_imports::{
    get_date_class, get_datetime_class, get_decimal_class, get_timedelta_class,
};
use pyo3::{IntoPyObjectExt, prelude::*};
use zero_mysql::constant::{ColumnFlags, ColumnType};
use zero_mysql::protocol::command::ColumnDefinitionTail;
use zero_mysql::protocol::primitive::*;
use zerocopy::FromBytes;

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

/// Directly decode raw bytes to Python object based on column type and flags (binary protocol)
///
/// This function skips the intermediate `Value` enum and directly converts
/// from raw MySQL binary protocol bytes to Python objects for better performance.
///
/// Returns the Python object and the remaining bytes.
pub fn decode_binary_bytes_to_python<'py, 'data>(
    py: Python<'py>,
    col: &ColumnDefinitionTail,
    data: &'data [u8],
) -> PyResult<(Bound<'py, PyAny>, &'data [u8])> {
    let type_and_flags = col.type_and_flags().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyException, _>("Failed to get type_and_flags")
    })?;
    let is_unsigned = type_and_flags.flags.contains(ColumnFlags::UNSIGNED_FLAG);
    let is_binary_charset = col.charset() == BINARY_CHARSET;

    match type_and_flags.column_type {
        ColumnType::MYSQL_TYPE_NULL => Ok((py.None().into_bound(py), data)),

        // Integer types
        ColumnType::MYSQL_TYPE_TINY => {
            let (val, rest) = read_int_1(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read TINY")
            })?;
            let py_val = if is_unsigned {
                (val as u64).into_bound_py_any(py)?
            } else {
                ((val as i8) as i64).into_bound_py_any(py)?
            };
            Ok((py_val, rest))
        }

        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
            let (val, rest) = read_int_2(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read SHORT")
            })?;
            let py_val = if is_unsigned {
                (val as u64).into_bound_py_any(py)?
            } else {
                ((val as i16) as i64).into_bound_py_any(py)?
            };
            Ok((py_val, rest))
        }

        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
            let (val, rest) = read_int_4(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read LONG")
            })?;
            let py_val = if is_unsigned {
                (val as u64).into_bound_py_any(py)?
            } else {
                ((val as i32) as i64).into_bound_py_any(py)?
            };
            Ok((py_val, rest))
        }

        ColumnType::MYSQL_TYPE_LONGLONG => {
            let (val, rest) = read_int_8(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read LONGLONG")
            })?;
            let py_val = if is_unsigned {
                val.into_bound_py_any(py)?
            } else {
                (val as i64).into_bound_py_any(py)?
            };
            Ok((py_val, rest))
        }

        // Floating point types
        ColumnType::MYSQL_TYPE_FLOAT => {
            let (val, rest) = read_int_4(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read FLOAT")
            })?;
            let f = f32::from_bits(val);
            // Convert f32 to f64 via string to maintain precision
            let mut buffer = ryu::Buffer::new();
            let f64_val = buffer.format(f).parse::<f64>().unwrap();
            Ok((f64_val.into_bound_py_any(py)?, rest))
        }

        ColumnType::MYSQL_TYPE_DOUBLE => {
            let (val, rest) = read_int_8(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read DOUBLE")
            })?;
            let d = f64::from_bits(val);
            Ok((d.into_bound_py_any(py)?, rest))
        }

        // DATE types - always return datetime.date
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
            let (len, mut rest) = read_int_1(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read date length")
            })?;
            match len {
                0 => {
                    // 0000-00-00 - return None for zero date
                    Ok((py.None().into_bound(py), rest))
                }
                4 => {
                    let ts = zero_mysql::protocol::value::Timestamp4::ref_from_bytes(&rest[..4])
                        .map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyException, _>("Invalid Timestamp4")
                        })?;
                    rest = &rest[4..];
                    let date_class = get_date_class(py)?;
                    let py_date = date_class.call1((ts.year(), ts.month, ts.day))?;
                    Ok((py_date, rest))
                }
                _ => Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Invalid date length",
                )),
            }
        }

        // DATETIME/TIMESTAMP types - always return datetime.datetime
        ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_DATETIME2 => {
            let (len, mut rest) = read_int_1(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read timestamp length")
            })?;
            match len {
                0 => {
                    // 0000-00-00 00:00:00 - return None for zero timestamp
                    Ok((py.None().into_bound(py), rest))
                }
                4 => {
                    // DATETIME with only date part (time is 00:00:00)
                    let ts = zero_mysql::protocol::value::Timestamp4::ref_from_bytes(&rest[..4])
                        .map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyException, _>("Invalid Timestamp4")
                        })?;
                    rest = &rest[4..];
                    let datetime_class = get_datetime_class(py)?;
                    let py_datetime =
                        datetime_class.call1((ts.year(), ts.month, ts.day, 0, 0, 0))?;
                    Ok((py_datetime, rest))
                }
                7 => {
                    // DATETIME without microseconds
                    let ts = zero_mysql::protocol::value::Timestamp7::ref_from_bytes(&rest[..7])
                        .map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyException, _>("Invalid Timestamp7")
                        })?;
                    rest = &rest[7..];
                    let datetime_class = get_datetime_class(py)?;
                    let py_datetime = datetime_class.call1((
                        ts.year(),
                        ts.month,
                        ts.day,
                        ts.hour,
                        ts.minute,
                        ts.second,
                    ))?;
                    Ok((py_datetime, rest))
                }
                11 => {
                    // DATETIME with microseconds
                    let ts = zero_mysql::protocol::value::Timestamp11::ref_from_bytes(&rest[..11])
                        .map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyException, _>("Invalid Timestamp11")
                        })?;
                    rest = &rest[11..];
                    let datetime_class = get_datetime_class(py)?;
                    let py_datetime = datetime_class.call1((
                        ts.year(),
                        ts.month,
                        ts.day,
                        ts.hour,
                        ts.minute,
                        ts.second,
                        ts.microsecond(),
                    ))?;
                    Ok((py_datetime, rest))
                }
                _ => Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Invalid timestamp length",
                )),
            }
        }

        // TIME types
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            let (len, mut rest) = read_int_1(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read time length")
            })?;
            match len {
                0 => {
                    // 00:00:00 - return timedelta(0)
                    let timedelta_class = get_timedelta_class(py)?;
                    let py_timedelta = timedelta_class.call1((0,))?;
                    Ok((py_timedelta, rest))
                }
                8 => {
                    // TIME without microseconds
                    let time = zero_mysql::protocol::value::Time8::ref_from_bytes(&rest[..8])
                        .map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyException, _>("Invalid Time8")
                        })?;
                    rest = &rest[8..];
                    let timedelta_class = get_timedelta_class(py)?;
                    let timedelta = timedelta_class.call1((
                        time.days(),
                        time.second as i32,
                        0, // microseconds
                        0, // milliseconds
                        time.minute as i32,
                        time.hour as i32,
                    ))?;
                    let py_timedelta = if time.is_negative() {
                        timedelta.call_method0("__neg__")?
                    } else {
                        timedelta
                    };
                    Ok((py_timedelta, rest))
                }
                12 => {
                    // TIME with microseconds
                    let time = zero_mysql::protocol::value::Time12::ref_from_bytes(&rest[..12])
                        .map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyException, _>("Invalid Time12")
                        })?;
                    rest = &rest[12..];
                    let timedelta_class = get_timedelta_class(py)?;
                    let timedelta = timedelta_class.call1((
                        time.days(),
                        time.second as i32,
                        time.microsecond(),
                        0, // milliseconds
                        time.minute as i32,
                        time.hour as i32,
                    ))?;
                    let py_timedelta = if time.is_negative() {
                        timedelta.call_method0("__neg__")?
                    } else {
                        timedelta
                    };
                    Ok((py_timedelta, rest))
                }
                _ => Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Invalid time length",
                )),
            }
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
            let (bytes, rest) = read_string_lenenc(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read text/blob")
            })?;

            let py_val = if is_binary_charset {
                pyo3::types::PyBytes::new(py, bytes).into_any()
            } else {
                pyo3::types::PyString::from_bytes(py, bytes)?.into_any()
            };
            Ok((py_val, rest))
        }

        // JSON: always decode as str (connection encoding, regardless of charset)
        ColumnType::MYSQL_TYPE_JSON => {
            let (bytes, rest) = read_string_lenenc(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read JSON")
            })?;
            Ok((
                pyo3::types::PyString::from_bytes(py, bytes)?.into_any(),
                rest,
            ))
        }

        // Decimal types
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let (bytes, rest) = read_string_lenenc(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read DECIMAL")
            })?;
            let decimal_str = pyo3::types::PyString::from_bytes(py, bytes)?;
            let decimal_class = get_decimal_class(py)?;
            Ok((decimal_class.call1((decimal_str,))?, rest))
        }

        // ENUM, SET, TYPED_ARRAY: always decode as str (ascii)
        ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET
        | ColumnType::MYSQL_TYPE_TYPED_ARRAY => {
            let (bytes, rest) = read_string_lenenc(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read value")
            })?;
            Ok((
                pyo3::types::PyString::from_bytes(py, bytes)?.into_any(),
                rest,
            ))
        }
    }
}

/// Decode text protocol value to Python object based on column type and flags
///
/// In the text protocol, all values come as strings and need to be parsed
/// according to the column type.
///
/// Returns the Python object.
pub fn decode_text_value_to_python<'py>(
    py: Python<'py>,
    col: &ColumnDefinitionTail,
    text_value: &[u8],
) -> PyResult<Bound<'py, PyAny>> {
    let type_and_flags = col.type_and_flags().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyException, _>("Failed to get type_and_flags")
    })?;
    let is_unsigned = type_and_flags.flags.contains(ColumnFlags::UNSIGNED_FLAG);
    let is_binary_charset = col.charset() == BINARY_CHARSET;

    match type_and_flags.column_type {
        ColumnType::MYSQL_TYPE_NULL => Ok(py.None().into_bound(py)),

        // Integer types - parse from string
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

        // Floating point types - parse from string
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

        // Temporal types - parse from string
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
