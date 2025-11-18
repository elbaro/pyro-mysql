/// Utilities for converting zero-mysql values to Python objects
use pyo3::{prelude::*, IntoPyObjectExt};
use zero_mysql::col::ColumnTypeAndFlags;
use zero_mysql::constant::{ColumnFlags, ColumnType};
use zero_mysql::protocol::primitive::*;
use zero_mysql::protocol::value::Value;
use zerocopy::FromBytes;

/// Convert zero_mysql::Value to Python object
///
/// This function converts values from the zero-mysql crate's Value type
/// to Python objects. It handles all MySQL data types including:
/// - NULL
/// - Integers (signed and unsigned)
/// - Floating point numbers
/// - Timestamps (dates and datetimes)
/// - Time values (as timedelta)
/// - Byte strings (as str or bytes)
pub fn zero_mysql_value_to_python<'py>(
    py: Python<'py>,
    value: Value,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        Value::Null => Ok(py.None().into_bound(py)),

        Value::SignedInt(i) => Ok(i.into_bound_py_any(py)?),

        Value::UnsignedInt(u) => Ok(u.into_bound_py_any(py)?),

        Value::Float(f) => {
            // Convert f32 to f64 via string to maintain precision
            let mut buffer = ryu::Buffer::new();
            let f64_val = buffer.format(f).parse::<f64>().unwrap(); // f32 -> str -> f64 never fails
            Ok(f64_val.into_bound_py_any(py)?)
        }

        Value::Double(d) => Ok(d.into_bound_py_any(py)?),

        Value::Timestamp0 => {
            // 0000-00-00 00:00:00 - return None for zero timestamp
            Ok(py.None().into_bound(py))
        }

        Value::Timestamp4(ts) => {
            // DATE: year, month, day
            let datetime_module = py.import("datetime")?;
            let date_class = datetime_module.getattr("date")?;
            date_class.call1((ts.year(), ts.month, ts.day))
        }

        Value::Timestamp7(ts) => {
            // DATETIME without microseconds
            let datetime_module = py.import("datetime")?;
            let datetime_class = datetime_module.getattr("datetime")?;
            datetime_class.call1((ts.year(), ts.month, ts.day, ts.hour, ts.minute, ts.second))
        }

        Value::Timestamp11(ts) => {
            // DATETIME with microseconds
            let datetime_module = py.import("datetime")?;
            let datetime_class = datetime_module.getattr("datetime")?;
            datetime_class.call1((
                ts.year(),
                ts.month,
                ts.day,
                ts.hour,
                ts.minute,
                ts.second,
                ts.microsecond(),
            ))
        }

        Value::Time0 => {
            // 00:00:00 - return timedelta(0)
            let datetime_module = py.import("datetime")?;
            let timedelta_class = datetime_module.getattr("timedelta")?;
            timedelta_class.call1((0,))
        }

        Value::Time8(time) => {
            // TIME without microseconds
            let datetime_module = py.import("datetime")?;
            let timedelta_class = datetime_module.getattr("timedelta")?;
            let timedelta = timedelta_class.call1((
                time.days(),
                time.second as i32,
                0, // microseconds
                0, // milliseconds
                time.minute as i32,
                time.hour as i32,
            ))?;
            if time.is_negative() {
                timedelta.call_method0("__neg__")
            } else {
                Ok(timedelta)
            }
        }

        Value::Time12(time) => {
            // TIME with microseconds
            let datetime_module = py.import("datetime")?;
            let timedelta_class = datetime_module.getattr("timedelta")?;
            let timedelta = timedelta_class.call1((
                time.days(),
                time.second as i32,
                time.microsecond(),
                0, // milliseconds
                time.minute as i32,
                time.hour as i32,
            ))?;
            if time.is_negative() {
                timedelta.call_method0("__neg__")
            } else {
                Ok(timedelta)
            }
        }

        Value::Byte(bytes) => {
            // Try to decode as UTF-8 string, otherwise return bytes
            match simdutf8::basic::from_utf8(bytes) {
                Ok(s) => Ok(pyo3::types::PyString::new(py, s).into_any()),
                Err(_) => Ok(pyo3::types::PyBytes::new(py, bytes).into_any()),
            }
        }
    }
}

/// Directly decode raw bytes to Python object based on column type and flags
///
/// This function skips the intermediate `Value` enum and directly converts
/// from raw MySQL binary protocol bytes to Python objects for better performance.
///
/// Returns the Python object and the remaining bytes.
pub fn decode_bytes_to_python<'py, 'data>(
    py: Python<'py>,
    type_and_flags: &ColumnTypeAndFlags,
    data: &'data [u8],
) -> PyResult<(Bound<'py, PyAny>, &'data [u8])> {
    let is_unsigned = type_and_flags.flags.contains(ColumnFlags::UNSIGNED_FLAG);
    let is_binary = type_and_flags.flags.contains(ColumnFlags::BINARY_FLAG);

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

        // Temporal types
        ColumnType::MYSQL_TYPE_DATE
        | ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_NEWDATE => {
            let (len, mut rest) = read_int_1(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read timestamp length")
            })?;
            match len {
                0 => {
                    // 0000-00-00 00:00:00 - return None for zero timestamp
                    Ok((py.None().into_bound(py), rest))
                }
                4 => {
                    // DATE: year, month, day
                    let ts = zero_mysql::protocol::value::Timestamp4::ref_from_bytes(&rest[..4])
                        .map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyException, _>("Invalid Timestamp4")
                        })?;
                    rest = &rest[4..];
                    let datetime_module = py.import("datetime")?;
                    let date_class = datetime_module.getattr("date")?;
                    let py_date = date_class.call1((ts.year(), ts.month, ts.day))?;
                    Ok((py_date, rest))
                }
                7 => {
                    // DATETIME without microseconds
                    let ts = zero_mysql::protocol::value::Timestamp7::ref_from_bytes(&rest[..7])
                        .map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyException, _>("Invalid Timestamp7")
                        })?;
                    rest = &rest[7..];
                    let datetime_module = py.import("datetime")?;
                    let datetime_class = datetime_module.getattr("datetime")?;
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
                    let datetime_module = py.import("datetime")?;
                    let datetime_class = datetime_module.getattr("datetime")?;
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
                    let datetime_module = py.import("datetime")?;
                    let timedelta_class = datetime_module.getattr("timedelta")?;
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
                    let datetime_module = py.import("datetime")?;
                    let timedelta_class = datetime_module.getattr("timedelta")?;
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
                    let datetime_module = py.import("datetime")?;
                    let timedelta_class = datetime_module.getattr("timedelta")?;
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

        // String and BLOB types - length-encoded
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB => {
            let (bytes, rest) = read_string_lenenc(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read string")
            })?;

            let py_val = if is_binary {
                pyo3::types::PyBytes::new(py, bytes).into_any()
            } else {
                // TODO: handle unwrap()
                pyo3::types::PyString::from_bytes(py, bytes)
                    .unwrap()
                    .into_any()
            };
            Ok((py_val, rest))
        }

        ColumnType::MYSQL_TYPE_JSON => {
            let (bytes, rest) = read_string_lenenc(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read JSON")
            })?;
            Ok((
                pyo3::types::PyString::from_bytes(py, bytes)
                    .unwrap()
                    .into_any(),
                rest,
            ))
        }

        // Other types (DECIMAL, NEWDECIMAL, ENUM, SET, BIT, GEOMETRY, TYPED_ARRAY)
        ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_DECIMAL
        | ColumnType::MYSQL_TYPE_NEWDECIMAL
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET
        | ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_TYPED_ARRAY => {
            let (bytes, rest) = read_string_lenenc(data).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyException, _>("Failed to read value")
            })?;

            let py_val = match simdutf8::basic::from_utf8(bytes) {
                Ok(s) => pyo3::types::PyString::new(py, s).into_any(),
                Err(_) => pyo3::types::PyBytes::new(py, bytes).into_any(),
            };
            Ok((py_val, rest))
        }
    }
}
