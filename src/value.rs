use crate::error::Error;
use crate::py_imports::{
    get_date_class, get_datetime_class, get_decimal_class, get_json_module, get_timedelta_class,
};
use mysql_common::Value as MySqlValue;
use mysql_common::constants::ColumnType;
use mysql_common::packets::Column;
use pyo3::types::PyByteArray;
use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    pybacked::{PyBackedBytes, PyBackedStr},
    types::{PyBytes, PyString},
};
use simdutf8::basic::from_utf8;

/// Zero-copy MySQL value type using PyBackedStr and PyBackedBytes
///
/// This enum is similar to mysql_common::Value but uses PyO3's zero-copy types
/// for string and byte data, avoiding unnecessary allocations when converting
/// from Python to Rust.
///
/// Note: This type does not implement Clone because PyBackedBytes and PyBackedStr
/// are non-cloneable zero-copy views into Python objects.
#[derive(Debug)]
pub enum Value {
    /// NULL value
    NULL,

    /// Byte data (zero-copy from Python bytes/bytearray)
    Bytes(PyBackedBytes),

    /// String data (zero-copy from Python str)
    Str(PyBackedStr),

    /// Signed 64-bit integer
    Int(i64),

    /// Unsigned 64-bit integer
    UInt(u64),

    /// 32-bit floating point
    Float(f32),

    /// 64-bit floating point
    Double(f64),

    /// Date/DateTime: year, month, day, hour, minutes, seconds, micro seconds
    Date(u16, u8, u8, u8, u8, u8, u32),

    /// Time/Duration: is negative, days, hours, minutes, seconds, micro seconds
    Time(bool, u32, u8, u8, u8, u32),
}

impl FromPyObject<'_, '_> for Value {
    type Error = PyErr;

    fn extract(ob: Borrowed<PyAny>) -> Result<Self, Self::Error> {
        let py = ob.py();

        // Get the type object and its name
        let type_obj = ob.get_type();
        let type_name = type_obj.name()?;

        // Match on type name
        match type_name.to_str()? {
            "NoneType" => Ok(Value::NULL),

            "bool" => {
                let v = ob.extract::<bool>()?;
                Ok(Value::Int(v as i64))
            }

            "int" => {
                // Try to fit in i64 first, then u64, otherwise convert to string
                if let Ok(v) = ob.extract::<i64>() {
                    Ok(Value::Int(v))
                } else if let Ok(v) = ob.extract::<u64>() {
                    Ok(Value::UInt(v))
                } else {
                    // Integer too large for i64/u64, store as zero-copy string
                    let int_str = ob.str()?;
                    let backed_str = int_str.extract::<PyBackedStr>()?;
                    Ok(Value::Str(backed_str))
                }
            }

            "float" => {
                let v = ob.extract::<f64>()?;
                Ok(Value::Double(v))
            }

            "str" => {
                // Zero-copy string extraction
                let backed_str = ob.extract::<PyBackedStr>()?;
                Ok(Value::Str(backed_str))
            }

            "bytes" => {
                // Zero-copy bytes extraction
                let backed_bytes = ob.extract::<PyBackedBytes>()?;
                Ok(Value::Bytes(backed_bytes))
            }

            "bytearray" => {
                // Extract from bytearray (requires a copy since PyBackedBytes doesn't support bytearray)
                let v = ob.cast::<PyByteArray>()?;
                // We need to create bytes from bytearray
                let bytes_obj = PyBytes::new(py, &v.to_vec());
                let backed_bytes = bytes_obj.extract::<PyBackedBytes>()?;
                Ok(Value::Bytes(backed_bytes))
            }

            "tuple" | "list" | "set" | "frozenset" | "dict" => {
                // Serialize collections to JSON as zero-copy string
                let json_module = get_json_module(py)?;
                let json_str = json_module
                    .call_method1("dumps", (ob,))?
                    .extract::<PyBackedStr>()?;
                Ok(Value::Str(json_str))
            }

            "datetime" => {
                // datetime.datetime
                let year = ob.getattr("year")?.extract::<u16>()?;
                let month = ob.getattr("month")?.extract::<u8>()?;
                let day = ob.getattr("day")?.extract::<u8>()?;
                let hour = ob.getattr("hour")?.extract::<u8>()?;
                let minute = ob.getattr("minute")?.extract::<u8>()?;
                let second = ob.getattr("second")?.extract::<u8>()?;
                let microsecond = ob.getattr("microsecond")?.extract::<u32>()?;
                Ok(Value::Date(
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    microsecond,
                ))
            }

            "date" => {
                // datetime.date
                let year = ob.getattr("year")?.extract::<u16>()?;
                let month = ob.getattr("month")?.extract::<u8>()?;
                let day = ob.getattr("day")?.extract::<u8>()?;
                Ok(Value::Date(year, month, day, 0, 0, 0, 0))
            }

            "time" => {
                // datetime.time
                let hour = ob.getattr("hour")?.extract::<u8>()?;
                let minute = ob.getattr("minute")?.extract::<u8>()?;
                let second = ob.getattr("second")?.extract::<u8>()?;
                let microsecond = ob.getattr("microsecond")?.extract::<u32>()?;
                Ok(Value::Time(false, 0, hour, minute, second, microsecond))
            }

            "timedelta" => {
                // datetime.timedelta
                let total_seconds = ob.call_method0("total_seconds")?.extract::<f64>()?;
                let is_negative = total_seconds < 0.0;
                let abs_seconds = total_seconds.abs();

                let days = (abs_seconds / 86400.0) as u32;
                let remaining = abs_seconds % 86400.0;
                let hours = (remaining / 3600.0) as u8;
                let remaining = remaining % 3600.0;
                let minutes = (remaining / 60.0) as u8;
                let seconds = (remaining % 60.0) as u8;
                let microseconds = ((remaining % 1.0) * 1_000_000.0) as u32;

                Ok(Value::Time(
                    is_negative,
                    days,
                    hours,
                    minutes,
                    seconds,
                    microseconds,
                ))
            }

            "struct_time" => {
                // time.struct_time
                let year = ob.getattr("tm_year")?.extract::<u16>()?;
                let month = ob.getattr("tm_mon")?.extract::<u8>()?;
                let day = ob.getattr("tm_mday")?.extract::<u8>()?;
                let hour = ob.getattr("tm_hour")?.extract::<u8>()?;
                let minute = ob.getattr("tm_min")?.extract::<u8>()?;
                let second = ob.getattr("tm_sec")?.extract::<u8>()?;
                Ok(Value::Date(year, month, day, hour, minute, second, 0))
            }

            "Decimal" => {
                // decimal.Decimal - convert to zero-copy string
                let decimal_str = ob.str()?.extract::<PyBackedStr>()?;
                Ok(Value::Str(decimal_str))
            }

            "UUID" => {
                // uuid.UUID - convert hex to zero-copy string
                let hex = ob.getattr("hex")?.extract::<PyBackedStr>()?;
                Ok(Value::Str(hex))
            }

            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Unsupported value type: {:?}",
                type_obj.fully_qualified_name()
            ))),
        }
    }
}

/// `value` is copied to the Python heap
pub fn value_to_python<'py>(
    py: Python<'py>,
    value: &MySqlValue,
    column: &Column,
) -> PyResult<Bound<'py, PyAny>> {
    // Handle NULL first as it's independent of column type
    if matches!(value, MySqlValue::NULL) {
        return Ok(py.None().into_bound(py));
    }

    let col_type = column.column_type();

    let bound = match col_type {
        // Date type
        ColumnType::MYSQL_TYPE_DATE => {
            match value {
                MySqlValue::Date(year, month, day, _, _, _, _) => {
                    get_date_class(py)?.call1((year, month, day))?
                }
                MySqlValue::Bytes(b) => {
                    let date_str = from_utf8(b).map_err(|_| Error::decode_error(col_type, b))?;

                    // Parse MySQL date format: YYYY-MM-DD
                    let parts: Vec<&str> = date_str.split('-').collect();
                    if parts.len() != 3 {
                        return Err(Error::decode_error(col_type, b).into());
                    }

                    let year = parts[0]
                        .parse::<u16>()
                        .map_err(|_| Error::decode_error(col_type, b))?;
                    let month = parts[1]
                        .parse::<u8>()
                        .map_err(|_| Error::decode_error(col_type, b))?;
                    let day = parts[2]
                        .parse::<u8>()
                        .map_err(|_| Error::decode_error(col_type, b))?;

                    get_date_class(py)?.call1((year, month, day))?
                }
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Unexpected value type for DATE column: {:?}",
                        value
                    )));
                }
            }
        }

        // Time type
        ColumnType::MYSQL_TYPE_TIME => {
            match value {
                MySqlValue::Time(is_negative, days, hours, minutes, seconds, microseconds) => {
                    let timedelta = get_timedelta_class(py)?.call1((
                        days,
                        seconds,
                        microseconds,
                        0,
                        minutes,
                        hours,
                    ))?;
                    if *is_negative {
                        timedelta.call_method0("__neg__")?
                    } else {
                        timedelta
                    }
                }
                MySqlValue::Bytes(b) => {
                    let time_str = from_utf8(b).map_err(|_| Error::decode_error(col_type, b))?;

                    // Parse MySQL time format: HH:MM:SS or HH:MM:SS.ffffff
                    // Can also be negative and exceed 24 hours for TIME type
                    let (is_negative, time_part) =
                        if let Some(time_str) = time_str.strip_prefix('-') {
                            (true, time_str)
                        } else {
                            (false, time_str)
                        };

                    let parts: Vec<&str> = time_part.split(':').collect();
                    if parts.len() != 3 {
                        return Err(Error::decode_error(col_type, b).into());
                    }

                    let hour = parts[0]
                        .parse::<u32>()
                        .map_err(|_| Error::decode_error(col_type, b))?;
                    let minute = parts[1]
                        .parse::<u8>()
                        .map_err(|_| Error::decode_error(col_type, b))?;

                    let (second, microsecond) = if let Some((sec_str, micro_str)) =
                        parts[2].split_once('.')
                    {
                        let second = sec_str
                            .parse::<u8>()
                            .map_err(|_| Error::decode_error(col_type, b))?;
                        let micro_padded = format!("{:0<6}", &micro_str[..micro_str.len().min(6)]);
                        let microsecond = micro_padded
                            .parse::<u32>()
                            .map_err(|_| Error::decode_error(col_type, b))?;
                        (second, microsecond)
                    } else {
                        let second = parts[2]
                            .parse::<u8>()
                            .map_err(|_| Error::decode_error(col_type, b))?;
                        (second, 0)
                    };

                    // MySQL TIME can exceed 24 hours, so use timedelta
                    let total_seconds = hour * 3600 + minute as u32 * 60 + second as u32;
                    let days = total_seconds / 86400;
                    let remaining_seconds = total_seconds % 86400;

                    let timedelta =
                        get_timedelta_class(py)?.call1((days, remaining_seconds, microsecond))?;
                    if is_negative {
                        timedelta.call_method0("__neg__")?
                    } else {
                        timedelta
                    }
                }
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Unexpected value type for TIME column: {:?}",
                        value
                    )));
                }
            }
        }

        // DateTime and Timestamp types
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_TIMESTAMP => {
            match value {
                MySqlValue::Date(year, month, day, hour, minutes, seconds, microseconds) => {
                    get_datetime_class(py)?.call1((
                        year,
                        month,
                        day,
                        hour,
                        minutes,
                        seconds,
                        microseconds,
                    ))?
                }
                MySqlValue::Bytes(b) => {
                    let datetime_str =
                        from_utf8(b).map_err(|_| Error::decode_error(col_type, b))?;

                    // Parse MySQL datetime format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff
                    let parts: Vec<&str> = datetime_str.split(' ').collect();
                    if parts.len() != 2 {
                        return Err(Error::decode_error(col_type, b).into());
                    }

                    let date_parts: Vec<&str> = parts[0].split('-').collect();
                    if date_parts.len() != 3 {
                        return Err(Error::decode_error(col_type, b).into());
                    }

                    let time_parts: Vec<&str> = parts[1].split(':').collect();
                    if time_parts.len() != 3 {
                        return Err(Error::decode_error(col_type, b).into());
                    }

                    let year = date_parts[0]
                        .parse::<u16>()
                        .map_err(|_| Error::decode_error(col_type, b))?;
                    let month = date_parts[1]
                        .parse::<u8>()
                        .map_err(|_| Error::decode_error(col_type, b))?;
                    let day = date_parts[2]
                        .parse::<u8>()
                        .map_err(|_| Error::decode_error(col_type, b))?;
                    let hour = time_parts[0]
                        .parse::<u8>()
                        .map_err(|_| Error::decode_error(col_type, b))?;
                    let minute = time_parts[1]
                        .parse::<u8>()
                        .map_err(|_| Error::decode_error(col_type, b))?;

                    let (second, microsecond) = if let Some((sec_str, micro_str)) =
                        time_parts[2].split_once('.')
                    {
                        let second = sec_str
                            .parse::<u8>()
                            .map_err(|_| Error::decode_error(col_type, b))?;
                        let micro_padded = format!("{:0<6}", &micro_str[..micro_str.len().min(6)]);
                        let microsecond = micro_padded
                            .parse::<u32>()
                            .map_err(|_| Error::decode_error(col_type, b))?;
                        (second, microsecond)
                    } else {
                        let second = time_parts[2]
                            .parse::<u8>()
                            .map_err(|_| Error::decode_error(col_type, b))?;
                        (second, 0)
                    };

                    get_datetime_class(py)?.call1((
                        year,
                        month,
                        day,
                        hour,
                        minute,
                        second,
                        microsecond,
                    ))?
                }
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Unexpected value type for DATETIME/TIMESTAMP column: {:?}",
                        value
                    )));
                }
            }
        }

        // Integer types
        ColumnType::MYSQL_TYPE_LONGLONG
        | ColumnType::MYSQL_TYPE_LONG
        | ColumnType::MYSQL_TYPE_INT24
        | ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_TINY
        | ColumnType::MYSQL_TYPE_YEAR => {
            match value {
                MySqlValue::Int(i) => i.into_bound_py_any(py)?,
                MySqlValue::UInt(u) => u.into_bound_py_any(py)?,
                MySqlValue::Bytes(b) => {
                    match from_utf8(b) {
                        Ok(int_str) => {
                            // Use PyLong::from_str to handle arbitrarily large integers
                            match py.import("builtins")?.getattr("int")?.call1((int_str,)) {
                                Ok(py_int) => py_int,
                                Err(_) => PyBytes::new(py, b).into_any(),
                            }
                        }
                        Err(_) => PyBytes::new(py, b).into_any(),
                    }
                }
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Unexpected value type for integer column: {:?}",
                        value
                    )));
                }
            }
        }

        // Floating point types
        ColumnType::MYSQL_TYPE_FLOAT | ColumnType::MYSQL_TYPE_DOUBLE => {
            match value {
                MySqlValue::Float(f) => {
                    let mut buffer = ryu::Buffer::new();
                    buffer
                        .format(*f)
                        .parse::<f64>()
                        .unwrap() // unwrap(): f32 -> str -> f64 never fails
                        .into_bound_py_any(py)?
                }
                MySqlValue::Double(f) => f.into_bound_py_any(py)?,
                MySqlValue::Bytes(b) => match from_utf8(b) {
                    Ok(float_str) => match float_str.parse::<f64>() {
                        Ok(f) => f.into_bound_py_any(py)?,
                        Err(_) => PyBytes::new(py, b).into_any(),
                    },
                    Err(_) => PyBytes::new(py, b).into_any(),
                },
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Unexpected value type for float column: {:?}",
                        value
                    )));
                }
            }
        }

        // JSON type
        ColumnType::MYSQL_TYPE_JSON => match value {
            MySqlValue::Bytes(b) => match PyString::from_bytes(py, b) {
                Ok(json_str) => {
                    let json_module = get_json_module(py)?;
                    json_module.call_method1("loads", (json_str,))?
                }
                Err(_) => PyBytes::new(py, b).into_any(),
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Unexpected value type for JSON column: {:?}",
                    value
                )));
            }
        },

        // Decimal types
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => match value {
            MySqlValue::Bytes(b) => match PyString::from_bytes(py, b) {
                Ok(decimal_str) => get_decimal_class(py)?.call1((decimal_str,))?,
                Err(_) => PyBytes::new(py, b).into_any(),
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Unexpected value type for DECIMAL column: {:?}",
                    value
                )));
            }
        },

        // Text and string types
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_BLOB => {
            match value {
                MySqlValue::Bytes(b) => {
                    if column.character_set() == 63 {
                        PyBytes::new(py, b).into_any()
                    } else {
                        // TODO: this can be non-utf8 if character_set_results is not utf8*
                        match PyString::from_bytes(py, b) {
                            Ok(s) => s.into_any(),
                            Err(_) => PyBytes::new(py, b).into_any(),
                        }
                    }
                }
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Unexpected value type for text column: {:?}",
                        value
                    )));
                }
            }
        }

        // ENUM and SET types
        ColumnType::MYSQL_TYPE_ENUM | ColumnType::MYSQL_TYPE_SET => match value {
            MySqlValue::Bytes(b) => match PyString::from_bytes(py, b) {
                Ok(s) => s.into_any(),
                Err(_) => PyBytes::new(py, b).into_any(),
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Unexpected value type for ENUM/SET column: {:?}",
                    value
                )));
            }
        },

        // BIT type
        ColumnType::MYSQL_TYPE_BIT => match value {
            MySqlValue::Bytes(b) => PyBytes::new(py, b).into_any(),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Unexpected value type for BIT column: {:?}",
                    value
                )));
            }
        },

        // GEOMETRY type
        ColumnType::MYSQL_TYPE_GEOMETRY => match value {
            MySqlValue::Bytes(b) => PyBytes::new(py, b).into_any(),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Unexpected value type for GEOMETRY column: {:?}",
                    value
                )));
            }
        },

        // Default: handle any unimplemented types
        _ => {
            log::error!("Unimplemented column type: {:?}", col_type);
            match value {
                MySqlValue::Int(i) => i.into_bound_py_any(py)?,
                MySqlValue::UInt(u) => u.into_bound_py_any(py)?,
                MySqlValue::Float(f) => {
                    let mut buffer = ryu::Buffer::new();
                    buffer
                        .format(*f)
                        .parse::<f64>()
                        .unwrap()
                        .into_bound_py_any(py)?
                }
                MySqlValue::Double(f) => f.into_bound_py_any(py)?,
                MySqlValue::Date(year, month, day, hour, minutes, seconds, microseconds) => {
                    get_datetime_class(py)?.call1((
                        year,
                        month,
                        day,
                        hour,
                        minutes,
                        seconds,
                        microseconds,
                    ))?
                }
                MySqlValue::Time(is_negative, days, hours, minutes, seconds, microseconds) => {
                    let timedelta = get_timedelta_class(py)?.call1((
                        days,
                        seconds,
                        microseconds,
                        0,
                        minutes,
                        hours,
                    ))?;
                    if *is_negative {
                        timedelta.call_method0("__neg__")?
                    } else {
                        timedelta
                    }
                }
                MySqlValue::Bytes(b) => match PyString::from_bytes(py, b) {
                    Ok(s) => s.into_any(),
                    Err(_) => PyBytes::new(py, b).into_any(),
                },
                MySqlValue::NULL => unreachable!(), // Already handled at the beginning
            }
        }
    };

    Ok(bound)
}

impl Value {
    /// Convert to mysql_common::Value for compatibility with mysql crate
    pub fn to_mysql_value(&self) -> MySqlValue {
        match self {
            Value::NULL => MySqlValue::NULL,
            Value::Bytes(b) => {
                let bytes: &[u8] = b.as_ref();
                MySqlValue::Bytes(bytes.to_vec())
            }
            Value::Str(s) => {
                let str_ref: &str = s.as_ref();
                MySqlValue::Bytes(str_ref.as_bytes().to_vec())
            }
            Value::Int(v) => MySqlValue::Int(*v),
            Value::UInt(v) => MySqlValue::UInt(*v),
            Value::Float(v) => MySqlValue::Float(*v),
            Value::Double(v) => MySqlValue::Double(*v),
            Value::Date(year, month, day, hour, minute, second, micro) => {
                MySqlValue::Date(*year, *month, *day, *hour, *minute, *second, *micro)
            }
            Value::Time(neg, days, hours, minutes, seconds, micro) => {
                MySqlValue::Time(*neg, *days, *hours, *minutes, *seconds, *micro)
            }
        }
    }

    /// Get a reference to the bytes (if this is a Bytes or Str variant)
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => {
                let bytes_ref: &[u8] = b.as_ref();
                Some(bytes_ref)
            }
            Value::Str(s) => {
                let str_ref: &str = s.as_ref();
                Some(str_ref.as_bytes())
            }
            _ => None,
        }
    }

    /// Get a reference to the string (if this is a Str variant)
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => {
                let str_ref: &str = s.as_ref();
                Some(str_ref)
            }
            Value::Bytes(b) => {
                let bytes_ref: &[u8] = b.as_ref();
                std::str::from_utf8(bytes_ref).ok()
            }
            _ => None,
        }
    }

    /// Check if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::NULL)
    }
}

// Implement ToSql for diesel backend using Text SQL type
use diesel::mysql::Mysql;
use diesel::serialize::{self, IsNull, Output, ToSql};
use std::io::Write;

impl ToSql<(), Mysql> for Value {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Mysql>) -> serialize::Result {
        match self {
            Value::NULL => Ok(IsNull::Yes),
            Value::Bytes(b) => {
                let bytes_ref: &[u8] = b.as_ref();
                out.write_all(bytes_ref)?;
                Ok(IsNull::No)
            }
            Value::Str(s) => {
                let str_ref: &str = s.as_ref();
                out.write_all(str_ref.as_bytes())?;
                Ok(IsNull::No)
            }
            Value::Int(v) => {
                // Diesel encodes integers as strings for MySQL
                write!(out, "{}", v)?;
                Ok(IsNull::No)
            }
            Value::UInt(v) => {
                write!(out, "{}", v)?;
                Ok(IsNull::No)
            }
            Value::Float(v) => {
                write!(out, "{}", v)?;
                Ok(IsNull::No)
            }
            Value::Double(v) => {
                write!(out, "{}", v)?;
                Ok(IsNull::No)
            }
            Value::Date(year, month, day, hour, minute, second, micro) => {
                // Format as MySQL DATETIME: YYYY-MM-DD HH:MM:SS.microseconds
                if *micro > 0 {
                    write!(
                        out,
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                        year, month, day, hour, minute, second, micro
                    )?;
                } else if *hour == 0 && *minute == 0 && *second == 0 {
                    write!(out, "{:04}-{:02}-{:02}", year, month, day)?;
                } else {
                    write!(
                        out,
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                        year, month, day, hour, minute, second
                    )?;
                }
                Ok(IsNull::No)
            }
            Value::Time(is_negative, days, hours, minutes, seconds, micro) => {
                // Format as MySQL TIME: [H]HH:MM:SS[.microseconds]
                let total_hours = *days as i32 * 24 + *hours as i32;
                if *micro > 0 {
                    write!(
                        out,
                        "{}{:02}:{:02}:{:02}.{:06}",
                        if *is_negative { "-" } else { "" },
                        total_hours,
                        minutes,
                        seconds,
                        micro
                    )?;
                } else {
                    write!(
                        out,
                        "{}{:02}:{:02}:{:02}",
                        if *is_negative { "-" } else { "" },
                        total_hours,
                        minutes,
                        seconds
                    )?;
                }
                Ok(IsNull::No)
            }
        }
    }
}
