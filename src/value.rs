use color_eyre::Result;
use mysql_common::Value as MySqlValue;
use mysql_common::constants::ColumnFlags;
use mysql_common::constants::ColumnType;
use mysql_common::packets::Column;
use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    sync::PyOnceLock,
    types::{PyBytes, PyString},
};

static DATETIME_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DATE_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static TIME_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static TIMEDELTA_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DECIMAL_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static JSON_MODULE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
static STRUCT_TIME_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn get_datetime_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(DATETIME_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "datetime")
                .unwrap()
                .getattr("datetime")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

fn get_timedelta_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(TIMEDELTA_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "datetime")
                .unwrap()
                .getattr("timedelta")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

fn get_decimal_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(DECIMAL_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "decimal")
                .unwrap()
                .getattr("Decimal")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

fn get_json_module<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyModule>> {
    Ok(JSON_MODULE
        .get_or_init(py, || PyModule::import(py, "json").unwrap().unbind())
        .bind(py))
}

fn get_date_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(DATE_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "datetime")
                .unwrap()
                .getattr("date")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

fn get_time_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(TIME_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "datetime")
                .unwrap()
                .getattr("time")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

fn get_struct_time_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(STRUCT_TIME_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "time")
                .unwrap()
                .getattr("struct_time")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

#[derive(Clone)]
pub struct Value {
    pub inner: MySqlValue,
}

impl FromPyObject<'_, '_> for Value {
    type Error = PyErr;

    fn extract(ob: Borrowed<PyAny>) -> Result<Self, Self::Error> {
        let py = ob.py();

        // Get the type object and its name
        let type_obj = ob.get_type();
        let type_name = type_obj.name()?; // TODO: use qualname() with 'builtins.int' for precise ident

        // Match on type name
        let inner = match type_name.to_str()? {
            "NoneType" => MySqlValue::NULL,
            "bool" => {
                let v = ob.extract::<bool>()?;
                MySqlValue::Int(v as i64)
            }
            "int" => {
                // Try to fit in i64 first, then u64, otherwise convert to string
                if let Ok(v) = ob.extract::<i64>() {
                    MySqlValue::Int(v)
                } else if let Ok(v) = ob.extract::<u64>() {
                    MySqlValue::UInt(v)
                } else {
                    // Integer too large for i64/u64, store as string
                    let int_str = ob.str()?.to_str()?.as_bytes().to_vec();
                    MySqlValue::Bytes(int_str)
                }
            }
            "float" => {
                let v = ob.extract::<f64>()?;
                MySqlValue::Double(v)
            }
            "str" => {
                let v = ob.extract::<String>()?;
                MySqlValue::Bytes(v.into_bytes())
            }
            "bytes" => {
                let v = ob.extract::<&[u8]>()?;
                MySqlValue::Bytes(v.to_vec())
            }
            "bytearray" => {
                let v = ob.extract::<&[u8]>()?;
                MySqlValue::Bytes(v.to_vec())
            }
            "tuple" | "list" | "set" | "frozenset" | "dict" => {
                // Serialize collections to JSON
                let json_module = get_json_module(py)?;
                let json_str = json_module
                    .call_method1("dumps", (ob,))?
                    .extract::<String>()?;
                MySqlValue::Bytes(json_str.into_bytes())
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
                MySqlValue::Date(year, month, day, hour, minute, second, microsecond)
            }
            "date" => {
                // datetime.date
                let year = ob.getattr("year")?.extract::<u16>()?;
                let month = ob.getattr("month")?.extract::<u8>()?;
                let day = ob.getattr("day")?.extract::<u8>()?;
                MySqlValue::Date(year, month, day, 0, 0, 0, 0)
            }
            "time" => {
                // datetime.time
                let hour = ob.getattr("hour")?.extract::<u8>()?;
                let minute = ob.getattr("minute")?.extract::<u8>()?;
                let second = ob.getattr("second")?.extract::<u8>()?;
                let microsecond = ob.getattr("microsecond")?.extract::<u32>()?;
                MySqlValue::Time(false, 0, hour, minute, second, microsecond)
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

                MySqlValue::Time(is_negative, days, hours, minutes, seconds, microseconds)
            }
            "struct_time" => {
                // time.struct_time
                let year = ob.getattr("tm_year")?.extract::<u16>()?;
                let month = ob.getattr("tm_mon")?.extract::<u8>()?;
                let day = ob.getattr("tm_mday")?.extract::<u8>()?;
                let hour = ob.getattr("tm_hour")?.extract::<u8>()?;
                let minute = ob.getattr("tm_min")?.extract::<u8>()?;
                let second = ob.getattr("tm_sec")?.extract::<u8>()?;
                MySqlValue::Date(year, month, day, hour, minute, second, 0)
            }
            "Decimal" => {
                // decimal.Decimal
                let decimal_str = ob.str()?.to_str()?.as_bytes().to_vec();
                MySqlValue::Bytes(decimal_str)
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Unsupported value type: {}",
                    type_name
                )));
            }
        };

        Ok(Value { inner })
    }
}

/// `value` is copied to the Python heap
pub fn value_to_python<'py>(
    py: Python<'py>,
    value: &MySqlValue,
    column: &Column,
) -> Result<Bound<'py, PyAny>> {
    let bound = match value {
        MySqlValue::NULL => py.None().into_bound(py),
        MySqlValue::Int(i) => i.into_bound_py_any(py)?,
        MySqlValue::UInt(u) => u.into_bound_py_any(py)?,
        MySqlValue::Float(f) => f.into_bound_py_any(py)?,
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
            let timedelta =
                get_timedelta_class(py)?.call1((days, seconds, microseconds, 0, minutes, hours))?;
            if *is_negative {
                timedelta.call_method0("__neg__")?
            } else {
                timedelta
            }
        }
        MySqlValue::Bytes(b) => {
            // Use column metadata to determine the best Python type
            let col_type = column.column_type();
            let flags = column.flags();

            // Note: column.column_length() provides max length for string types
            // column.decimals() provides scale for DECIMAL or fractional seconds for TIME types
            // These aren't needed for Bytes conversion as MySQL already formats the data
            match col_type {
                // JSON columns - parse as Python dict/list
                ColumnType::MYSQL_TYPE_JSON => match PyString::from_bytes(py, b) {
                    Ok(json_str) => {
                        let json_module = get_json_module(py)?;
                        json_module.call_method1("loads", (json_str,))?
                    }
                    Err(_) => PyBytes::new(py, b).into_any(),
                },

                // Decimal/Numeric - use Python Decimal class
                // Note: Precision/scale info from col.decimals() isn't needed here
                // because MySQL already sends the string with correct precision applied
                ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                    match PyString::from_bytes(py, b) {
                        Ok(decimal_str) => get_decimal_class(py)?.call1((decimal_str,))?,
                        Err(_) => PyBytes::new(py, b).into_any(),
                    }
                }

                // Text types - return as str
                ColumnType::MYSQL_TYPE_VARCHAR
                | ColumnType::MYSQL_TYPE_VAR_STRING
                | ColumnType::MYSQL_TYPE_STRING => {
                    // Check if it's a BINARY flag (BINARY/VARBINARY columns)
                    if flags.contains(ColumnFlags::BINARY_FLAG) {
                        PyBytes::new(py, &b).into_any()
                    } else {
                        // Text column - try UTF-8, fall back to bytes
                        match PyString::from_bytes(py, b) {
                            Ok(s) => s.into_any(),
                            Err(_) => PyBytes::new(py, b).into_any(),
                        }
                    }
                }

                // BLOB types - always return as bytes
                ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_BLOB => PyBytes::new(py, &b).into_any(),

                // ENUM and SET - return as str
                ColumnType::MYSQL_TYPE_ENUM | ColumnType::MYSQL_TYPE_SET => {
                    match PyString::from_bytes(py, b) {
                        Ok(s) => s.into_any(),
                        Err(_) => PyBytes::new(py, &b).into_any(),
                    }
                }

                // Integer types - parse bytes as integers
                ColumnType::MYSQL_TYPE_LONGLONG
                | ColumnType::MYSQL_TYPE_LONG
                | ColumnType::MYSQL_TYPE_INT24
                | ColumnType::MYSQL_TYPE_SHORT
                | ColumnType::MYSQL_TYPE_TINY
                | ColumnType::MYSQL_TYPE_YEAR => {
                    match std::str::from_utf8(b) {
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

                // BIT type - return as bytes
                ColumnType::MYSQL_TYPE_BIT => PyBytes::new(py, &b).into_any(),

                // GEOMETRY type - return as bytes (WKB format)
                ColumnType::MYSQL_TYPE_GEOMETRY => PyBytes::new(py, &b).into_any(),

                // Default: try string, fall back to bytes
                _ => match PyString::from_bytes(py, b) {
                    Ok(s) => s.into_any(),
                    Err(_) => PyBytes::new(py, b).into_any(),
                },
            }
        }
    };
    Ok(bound)
}

impl From<Value> for MySqlValue {
    fn from(value: Value) -> Self {
        value.inner
    }
}

impl From<MySqlValue> for Value {
    fn from(value: MySqlValue) -> Self {
        Value { inner: value }
    }
}
