use pyo3::types::PyByteArray;
use pyo3::{
    prelude::*,
    pybacked::{PyBackedBytes, PyBackedStr},
    types::PyBytes,
};

use crate::py_imports::get_json_module;

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

impl Value {
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
