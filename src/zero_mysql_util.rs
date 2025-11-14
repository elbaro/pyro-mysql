/// Utilities for converting zero-mysql values to Python objects
use pyo3::{prelude::*, IntoPyObjectExt};
use zero_mysql::protocol::value::Value;

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
pub fn zero_mysql_value_to_python<'py>(py: Python<'py>, value: Value) -> PyResult<Bound<'py, PyAny>> {
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
