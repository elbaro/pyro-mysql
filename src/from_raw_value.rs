use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use pyo3::IntoPyObjectExt;
use zero_mysql::error::{Error, Result};
use zero_mysql::raw::FromRawValue;
use zero_mysql::value::{Time12, Time8, Timestamp11, Timestamp4, Timestamp7};

use crate::py_imports::{
    get_date_class, get_datetime_class, get_decimal_class, get_timedelta_class,
};

/// Wrapper for Py<PyAny> that implements FromRawValue.
///
/// # Safety
/// All methods assume GIL is held. Only use in contexts where GIL is guaranteed
/// (e.g., sync handlers called from Python).
pub struct PyValue(pub Py<PyAny>);

/// Helper to get Python token assuming GIL is held
fn py() -> Python<'static> {
    // SAFETY: PyValue is only used in sync handlers where GIL is held
    unsafe { Python::assume_attached() }
}

/// Convert PyErr to zero_mysql::error::Error
fn py_err(e: PyErr) -> Error {
    Error::BadUsageError(e.to_string())
}

impl FromRawValue<'_> for PyValue {
    fn from_null() -> Result<Self> {
        Ok(PyValue(py().None()))
    }

    fn from_i8(v: i8) -> Result<Self> {
        Ok(PyValue((v as i64).into_py_any(py()).map_err(py_err)?))
    }

    fn from_i16(v: i16) -> Result<Self> {
        Ok(PyValue((v as i64).into_py_any(py()).map_err(py_err)?))
    }

    fn from_i32(v: i32) -> Result<Self> {
        Ok(PyValue((v as i64).into_py_any(py()).map_err(py_err)?))
    }

    fn from_i64(v: i64) -> Result<Self> {
        Ok(PyValue(v.into_py_any(py()).map_err(py_err)?))
    }

    fn from_u8(v: u8) -> Result<Self> {
        Ok(PyValue((v as u64).into_py_any(py()).map_err(py_err)?))
    }

    fn from_u16(v: u16) -> Result<Self> {
        Ok(PyValue((v as u64).into_py_any(py()).map_err(py_err)?))
    }

    fn from_u32(v: u32) -> Result<Self> {
        Ok(PyValue((v as u64).into_py_any(py()).map_err(py_err)?))
    }

    fn from_u64(v: u64) -> Result<Self> {
        Ok(PyValue(v.into_py_any(py()).map_err(py_err)?))
    }

    fn from_float(v: f32) -> Result<Self> {
        // Convert f32 to f64 via ryu to maintain precision
        let mut buffer = ryu::Buffer::new();
        let f64_val: f64 = buffer.format(v).parse().expect("ryu format is valid");
        Ok(PyValue(f64_val.into_py_any(py()).map_err(py_err)?))
    }

    fn from_double(v: f64) -> Result<Self> {
        Ok(PyValue(v.into_py_any(py()).map_err(py_err)?))
    }

    fn from_bytes(v: &[u8]) -> Result<Self> {
        Ok(PyValue(PyBytes::new(py(), v).into_py_any(py()).map_err(py_err)?))
    }

    fn from_str(v: &[u8]) -> Result<Self> {
        let s = PyString::from_bytes(py(), v).map_err(py_err)?;
        Ok(PyValue(s.into_py_any(py()).map_err(py_err)?))
    }

    fn from_decimal(v: &[u8]) -> Result<Self> {
        let py = py();
        let decimal_str = PyString::from_bytes(py, v).map_err(py_err)?;
        let decimal_class = get_decimal_class(py).map_err(py_err)?;
        let decimal = decimal_class.call1((decimal_str,)).map_err(py_err)?;
        Ok(PyValue(decimal.into_py_any(py).map_err(py_err)?))
    }

    fn from_date0() -> Result<Self> {
        Ok(PyValue(py().None()))
    }

    fn from_date4(v: &Timestamp4) -> Result<Self> {
        let py = py();
        let date_class = get_date_class(py).map_err(py_err)?;
        let date = date_class.call1((v.year(), v.month, v.day)).map_err(py_err)?;
        Ok(PyValue(date.into_py_any(py).map_err(py_err)?))
    }

    fn from_datetime0() -> Result<Self> {
        Ok(PyValue(py().None()))
    }

    fn from_datetime4(v: &Timestamp4) -> Result<Self> {
        let py = py();
        let datetime_class = get_datetime_class(py).map_err(py_err)?;
        let dt = datetime_class
            .call1((v.year(), v.month, v.day, 0, 0, 0))
            .map_err(py_err)?;
        Ok(PyValue(dt.into_py_any(py).map_err(py_err)?))
    }

    fn from_datetime7(v: &Timestamp7) -> Result<Self> {
        let py = py();
        let datetime_class = get_datetime_class(py).map_err(py_err)?;
        let dt = datetime_class
            .call1((v.year(), v.month, v.day, v.hour, v.minute, v.second))
            .map_err(py_err)?;
        Ok(PyValue(dt.into_py_any(py).map_err(py_err)?))
    }

    fn from_datetime11(v: &Timestamp11) -> Result<Self> {
        let py = py();
        let datetime_class = get_datetime_class(py).map_err(py_err)?;
        let dt = datetime_class
            .call1((
                v.year(),
                v.month,
                v.day,
                v.hour,
                v.minute,
                v.second,
                v.microsecond(),
            ))
            .map_err(py_err)?;
        Ok(PyValue(dt.into_py_any(py).map_err(py_err)?))
    }

    fn from_time0() -> Result<Self> {
        let py = py();
        let timedelta_class = get_timedelta_class(py).map_err(py_err)?;
        let td = timedelta_class.call1((0,)).map_err(py_err)?;
        Ok(PyValue(td.into_py_any(py).map_err(py_err)?))
    }

    fn from_time8(v: &Time8) -> Result<Self> {
        let py = py();
        let timedelta_class = get_timedelta_class(py).map_err(py_err)?;
        let td = timedelta_class
            .call1((
                v.days(),
                v.second as i32,
                0,
                0,
                v.minute as i32,
                v.hour as i32,
            ))
            .map_err(py_err)?;
        let td = if v.is_negative() {
            td.call_method0("__neg__").map_err(py_err)?
        } else {
            td
        };
        Ok(PyValue(td.into_py_any(py).map_err(py_err)?))
    }

    fn from_time12(v: &Time12) -> Result<Self> {
        let py = py();
        let timedelta_class = get_timedelta_class(py).map_err(py_err)?;
        let td = timedelta_class
            .call1((
                v.days(),
                v.second as i32,
                v.microsecond(),
                0,
                v.minute as i32,
                v.hour as i32,
            ))
            .map_err(py_err)?;
        let td = if v.is_negative() {
            td.call_method0("__neg__").map_err(py_err)?
        } else {
            td
        };
        Ok(PyValue(td.into_py_any(py).map_err(py_err)?))
    }
}
