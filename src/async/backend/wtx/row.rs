use pyo3::{
    prelude::*,
    types::{PyBytes, PyFloat, PyInt, PyString},
};

/// Convert wtx MysqlRecord to async Row with Python objects
pub fn wtx_record_to_row(
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
