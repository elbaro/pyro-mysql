use diesel::{
    data_types::MysqlTime,
    deserialize::FromSqlRow,
    mysql::{Mysql, MysqlValue},
    row::Field,
    sql_types::Untyped,
};
use pyo3::{
    Bound, IntoPyObjectExt, Py, PyResult, Python,
    types::{PyAny, PyAnyMethods, PyBytes, PyDict, PyDictMethods, PyString, PyTuple},
};

use crate::py_imports::{
    get_date_class, get_datetime_class, get_decimal_class, get_timedelta_class,
};
use crate::row::{RowDict, RowTuple};

/// Convert diesel's MysqlValue to a Python object
/// bytes(&[u8]) point to the data provided by libmysqlclient, which can be different from the packet data
fn diesel_value_to_python<'py>(
    py: Python<'py>,
    value: Option<MysqlValue<'_>>,
) -> pyo3::PyResult<Bound<'py, PyAny>> {
    use diesel::mysql::MysqlType;

    let Some(value) = value else {
        return Ok(py.None().into_bound(py));
    };

    let bytes = value.as_bytes();
    let mysql_type = value.value_type();

    match mysql_type {
        // TINYINT - 1 byte
        MysqlType::Tiny => {
            let value = i8::from_ne_bytes(bytes.try_into()?);
            value.into_bound_py_any(py)
        }

        MysqlType::UnsignedTiny => {
            let value = u8::from_ne_bytes(bytes.try_into()?);
            value.into_bound_py_any(py)
        }

        // SMALLINT - 2 bytes
        MysqlType::Short => {
            let value = i16::from_ne_bytes(bytes.try_into()?);
            value.into_bound_py_any(py)
        }

        MysqlType::UnsignedShort => {
            let value = u16::from_ne_bytes(bytes.try_into()?);
            value.into_bound_py_any(py)
        }

        // INT - 4 bytes
        MysqlType::Long => {
            let value = i32::from_ne_bytes(bytes.try_into()?);
            value.into_bound_py_any(py)
        }

        MysqlType::UnsignedLong => {
            let value = u32::from_ne_bytes(bytes.try_into()?);
            value.into_bound_py_any(py)
        }

        // BIGINT - 8 bytes
        MysqlType::LongLong => {
            let value = i64::from_ne_bytes(bytes.try_into()?);
            value.into_bound_py_any(py)
        }

        MysqlType::UnsignedLongLong => {
            let value = u64::from_ne_bytes(bytes.try_into()?);
            value.into_bound_py_any(py)
        }

        // FLOAT - 4 bytes
        MysqlType::Float => {
            let value = f32::from_ne_bytes(bytes.try_into()?);
            let mut buffer = ryu::Buffer::new();
            buffer
                .format(value)
                .parse::<f64>()
                .unwrap() // unwrap(): f32 -> str -> f64 never fails
                .into_bound_py_any(py)
        }

        // DOUBLE - 8 bytes
        MysqlType::Double => {
            let value = f64::from_ne_bytes(bytes.try_into()?);
            value.into_bound_py_any(py)
        }

        // Decimal/Numeric type - still uses string representation
        MysqlType::Numeric => match std::str::from_utf8(bytes) {
            Ok(s) => get_decimal_class(py)?.call1((s,)),
            Err(_) => Ok(PyBytes::new(py, bytes).into_any()),
        },

        // https://docs.rs/mysql_common/latest/src/mysql_common/value/mod.rs.html
        // https://docs.rs/diesel/latest/diesel/mysql/data_types/struct.MysqlTime.html
        MysqlType::DateTime | MysqlType::Date | MysqlType::Time | MysqlType::Timestamp => {
            if bytes.len() < std::mem::size_of::<mysqlclient_sys::MYSQL_TIME>() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Invalid MYSQL_TIME data: buffer too small",
                ));
            }

            // We should not access time_zone_displacement, which only exists in mysql and not in mariadb
            // https://github.com/diesel-rs/diesel/issues/2373#issuecomment-618408680
            let c_struct: &MysqlTime =
                unsafe { &*(bytes.as_ptr() as *const diesel::data_types::MysqlTime) };
            match mysql_type {
                MysqlType::Date => {
                    get_date_class(py)?.call1((c_struct.year as i32, c_struct.month, c_struct.day))
                }
                MysqlType::DateTime | MysqlType::Timestamp => get_datetime_class(py)?.call1((
                    c_struct.year as i32,
                    c_struct.month,
                    c_struct.day,
                    c_struct.hour,
                    c_struct.minute,
                    c_struct.second,
                    c_struct.second_part as u32,
                )),
                MysqlType::Time => {
                    // MySQL TIME can represent intervals with days
                    // Convert to timedelta: negative flag, days, hours, minutes, seconds, microseconds
                    let total_seconds =
                        c_struct.hour * 3600 + c_struct.minute * 60 + c_struct.second;
                    let days = c_struct.day + (total_seconds / 86400);
                    let remaining_seconds = total_seconds % 86400;

                    let timedelta = get_timedelta_class(py)?.call1((
                        days as i32,
                        remaining_seconds as i32,
                        c_struct.second_part as i32,
                    ))?;

                    if c_struct.neg {
                        timedelta.call_method0("__neg__")
                    } else {
                        Ok(timedelta)
                    }
                }
                _ => unreachable!(),
            }
        }
        // String types (String, Enum, Set)
        MysqlType::String | MysqlType::Enum | MysqlType::Set => match std::str::from_utf8(bytes) {
            Ok(s) => Ok(PyString::new(py, s).into_any()),
            Err(_) => Ok(PyBytes::new(py, bytes).into_any()),
        },

        // Binary types (Blob, Bit)
        MysqlType::Blob | MysqlType::Bit => Ok(PyBytes::new(py, bytes).into_any()),

        // Default: handle any unknown types by trying string then bytes
        _ => match std::str::from_utf8(bytes) {
            Ok(s) => Ok(PyString::new(py, s).into_any()),
            Err(_) => Ok(PyBytes::new(py, bytes).into_any()),
        },
    }
}

impl FromSqlRow<Untyped, Mysql> for RowTuple {
    fn build_from_row<'a>(
        row: &impl diesel::row::Row<'a, Mysql>,
    ) -> diesel::deserialize::Result<Self> {
        let tuple = Python::attach(|py| -> PyResult<Py<PyTuple>> {
            let mut vec = vec![];
            for i in 0..row.field_count() {
                let field = row.get(i).ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Unexpected end of row")
                })?;
                let value = field.value();
                let py_value = diesel_value_to_python(py, value)?;
                vec.push(py_value);
            }
            Ok(PyTuple::new(py, vec)?.unbind())
        })?;

        Ok(Self(tuple))
    }
}

impl FromSqlRow<Untyped, Mysql> for RowDict {
    fn build_from_row<'a>(
        row: &impl diesel::row::Row<'a, Mysql>,
    ) -> diesel::deserialize::Result<Self> {
        let dic = Python::attach(|py| -> PyResult<Py<PyDict>> {
            let dic = PyDict::new(py);
            for i in 0..row.field_count() {
                let field = row.get(i).ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Unexpected end of row")
                })?;
                let value = field.value();
                let py_value = diesel_value_to_python(py, value)?;

                let field_name = field
                    .field_name()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| i.to_string());

                dic.set_item(PyString::new(py, &field_name), py_value)?;
            }

            Ok(dic.unbind())
        })?;

        Ok(Self(dic))
    }
}
