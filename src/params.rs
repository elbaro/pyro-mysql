use mysql_common::params::Params as MySqlParams;
use pyo3::prelude::*;

use crate::value::Value;

#[derive(Debug)]
pub struct Params {
    pub inner: MySqlParams,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            inner: MySqlParams::Empty,
        }
    }
}

impl Params {
    pub fn into_inner(self) -> MySqlParams {
        self.inner
    }
}

impl FromPyObject<'_, '_> for Params {
    type Error = PyErr;

    fn extract(ob: Borrowed<PyAny>) -> Result<Self, Self::Error> {
        // Get the fully qualified type name and match against it
        let py_type = ob.get_type();
        let type_name = py_type.fully_qualified_name()?;

        if type_name == "NoneType" {
            Ok(Params {
                inner: MySqlParams::Empty,
            })
        } else if type_name == "tuple" {
            let tuple = ob.cast::<pyo3::types::PyTuple>()?;
            let mut params = Vec::<mysql_async::Value>::with_capacity(tuple.len());
            for item in tuple.iter() {
                params.push(Value::extract(item.as_borrowed())?.into());
            }
            Ok(Params {
                inner: MySqlParams::Positional(params),
            })
        } else if type_name == "list" {
            let list = ob.cast::<pyo3::types::PyList>()?;
            let mut params = Vec::with_capacity(list.len());
            for item in list.iter() {
                params.push(Value::extract(item.as_borrowed())?.into());
            }
            Ok(Params {
                inner: MySqlParams::Positional(params),
            })
        } else if type_name == "dict" {
            let dict = ob.cast::<pyo3::types::PyDict>()?;
            let mut params = std::collections::HashMap::new();
            for (key, value) in dict.iter() {
                let key_str = key.extract::<String>()?;
                let param_value = Value::extract(value.as_borrowed())?.into();
                // Convert String key to Vec<u8> as required by mysql_async
                params.insert(key_str.into_bytes(), param_value);
            }
            Ok(Params {
                inner: MySqlParams::Named(params),
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Expected None, tuple, list, or dict for Params, got '{}'",
                type_name
            )))
        }
    }
}

impl From<Params> for MySqlParams {
    fn from(params: Params) -> Self {
        params.inner
    }
}
