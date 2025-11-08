use pyo3::prelude::*;
use std::collections::HashMap;

use crate::value::Value;

/// Parameter type for SQL queries
/// Supports positional (tuple/list) and named (dict) parameters
#[derive(Debug)]
pub enum Params {
    /// No parameters
    Empty,

    /// Positional parameters (from tuple or list)
    /// Uses ? placeholders in SQL
    Positional(Vec<Value>),

    /// Named parameters (from dict)
    /// Uses :name placeholders in SQL
    Named(HashMap<String, Value>),
}

impl Default for Params {
    fn default() -> Self {
        Self::Empty
    }
}

impl Params {
    /// Convert to mysql_common::params::Params for mysql backend
    pub fn to_mysql_params(self) -> mysql_common::params::Params {
        match self {
            Params::Empty => mysql_common::params::Params::Empty,
            Params::Positional(values) => {
                let mysql_values: Vec<mysql_async::Value> = values
                    .into_iter()
                    .map(|v| v.to_mysql_value())
                    .collect();
                mysql_common::params::Params::Positional(mysql_values)
            }
            Params::Named(map) => {
                let mysql_map: HashMap<Vec<u8>, mysql_async::Value> = map
                    .into_iter()
                    .map(|(k, v)| (k.into_bytes(), v.to_mysql_value()))
                    .collect();
                mysql_common::params::Params::Named(mysql_map)
            }
        }
    }

    /// Check if parameters are empty
    pub fn is_empty(&self) -> bool {
        matches!(self, Params::Empty)
    }

    /// Get the number of parameters
    pub fn len(&self) -> usize {
        match self {
            Params::Empty => 0,
            Params::Positional(v) => v.len(),
            Params::Named(m) => m.len(),
        }
    }
}

impl FromPyObject<'_, '_> for Params {
    type Error = PyErr;

    fn extract(ob: Borrowed<PyAny>) -> Result<Self, Self::Error> {
        // Get the fully qualified type name and match against it
        let py_type = ob.get_type();
        let type_name = py_type.fully_qualified_name()?;

        if type_name == "NoneType" {
            Ok(Params::Empty)
        } else if type_name == "tuple" {
            let tuple = ob.cast::<pyo3::types::PyTuple>()?;
            let mut params = Vec::with_capacity(tuple.len());
            for item in tuple.iter() {
                params.push(Value::extract(item.as_borrowed())?);
            }
            Ok(Params::Positional(params))
        } else if type_name == "list" {
            let list = ob.cast::<pyo3::types::PyList>()?;
            let mut params = Vec::with_capacity(list.len());
            for item in list.iter() {
                params.push(Value::extract(item.as_borrowed())?);
            }
            Ok(Params::Positional(params))
        } else if type_name == "dict" {
            let dict = ob.cast::<pyo3::types::PyDict>()?;
            let mut params = HashMap::new();
            for (key, value) in dict.iter() {
                let key_str = key.extract::<String>()?;
                let param_value = Value::extract(value.as_borrowed())?;
                params.insert(key_str, param_value);
            }
            Ok(Params::Named(params))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Expected None, tuple, list, or dict for Params, got '{}'",
                type_name
            )))
        }
    }
}

impl From<Params> for mysql_common::params::Params {
    fn from(params: Params) -> Self {
        params.to_mysql_params()
    }
}
