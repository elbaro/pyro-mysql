use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};

/// Async Row type that stores Python objects directly
/// Supports both wtx (stores Python objects) and mysql_async (converts from mysql_common::Row)
#[pyclass(module = "pyro_mysql.async_")]
pub struct Row {
    // Store decoded Python objects directly
    values: Vec<Py<PyAny>>,
    column_names: Vec<String>,
}

impl Row {
    /// Create a new Row from Python objects and column names (for wtx)
    pub fn new(values: Vec<Py<PyAny>>, column_names: Vec<String>) -> Self {
        Self {
            values,
            column_names,
        }
    }
}

/// Support conversion from mysql_common::Row (for mysql_async backend)
impl mysql_common::prelude::FromRow for Row {
    fn from_row(row: mysql_common::Row) -> Self
    where
        Self: Sized,
    {
        Self::try_from_row(row).unwrap()
    }

    fn from_row_opt(row: mysql_common::Row) -> Result<Self, mysql_common::FromRowError>
    where
        Self: Sized,
    {
        Self::try_from_row(row)
    }
}

impl Row {
    fn try_from_row(row: mysql_common::Row) -> Result<Self, mysql_common::FromRowError> {
        // Convert mysql_common::Row to our async Row format
        Python::attach(|py| {
            let columns = row.columns_ref();
            let mut values = Vec::with_capacity(row.len());
            let mut column_names = Vec::with_capacity(columns.len());

            for (i, column) in columns.iter().enumerate() {
                column_names.push(column.name_str().to_string());
                // Convert mysql value to Python object
                let py_obj = crate::value::value_to_python(py, &row[i], column)
                    .map_err(|_| mysql_common::FromRowError(row.clone()))?;
                values.push(py_obj.unbind());
            }

            Ok(Self {
                values,
                column_names,
            })
        })
    }
}

#[pymethods]
impl Row {
    pub fn to_tuple<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let vec: Vec<_> = self.values.iter().map(|obj| obj.clone_ref(py)).collect();
        PyTuple::new(py, vec)
    }

    pub fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, obj) in self.column_names.iter().zip(self.values.iter()) {
            dict.set_item(name, obj.clone_ref(py))?;
        }
        Ok(dict)
    }
}
