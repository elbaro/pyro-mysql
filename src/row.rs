use crate::value::value_to_python;
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};

#[pyclass(module = "pyro_mysql")]
pub struct Row {
    pub inner: RowInner,
}

pub enum RowInner {
    MysqlCommon(mysql_common::Row),
    Wtx {
        // Store decoded Python objects directly
        values: Vec<PyObject>,
        column_names: Vec<String>,
    },
}

impl mysql_common::prelude::FromRow for Row {
    fn from_row_opt(row: mysql_common::Row) -> Result<Self, mysql_common::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self { inner: RowInner::MysqlCommon(row) })
    }
}

#[pymethods]
impl Row {
    pub fn to_tuple<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        match &self.inner {
            RowInner::MysqlCommon(row) => {
                let columns = row.columns_ref();
                let mut vec = Vec::with_capacity(row.len());
                for (i, column) in columns.iter().enumerate() {
                    vec.push(value_to_python(py, &row[i], column)?);
                }
                PyTuple::new(py, vec)
            }
            RowInner::Wtx { values, .. } => {
                // Values are already Python objects
                let vec: Vec<_> = values.iter()
                    .map(|obj| obj.clone_ref(py))
                    .collect();
                PyTuple::new(py, vec)
            }
        }
    }

    pub fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        match &self.inner {
            RowInner::MysqlCommon(row) => {
                let columns = row.columns_ref();
                let dict = PyDict::new(py);
                for (i, column) in columns.iter().enumerate() {
                    dict.set_item(
                        columns[i].name_str(),
                        value_to_python(py, &row[i], column)?,
                    )?;
                }
                Ok(dict)
            }
            RowInner::Wtx { values, column_names } => {
                let dict = PyDict::new(py);
                for (name, obj) in column_names.iter().zip(values.iter()) {
                    dict.set_item(name, obj.clone_ref(py))?;
                }
                Ok(dict)
            }
        }
    }
}
