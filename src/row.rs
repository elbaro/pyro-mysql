use crate::value::value_to_python;
use color_eyre::Result;
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};

#[pyclass]
pub struct Row {
    pub inner: mysql_common::Row,
}

impl mysql_common::prelude::FromRow for Row {
    fn from_row_opt(row: mysql_common::Row) -> Result<Self, mysql_common::FromRowError>
    where
        Self: Sized,
    {
        Ok(Self { inner: row })
    }
}

#[pymethods]
impl Row {
    pub fn to_tuple<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyTuple>> {
        let n = self.inner.len();
        let columns = self.inner.columns_ref();
        let mut vec = Vec::with_capacity(n);
        for i in 0..n {
            vec.push(value_to_python(py, &self.inner[i], &columns[i])?);
        }
        Ok(PyTuple::new(py, vec)?)
    }

    pub fn to_dict<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyDict>> {
        let n = self.inner.len();
        let columns = self.inner.columns_ref();
        let dict = PyDict::new(py);
        for i in 0..n {
            dict.set_item(
                columns[i].name_str(),
                value_to_python(py, &self.inner[i], &columns[i])?,
            )?;
        }
        Ok(dict)
    }
}
