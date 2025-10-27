use pyo3::{prelude::*, types::{PyDict, PyTuple}};

#[pyclass(module = "pyro_mysql.async_")]
pub struct Row {
    // Store decoded Python objects directly - zero-copy from wtx
    values: Vec<Py<PyAny>>,
    column_names: Vec<String>,
}

impl Row {
    pub fn new(values: Vec<Py<PyAny>>, column_names: Vec<String>) -> Self {
        Self { values, column_names }
    }
}

#[pymethods]
impl Row {
    pub fn to_tuple<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let vec: Vec<_> = self.values.iter()
            .map(|obj| obj.clone_ref(py))
            .collect();
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
