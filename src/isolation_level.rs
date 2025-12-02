use pyo3::prelude::*;

#[pyclass(frozen, module = "pyro_mysql")]
#[derive(Clone)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl IsolationLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

#[pymethods]
impl IsolationLevel {
    #[getter]
    fn name(&self) -> &'static str {
        self.as_str()
    }
}
