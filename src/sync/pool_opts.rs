use pyo3::prelude::*;

#[pyclass]
#[derive(Clone, Debug)]
pub struct SyncPoolOpts {
    pub(crate) inner: mysql::PoolOpts,
}

#[pymethods]
impl SyncPoolOpts {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: mysql::PoolOpts::default(),
        }
    }

    pub fn with_constraints(&self, constraints: (usize, usize)) -> PyResult<Self> {
        let (min, max) = constraints;
        match mysql::PoolConstraints::new(min, max) {
            Some(pool_constraints) => Ok(Self {
                inner: self.inner.clone().with_constraints(pool_constraints),
            }),
            None => Err(pyo3::exceptions::PyValueError::new_err(
                "Invalid pool constraints: min must be <= max",
            )),
        }
    }
}

impl Default for SyncPoolOpts {
    fn default() -> Self {
        Self::new()
    }
}
