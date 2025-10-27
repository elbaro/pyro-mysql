use pyo3::prelude::*;
use std::time::Duration;

use crate::error::{Error, PyroResult};

#[pyclass(module = "pyro_mysql.async_", name = "PoolOpts")]
#[derive(Clone, Debug)]
pub struct AsyncPoolOpts {
    pub(crate) inner: mysql_async::PoolOpts,
}

#[pymethods]
impl AsyncPoolOpts {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: mysql_async::PoolOpts::default(),
        }
    }

    pub fn with_constraints(&self, constraints: (usize, usize)) -> PyroResult<Self> {
        let (min, max) = constraints;
        match mysql_async::PoolConstraints::new(min, max) {
            Some(pool_constraints) => Ok(Self {
                inner: self.inner.clone().with_constraints(pool_constraints),
            }),
            None => Err(Error::IncorrectApiUsageError(
                "Invalid pool constraints: min must be <= max",
            )),
        }
    }

    #[pyo3(signature = (ttl,))]
    pub fn with_inactive_connection_ttl(&self, ttl: &Bound<'_, PyAny>) -> PyResult<Self> {
        use pyo3::types::PyDelta;

        let duration = if let Ok(delta) = ttl.cast::<PyDelta>() {
            // without abi3: delta.get_seconds() as f64 + delta.get_microseconds() as f64 / 1_000_000.0;
            let seconds = delta.getattr("seconds")?.extract::<u64>()?;
            let microseconds = delta.getattr("microseconds")?.extract::<u64>()?;
            Duration::from_micros(seconds * 1_000_000 + microseconds)
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Expected timedelta object",
            ));
        };

        Ok(Self {
            inner: self.inner.clone().with_inactive_connection_ttl(duration),
        })
    }

    #[pyo3(signature = (interval,))]
    pub fn with_ttl_check_interval(&self, interval: &Bound<'_, PyAny>) -> PyResult<Self> {
        use pyo3::types::PyDelta;

        let duration = if let Ok(delta) = interval.cast::<PyDelta>() {
            let seconds = delta.getattr("seconds")?.extract::<u64>()?;
            let microseconds = delta.getattr("microseconds")?.extract::<u64>()?;
            Duration::from_micros(seconds * 1_000_000 + microseconds)
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Expected timedelta object",
            ));
        };

        Ok(Self {
            inner: self.inner.clone().with_ttl_check_interval(duration),
        })
    }
}

impl Default for AsyncPoolOpts {
    fn default() -> Self {
        Self::new()
    }
}
