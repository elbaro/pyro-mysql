use crate::{sync::opts::SyncOpts, sync::pooled_conn::SyncPooledConn};
use either::Either;
use mysql::{Opts, Pool};
use pyo3::prelude::*;

#[pyclass]
pub struct SyncPool {
    pool: Pool, // This is clonable
}

#[pymethods]
impl SyncPool {
    /// new() won't assert server availability.
    /// Can accept either a URL string or SyncOpts object
    #[new]
    pub fn new(url_or_opts: Either<String, PyRef<SyncOpts>>) -> PyResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?,
            Either::Right(opts) => opts.opts.clone(),
        };

        let pool = Pool::new(opts)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(Self { pool })
    }

    fn get_conn(&self) -> PyResult<SyncPooledConn> {
        let conn = self
            .pool
            .get_conn()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(SyncPooledConn {
            inner: Some(conn),
        })
    }

    fn acquire(&self) -> PyResult<SyncPooledConn> {
        self.get_conn()
    }
}
