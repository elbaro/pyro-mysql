use crate::{
    error::PyroResult,
    sync::{opts::SyncOpts, pooled_conn::SyncPooledConn},
};
use either::Either;
use mysql::{Opts, Pool};
use parking_lot::RwLock;
use pyo3::prelude::*;

#[pyclass(module = "pyro_mysql.sync", name = "Pool")]
pub struct SyncPool {
    pool: Pool, // This is clonable
}

#[pymethods]
impl SyncPool {
    /// new() won't assert server availability.
    /// Can accept either a URL string or SyncOpts object
    #[new]
    pub fn new(url_or_opts: Either<String, PyRef<SyncOpts>>) -> PyroResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url)?,
            Either::Right(opts) => opts.opts.clone(),
        };

        let pool = Pool::new(opts)?;
        Ok(Self { pool })
    }

    fn get(&self) -> PyroResult<SyncPooledConn> {
        let conn = self.pool.get_conn()?;
        Ok(SyncPooledConn {
            inner: RwLock::new(Some(conn)),
        })
    }
}
