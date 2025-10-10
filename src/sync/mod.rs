pub mod conn;
pub mod iterator;
pub mod opts;
pub mod pool;
pub mod pool_opts;
pub mod pooled_conn;
pub mod transaction;

pub use conn::SyncConn;
use either::Either;
pub use pool::SyncPool;
pub use pool_opts::SyncPoolOpts;
pub use pooled_conn::SyncPooledConn;
pub use transaction::SyncTransaction;

use pyo3::prelude::*;

use crate::{error::PyroResult, sync::opts::SyncOpts};

#[pyfunction]
pub fn connect(url_or_opts: Either<String, PyRef<SyncOpts>>) -> PyroResult<SyncConn> {
    SyncConn::new(url_or_opts)
}
