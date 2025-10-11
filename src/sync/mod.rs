pub mod conn;
pub mod cursor;
pub mod dbapi_conn;
pub mod iterator;
pub mod opts;
pub mod pool;
pub mod pool_opts;
pub mod pooled_conn;
pub mod transaction;
pub mod type_constructor;
pub mod type_object;

pub use dbapi_conn::SyncDbApiConn;
use either::Either;
pub use pool::SyncPool;
pub use pool_opts::SyncPoolOpts;
pub use pooled_conn::SyncPooledConn;
pub use transaction::SyncTransaction;

use pyo3::prelude::*;

use crate::{error::PyroResult, sync::opts::SyncOpts};

#[pyfunction]
#[pyo3(signature = (url_or_opts, autocommit=Some(false)))]
pub fn connect(
    url_or_opts: Either<String, PyRef<SyncOpts>>,
    autocommit: Option<bool>,
) -> PyroResult<SyncDbApiConn> {
    let conn = SyncDbApiConn::new(url_or_opts)?;
    if let Some(on) = autocommit {
        conn.set_autocommit(on)?;
    }
    Ok(conn)
}
