pub mod type_constructor;
pub mod type_object;

use crate::{
    error::PyroResult,
    sync::{dbapi_conn::SyncDbApiConn, opts::SyncOpts},
};
use either::Either;

use pyo3::prelude::*;

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
