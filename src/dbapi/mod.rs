pub mod conn;
pub mod cursor;
pub mod type_constructor;
pub mod type_object;

use crate::{dbapi::conn::DbApiConn, error::PyroResult, sync::opts::SyncOpts};
use either::Either;

use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (url_or_opts, autocommit=Some(false)))]
pub fn connect(
    url_or_opts: Either<String, PyRef<SyncOpts>>,
    autocommit: Option<bool>,
) -> PyroResult<DbApiConn> {
    let conn = DbApiConn::new(url_or_opts)?;
    if let Some(on) = autocommit {
        conn.set_autocommit(on)?;
    }
    Ok(conn)
}
