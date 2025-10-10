pub mod conn;
pub mod dbapi_conn;
pub mod opts;
pub mod pool;
pub mod pool_opts;
pub mod queryable;
pub mod transaction;

use pyo3::prelude::*;

use crate::{r#async::conn::AsyncConn, util::PyroFuture};

#[pyfunction]
pub fn connect(
    py: Python,
    url_or_opts: either::Either<String, PyRef<opts::AsyncOpts>>,
) -> PyResult<Py<PyroFuture>> {
    AsyncConn::new(py, url_or_opts)
}
