pub mod backend;
pub mod conn;
pub mod multi_conn;
pub mod opts;
pub mod pool;
pub mod pool_opts;
pub mod queryable;
pub mod row;
pub mod transaction;

use pyo3::prelude::*;

use crate::{r#async::conn::AsyncConn, util::PyroFuture};

#[pyfunction]
#[pyo3(signature = (url_or_opts, backend="mysql_async"))]
pub fn connect(
    py: Python,
    url_or_opts: either::Either<String, PyRef<opts::AsyncOpts>>,
    backend: &str,
) -> PyResult<Py<PyroFuture>> {
    AsyncConn::new(py, url_or_opts, backend)
}
