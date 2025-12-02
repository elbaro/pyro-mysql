pub mod conn;
pub mod handler;
pub mod transaction;

use pyo3::prelude::*;

use crate::{r#async::conn::AsyncConn, opts::Opts, util::PyroFuture};

#[pyfunction]
#[pyo3(signature = (url_or_opts))]
pub fn connect(
    py: Python,
    url_or_opts: either::Either<String, PyRef<Opts>>,
) -> PyResult<Py<PyroFuture>> {
    AsyncConn::new(py, url_or_opts)
}
