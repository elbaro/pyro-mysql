pub mod async_conn;
pub mod async_cursor;
pub mod async_zero_handler;
pub mod conn;
pub mod cursor;
pub mod error;
pub mod type_constructor;
pub mod type_object;
pub mod zero_handler;

use std::sync::Arc;

use crate::{
    r#async::backend::ZeroMysqlConn,
    dbapi::{async_conn::AsyncDbApiConn, conn::DbApiConn, error::DbApiResult},
    error::Error,
    opts::Opts,
    util::{PyroFuture, rust_future_into_py},
};
use either::Either;

use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (url_or_opts, autocommit=Some(false)))]
pub fn connect(
    url_or_opts: Either<String, PyRef<Opts>>,
    autocommit: Option<bool>,
) -> DbApiResult<DbApiConn> {
    let conn = DbApiConn::new(url_or_opts)?;
    if let Some(on) = autocommit {
        conn.set_autocommit(on)?;
    }
    Ok(conn)
}

#[pyfunction()]
#[pyo3(name = "connect", signature = (url_or_opts, autocommit=Some(false)))]
pub fn connect_async(
    py: Python,
    url_or_opts: Either<String, PyRef<Opts>>,
    autocommit: Option<bool>,
) -> DbApiResult<Py<PyroFuture>> {
    // Use zero_mysql backend
    let opts = match url_or_opts {
        Either::Left(url) => {
            let inner: zero_mysql::Opts = url.as_str().try_into().map_err(Error::ZeroMysqlError)?;
            inner
        }
        Either::Right(opts) => opts.inner.clone(),
    };
    Ok(rust_future_into_py(py, async move {
        let mut conn = ZeroMysqlConn::new_with_opts(opts).await?;
        if let Some(on) = autocommit {
            let query = if on {
                "SET autocommit=1"
            } else {
                "SET autocommit=0"
            };
            conn.query_drop(query.to_string()).await?;
        }
        Ok(AsyncDbApiConn(Arc::new(tokio::sync::RwLock::new(Some(
            conn,
        )))))
    })?)
}
