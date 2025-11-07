pub mod async_conn;
pub mod async_cursor;
pub mod conn;
pub mod cursor;
pub mod error;
pub mod type_constructor;
pub mod type_object;

use std::sync::Arc;

use crate::{
    r#async::multi_conn::MultiAsyncConn,
    r#async::opts::AsyncOpts,
    dbapi::{async_conn::AsyncDbApiConn, conn::DbApiConn, error::DbApiResult},
    error::Error,
    params::Params,
    sync::opts::SyncOpts,
    util::{PyroFuture, rust_future_into_py, url_error_to_pyerr},
};
use either::Either;

use mysql_async::prelude::Queryable;
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (url_or_opts, autocommit=Some(false)))]
pub fn connect(
    url_or_opts: Either<String, PyRef<SyncOpts>>,
    autocommit: Option<bool>,
) -> DbApiResult<DbApiConn> {
    let conn = DbApiConn::new(url_or_opts)?;
    if let Some(on) = autocommit {
        conn.set_autocommit(on)?;
    }
    Ok(conn)
}

#[pyfunction()]
#[pyo3(name = "connect", signature = (url_or_opts, autocommit=Some(false), wtx=false))]
pub fn connect_async(
    py: Python,
    url_or_opts: Either<String, PyRef<AsyncOpts>>,
    autocommit: Option<bool>,
    wtx: bool,
) -> DbApiResult<Py<PyroFuture>> {
    if wtx {
        // Use wtx backend
        let url = match url_or_opts {
            Either::Left(url) => url,
            Either::Right(_) => {
                return Err(error::InterfaceError::new_err(
                    "AsyncOpts is not supported for wtx connections, use URL string instead",
                )
                .into());
            }
        };

        Ok(rust_future_into_py(py, async move {
            let mut multi_conn = MultiAsyncConn::new_wtx(&url, None, None).await?;

            // Set autocommit if specified
            if let Some(on) = autocommit {
                use wtx::database::Executor;
                let query = if on {
                    "SET autocommit=1"
                } else {
                    "SET autocommit=0"
                };

                match &mut multi_conn {
                    MultiAsyncConn::Wtx(wtx_conn) => {
                        wtx_conn.executor
                            .execute(query, |_| Ok(()))
                            .await
                            .map_err(|e: wtx::Error| Error::WtxError(e.to_string()))?;
                    }
                    _ => unreachable!(),
                }
            }

            Ok(AsyncDbApiConn(Arc::new(tokio::sync::RwLock::new(Some(
                multi_conn,
            )))))
        })?)
    } else {
        // Use mysql_async backend
        let opts = match url_or_opts {
            Either::Left(url) => mysql_async::Opts::from_url(&url).map_err(url_error_to_pyerr)?,
            Either::Right(opts) => opts.opts.clone(),
        };
        Ok(rust_future_into_py(py, async move {
            let mut conn = mysql_async::Conn::new(opts).await?;
            if let Some(on) = autocommit {
                let query = if on {
                    "SET autocommit=1"
                } else {
                    "SET autocommit=0"
                };
                conn.exec_drop(query, Params::default())
                    .await
                    .map_err(Error::from)?;
            }
            Ok(AsyncDbApiConn(Arc::new(tokio::sync::RwLock::new(Some(
                MultiAsyncConn::MysqlAsync(conn),
            )))))
        })?)
    }
}
