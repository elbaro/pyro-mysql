// PEP 249 – Python Database API Specification v2.0

use std::{borrow::Cow, sync::Arc};

use crate::{
    r#async::opts::AsyncOpts,
    error::{Error, PyroResult},
    params::Params,
    util::{PyroFuture, rust_future_into_py},
};
use either::Either;
use mysql_async::{Opts, prelude::Queryable};
use pyo3::prelude::*;
use tokio::sync::RwLock;

#[pyclass]
pub struct AsyncDbApiConn(Arc<RwLock<Option<mysql_async::Conn>>>);

impl AsyncDbApiConn {
    pub async fn new(url_or_opts: Either<String, AsyncOpts>) -> PyroResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url)?,
            Either::Right(opts) => opts.opts.clone(),
        };
        let conn = mysql_async::Conn::new(opts).await?;
        Ok(Self(Arc::new(RwLock::new(Some(conn)))))
    }

    fn exec_drop(
        &self,
        py: Python,
        query: impl Into<Cow<'static, str>>,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.0.clone();
        let query = query.into();
        rust_future_into_py::<_, ()>(py, async move {
            let mut inner = inner.write().await;
            Ok(inner
                .as_mut()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .exec_drop(&query, params)
                .await?)
        })
    }
}

#[pymethods]
impl AsyncDbApiConn {
    // ─── Pep249 ──────────────────────────────────────────────────────────

    fn close(&self, py: Python) -> PyResult<Py<PyroFuture>> {
        let inner = self.0.clone();
        rust_future_into_py(py, async move {
            let mut inner = inner.write().await;
            *inner = None;
            Ok(())
        })
    }

    fn commit(&self, py: Python) -> PyResult<Py<PyroFuture>> {
        self.exec_drop(py, "COMMIT", Params::default())
    }

    fn rollback(&self, py: Python) -> PyResult<Py<PyroFuture>> {
        self.exec_drop(py, "ROLLBACK", Params::default())
    }

    /// Cursor instances hold a reference to the python connection object.
    fn cursor(&self) {
        todo!()
    }

    // ─── Helper ──────────────────────────────────────────────────────────

    pub fn set_autocommit(&self, py: Python, on: bool) -> PyResult<Py<PyroFuture>> {
        if on {
            self.exec_drop(py, "SET autocommit=1", Params::default())
        } else {
            self.exec_drop(py, "SET autocommit=0", Params::default())
        }
    }
}
