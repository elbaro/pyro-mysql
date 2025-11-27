use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use either::Either;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use tokio::sync::RwLock;

use crate::r#async::multi_conn::MultiAsyncConn;
use crate::r#async::queryable::Queryable;
use crate::r#async::transaction::AsyncTransaction;
use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::opts::Opts;
use crate::util::{PyroFuture, rust_future_into_py, url_error_to_pyerr};

#[pyclass(module = "pyro_mysql.async_", name = "Conn")]
pub struct AsyncConn {
    pub inner: Arc<RwLock<Option<MultiAsyncConn>>>,
    pub in_transaction: AtomicBool,
}

#[pymethods]
impl AsyncConn {
    // ─── Connection Management ───────────────────────────────────────────
    #[new]
    fn _new() -> PyroResult<Self> {
        Err(Error::IncorrectApiUsageError(
            "use `await Conn.new(url) instead of Conn()`.",
        ))
    }

    #[allow(clippy::new_ret_no_self)]
    #[staticmethod]
    #[pyo3(signature = (url_or_opts, backend="mysql"))]
    pub fn new<'py>(
        py: Python<'py>,
        url_or_opts: Either<String, PyRef<Opts>>,
        backend: &str,
    ) -> PyResult<Py<PyroFuture>> {
        match backend {
            "mysql" => {
                let opts = match url_or_opts {
                    Either::Left(url) => {
                        mysql_async::Opts::from_url(&url).map_err(url_error_to_pyerr)?
                    }
                    Either::Right(opts) => opts.to_mysql_async_opts(),
                };
                rust_future_into_py(py, async move {
                    let conn = mysql_async::Conn::new(opts).await?;
                    Ok(Self {
                        inner: Arc::new(RwLock::new(Some(MultiAsyncConn::MysqlAsync(conn)))),
                        in_transaction: AtomicBool::new(false),
                    })
                })
            }
            "wtx" => {
                let url = match url_or_opts {
                    Either::Left(url) => url,
                    Either::Right(_opts) => {
                        return Err(Error::IncorrectApiUsageError(
                            "WTX backend currently only supports URL strings",
                        )
                        .into());
                    }
                };
                rust_future_into_py(py, async move {
                    let multi_conn = MultiAsyncConn::new_wtx(&url, None, None).await?;
                    Ok(Self {
                        inner: Arc::new(RwLock::new(Some(multi_conn))),
                        in_transaction: AtomicBool::new(false),
                    })
                })
            }
            "zero" => {
                let opts = match url_or_opts {
                    Either::Left(url) => {
                        let inner: zero_mysql::Opts =
                            url.as_str().try_into().map_err(Error::from)?;
                        inner
                    }
                    Either::Right(opts) => opts.inner.clone(),
                };
                rust_future_into_py(py, async move {
                    let multi_conn = MultiAsyncConn::new_zero_mysql_with_opts(opts).await?;
                    Ok(Self {
                        inner: Arc::new(RwLock::new(Some(multi_conn))),
                        in_transaction: AtomicBool::new(false),
                    })
                })
            }
            _ => Err(Error::IncorrectApiUsageError(
                "Unknown backend. Supported backends: 'mysql', 'wtx', 'zero'",
            )
            .into()),
        }
    }

    #[pyo3(signature = (consistent_snapshot=false, isolation_level=None, readonly=None))]
    fn start_transaction(
        slf: Py<Self>,
        consistent_snapshot: bool,
        isolation_level: Option<PyRef<IsolationLevel>>,
        readonly: Option<bool>,
    ) -> AsyncTransaction {
        let isolation_level_str: Option<String> = isolation_level.map(|l| l.as_str().to_string());
        AsyncTransaction::new(slf, consistent_snapshot, isolation_level_str, readonly)
    }

    async fn id(&self) -> PyResult<u64> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .id())
    }

    async fn affected_rows(&self) -> PyResult<u64> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .affected_rows())
    }

    async fn last_insert_id(&self) -> PyResult<Option<u64>> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .last_insert_id())
    }
    async fn close(&self) -> PyroResult<()> {
        let mut inner = self.inner.write().await;
        if let Some(conn) = inner.take() {
            conn.disconnect().await?;
        }
        Ok(())
    }
    async fn reset(&self) -> PyroResult<()> {
        let mut inner = self.inner.write().await;
        inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .reset()
            .await?;
        Ok(())
    }

    fn server_version<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            Ok(inner
                .read()
                .await
                .as_ref()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .server_version())
        })
    }
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        self.inner.ping(py)
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    #[pyo3(signature = (query, *, as_dict=false))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.query(py, query, as_dict)
    }
    #[pyo3(signature = (query, *, as_dict=false))]
    fn query_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.query_first(py, query, as_dict)
    }
    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.inner.query_drop(py, query)
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────
    #[pyo3(signature = (query, params=None, *, as_dict=false))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec(py, query, params, as_dict)
    }
    #[pyo3(signature = (query, params=None, *, as_dict=false))]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec_first(py, query, params, as_dict)
    }
    #[pyo3(signature = (query, params=None))]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec_drop(py, query, params)
    }
    #[pyo3(signature = (query, params=vec![]))]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_batch(py, query, params)
    }
    #[pyo3(signature = (query, params=vec![], *, as_dict=false))]
    fn exec_bulk<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_bulk(py, query, params, as_dict)
    }
}
