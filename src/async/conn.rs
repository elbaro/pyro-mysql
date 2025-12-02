use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use either::Either;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyDict, PyTuple};
use tokio::sync::RwLock;
use zero_mysql::PreparedStatement;
use zero_mysql::tokio::Conn;

use crate::r#async::handler::{DictHandler, DropHandler, TupleHandler};
use crate::r#async::transaction::AsyncTransaction;
use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::opts::Opts;
use crate::params::Params;
use crate::util::{PyroFuture, rust_future_into_py};
use crate::zero_params_adapter::{BulkParamsSetAdapter, ParamsAdapter};

#[pyclass(module = "pyro_mysql.async_", name = "Conn")]
pub struct AsyncConn {
    pub inner: Arc<RwLock<Option<Conn>>>,
    pub in_transaction: AtomicBool,
    pub stmt_cache: Arc<RwLock<HashMap<String, PreparedStatement>>>,
    tuple_handler: Arc<RwLock<TupleHandler>>,
    dict_handler: Arc<RwLock<DictHandler>>,
    affected_rows: Arc<RwLock<u64>>,
    last_insert_id: Arc<RwLock<u64>>,
}

#[pymethods]
impl AsyncConn {
    #[new]
    fn _new() -> PyroResult<Self> {
        Err(Error::IncorrectApiUsageError(
            "use `await Conn.new(url) instead of Conn()`.",
        ))
    }

    #[expect(clippy::new_ret_no_self)]
    #[staticmethod]
    #[pyo3(signature = (url_or_opts))]
    pub fn new<'py>(
        py: Python<'py>,
        url_or_opts: Either<String, PyRef<Opts>>,
    ) -> PyResult<Py<PyroFuture>> {
        let opts = match url_or_opts {
            Either::Left(url) => {
                let inner: zero_mysql::Opts = url.as_str().try_into().map_err(Error::from)?;
                inner
            }
            Either::Right(opts) => opts.inner.clone(),
        };
        rust_future_into_py(py, async move {
            let conn = Conn::new(opts).await?;
            Ok(Self {
                inner: Arc::new(RwLock::new(Some(conn))),
                in_transaction: AtomicBool::new(false),
                stmt_cache: Arc::new(RwLock::new(HashMap::new())),
                tuple_handler: Arc::new(RwLock::new(TupleHandler::default())),
                dict_handler: Arc::new(RwLock::new(DictHandler::default())),
                affected_rows: Arc::new(RwLock::new(0)),
                last_insert_id: Arc::new(RwLock::new(0)),
            })
        })
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
            .connection_id())
    }

    async fn affected_rows(&self) -> PyResult<u64> {
        Ok(*self.affected_rows.read().await)
    }

    async fn last_insert_id(&self) -> PyResult<Option<u64>> {
        Ok(Some(*self.last_insert_id.read().await))
    }

    async fn close(&self) -> PyroResult<()> {
        let mut inner = self.inner.write().await;
        *inner = None;
        Ok(())
    }

    async fn reset(&self) -> PyroResult<()> {
        let mut inner = self.inner.write().await;
        let conn = inner.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        conn.reset().await?;
        self.stmt_cache.write().await.clear();
        Ok(())
    }

    fn server_version<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            let guard = inner.read().await;
            let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
            let version_bytes = conn.server_version();
            Ok(String::from_utf8_lossy(version_bytes).to_string())
        })
    }

    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            let mut guard = inner.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            conn.ping().await?;
            Ok(())
        })
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    #[pyo3(signature = (query, *, as_dict=false))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();
        let affected_rows_arc = self.affected_rows.clone();
        let last_insert_id_arc = self.last_insert_id.clone();

        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut guard = inner.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

            if as_dict {
                let mut handler = dict_handler.write().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(|d| d.into_any()).collect())
                })
            } else {
                let mut handler = tuple_handler.write().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows: Vec<Py<PyTuple>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(|t| t.into_any()).collect())
                })
            }
        })
    }

    #[pyo3(signature = (query, *, as_dict=false))]
    fn query_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();
        let affected_rows_arc = self.affected_rows.clone();
        let last_insert_id_arc = self.last_insert_id.clone();

        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut guard = inner.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

            if as_dict {
                let mut handler = dict_handler.write().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(|d| d.into_any()))
                })
            } else {
                let mut handler = tuple_handler.write().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(|t| t.into_any()))
                })
            }
        })
    }

    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        let affected_rows_arc = self.affected_rows.clone();
        let last_insert_id_arc = self.last_insert_id.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut guard = inner.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

            let mut handler = DropHandler::default();
            conn.query(&query, &mut handler).await?;

            *affected_rows_arc.write().await = handler.affected_rows;
            *last_insert_id_arc.write().await = handler.last_insert_id;
            Ok(())
        })
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
        let params_obj: Params = params
            .map(|p| p.extract(py))
            .transpose()?
            .unwrap_or_default();
        let query_string = query.to_string();

        let inner = self.inner.clone();
        let stmt_cache = self.stmt_cache.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();
        let affected_rows_arc = self.affected_rows.clone();
        let last_insert_id_arc = self.last_insert_id.clone();

        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut guard = inner.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

            let mut cache = stmt_cache.write().await;
            if !cache.contains_key(&query_string) {
                let stmt = conn.prepare(&query_string).await?;
                cache.insert(query_string.clone(), stmt);
            }
            #[expect(clippy::unwrap_used)]
            let stmt = cache.get_mut(&query_string).unwrap();

            let params_adapter = ParamsAdapter::new(&params_obj);
            if as_dict {
                let mut handler = dict_handler.write().await;
                handler.clear();
                conn.exec(stmt, params_adapter, &mut *handler).await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(|d| d.into_any()).collect())
                })
            } else {
                let mut handler = tuple_handler.write().await;
                handler.clear();
                conn.exec(stmt, params_adapter, &mut *handler).await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows: Vec<Py<PyTuple>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(|t| t.into_any()).collect())
                })
            }
        })
    }

    #[pyo3(signature = (query, params=None, *, as_dict=false))]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let params_obj: Params = params
            .map(|p| p.extract(py))
            .transpose()?
            .unwrap_or_default();
        let query_string = query.to_string();

        let inner = self.inner.clone();
        let stmt_cache = self.stmt_cache.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();
        let affected_rows_arc = self.affected_rows.clone();
        let last_insert_id_arc = self.last_insert_id.clone();

        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut guard = inner.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

            let mut cache = stmt_cache.write().await;
            if !cache.contains_key(&query_string) {
                let stmt = conn.prepare(&query_string).await?;
                cache.insert(query_string.clone(), stmt);
            }
            #[expect(clippy::unwrap_used)]
            let stmt = cache.get_mut(&query_string).unwrap();

            let params_adapter = ParamsAdapter::new(&params_obj);
            if as_dict {
                let mut handler = dict_handler.write().await;
                handler.clear();
                conn.exec_first(stmt, params_adapter, &mut *handler).await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(|d| d.into_any()))
                })
            } else {
                let mut handler = tuple_handler.write().await;
                handler.clear();
                conn.exec_first(stmt, params_adapter, &mut *handler).await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(|t| t.into_any()))
                })
            }
        })
    }

    #[pyo3(signature = (query, params=None))]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params_obj: Params = params
            .map(|p| p.extract(py))
            .transpose()?
            .unwrap_or_default();
        let query_string = query.to_string();

        let inner = self.inner.clone();
        let stmt_cache = self.stmt_cache.clone();
        let affected_rows_arc = self.affected_rows.clone();
        let last_insert_id_arc = self.last_insert_id.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut guard = inner.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

            let mut cache = stmt_cache.write().await;
            if !cache.contains_key(&query_string) {
                let stmt = conn.prepare(&query_string).await?;
                cache.insert(query_string.clone(), stmt);
            }
            #[expect(clippy::unwrap_used)]
            let stmt = cache.get_mut(&query_string).unwrap();

            let mut handler = DropHandler::default();
            let params_adapter = ParamsAdapter::new(&params_obj);
            conn.exec(stmt, params_adapter, &mut handler).await?;

            *affected_rows_arc.write().await = handler.affected_rows;
            *last_insert_id_arc.write().await = handler.last_insert_id;
            Ok(())
        })
    }

    #[pyo3(signature = (query, params=vec![]))]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let mut params_vec = Vec::new();
        for p in params {
            params_vec.push(p.extract::<Params>(py)?);
        }
        let query_string = query.to_string();

        let inner = self.inner.clone();
        let stmt_cache = self.stmt_cache.clone();
        let affected_rows_arc = self.affected_rows.clone();
        let last_insert_id_arc = self.last_insert_id.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut guard = inner.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

            let mut cache = stmt_cache.write().await;
            if !cache.contains_key(&query_string) {
                let stmt = conn.prepare(&query_string).await?;
                cache.insert(query_string.clone(), stmt);
            }
            #[expect(clippy::unwrap_used)]
            let stmt = cache.get_mut(&query_string).unwrap();

            for params_obj in params_vec {
                let mut handler = DropHandler::default();
                let params_adapter = ParamsAdapter::new(&params_obj);
                conn.exec(stmt, params_adapter, &mut handler).await?;
                *affected_rows_arc.write().await = handler.affected_rows;
                *last_insert_id_arc.write().await = handler.last_insert_id;
            }
            Ok(())
        })
    }

    #[pyo3(signature = (query, params=vec![], *, as_dict=false))]
    fn exec_bulk<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let mut params_vec = Vec::new();
        for p in params {
            params_vec.push(p.extract::<Params>(py)?);
        }
        let query_string = query.to_string();

        let inner = self.inner.clone();
        let stmt_cache = self.stmt_cache.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();
        let affected_rows_arc = self.affected_rows.clone();
        let last_insert_id_arc = self.last_insert_id.clone();

        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut guard = inner.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

            let mut cache = stmt_cache.write().await;
            if !cache.contains_key(&query_string) {
                let stmt = conn.prepare(&query_string).await?;
                cache.insert(query_string.clone(), stmt);
            }
            #[expect(clippy::unwrap_used)]
            let stmt = cache.get_mut(&query_string).unwrap();

            let params_adapters: Vec<ParamsAdapter> =
                params_vec.iter().map(ParamsAdapter::new).collect();
            let bulk_params = BulkParamsSetAdapter::new(params_adapters);
            let flags = zero_mysql::protocol::command::bulk_exec::BulkFlags::SEND_TYPES_TO_SERVER;

            if as_dict {
                let mut handler = dict_handler.write().await;
                handler.clear();
                conn.exec_bulk(stmt, bulk_params, flags, &mut *handler)
                    .await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(|d| d.into_any()).collect())
                })
            } else {
                let mut handler = tuple_handler.write().await;
                handler.clear();
                conn.exec_bulk(stmt, bulk_params, flags, &mut *handler)
                    .await?;
                *affected_rows_arc.write().await = handler.affected_rows();
                *last_insert_id_arc.write().await = handler.last_insert_id();
                Python::attach(|py| {
                    let rows: Vec<Py<PyTuple>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(|t| t.into_any()).collect())
                })
            }
        })
    }
}

// Public methods for internal use (not exposed to Python via #[pymethods])
impl AsyncConn {
    pub async fn query_drop_internal(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.write().await;
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        let mut handler = DropHandler::default();
        conn.query(&query, &mut handler).await?;

        *self.affected_rows.write().await = handler.affected_rows;
        *self.last_insert_id.write().await = handler.last_insert_id;
        Ok(())
    }
}
