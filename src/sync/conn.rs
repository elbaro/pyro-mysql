use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use either::Either;
use parking_lot::RwLock;
use pyo3::prelude::*;
use pyo3::types::PyList;
use zero_mysql::PreparedStatement;
use zero_mysql::sync::Conn;

use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::opts::Opts;
use crate::params::Params;
use crate::sync::handler::{DictHandler, DropHandler, TupleHandler};
use crate::sync::transaction::SyncTransaction;
use crate::zero_params_adapter::{BulkParamsSetAdapter, ParamsAdapter};

#[pyclass(module = "pyro_mysql.sync", name = "Conn")]
pub struct SyncConn {
    pub inner: RwLock<Option<Conn>>,
    pub in_transaction: AtomicBool,
    pub stmt_cache: RwLock<HashMap<String, PreparedStatement>>,
    affected_rows: RwLock<u64>,
    last_insert_id: RwLock<u64>,
}

#[pymethods]
impl SyncConn {
    #[new]
    #[pyo3(signature = (url_or_opts))]
    pub fn new(url_or_opts: Either<String, PyRef<Opts>>) -> PyroResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => {
                let inner: zero_mysql::Opts = url.as_str().try_into().map_err(Error::from)?;
                inner
            }
            Either::Right(opts) => opts.inner.clone(),
        };
        let conn = Conn::new(opts)?;

        Ok(Self {
            inner: RwLock::new(Some(conn)),
            in_transaction: AtomicBool::new(false),
            stmt_cache: RwLock::new(HashMap::new()),
            affected_rows: RwLock::new(0),
            last_insert_id: RwLock::new(0),
        })
    }

    #[pyo3(signature=(consistent_snapshot=false, isolation_level=None, readonly=None))]
    fn start_transaction(
        slf: Py<Self>,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> SyncTransaction {
        let isolation_level_str: Option<String> = isolation_level.map(|l| l.as_str().to_string());
        SyncTransaction::new(slf, consistent_snapshot, isolation_level_str, readonly)
    }

    fn id(&self) -> PyroResult<u64> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.connection_id())
    }

    fn affected_rows(&self) -> PyResult<u64> {
        Ok(*self.affected_rows.read())
    }

    fn last_insert_id(&self) -> PyroResult<Option<u64>> {
        Ok(Some(*self.last_insert_id.read()))
    }

    fn ping(&self) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        conn.ping()?;
        Ok(())
    }

    // ─── Text Protocol ───────────────────────────────────────────────────

    #[pyo3(signature = (query, *, as_dict=false))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyroResult<Vec<Py<PyAny>>> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        if as_dict {
            let mut handler = DictHandler::new(py);
            conn.query(&query, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            let rows = handler.into_rows();
            Ok(rows.bind(py).iter().map(|item| item.unbind()).collect())
        } else {
            let mut handler = TupleHandler::new(py);
            conn.query(&query, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            let rows = handler.into_rows();
            Ok(rows.bind(py).iter().map(|item| item.unbind()).collect())
        }
    }

    #[pyo3(signature = (query, *, as_dict=false))]
    fn query_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        as_dict: bool,
    ) -> PyroResult<Option<Py<PyAny>>> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        if as_dict {
            let mut handler = DictHandler::new(py);
            conn.query(&query, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        } else {
            let mut handler = TupleHandler::new(py);
            conn.query(&query, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        }
    }

    #[pyo3(signature = (query))]
    fn query_drop(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        let mut handler = DropHandler::default();
        conn.query(&query, &mut handler)?;

        *self.affected_rows.write() = handler.affected_rows;
        *self.last_insert_id.write() = handler.last_insert_id;
        Ok(())
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────

    #[pyo3(signature = (query, params=Params::default(), *, as_dict=false))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
        as_dict: bool,
    ) -> PyroResult<Py<PyList>> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        let mut cache = self.stmt_cache.write();
        if !cache.contains_key(&query) {
            let stmt = conn
                .prepare(&query)
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            cache.insert(query.clone(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = cache.get_mut(&query).unwrap();

        let params_adapter = ParamsAdapter::new(&params);
        if as_dict {
            let mut handler = DictHandler::new(py);
            conn.exec(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            Ok(handler.into_rows())
        } else {
            let mut handler = TupleHandler::new(py);
            conn.exec(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            Ok(handler.into_rows())
        }
    }

    #[pyo3(signature = (query, params=Params::default(), *, as_dict=false))]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
        as_dict: bool,
    ) -> PyroResult<Option<Py<PyAny>>> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        let mut cache = self.stmt_cache.write();
        if !cache.contains_key(&query) {
            let stmt = conn
                .prepare(&query)
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            cache.insert(query.clone(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = cache.get_mut(&query).unwrap();

        let params_adapter = ParamsAdapter::new(&params);
        if as_dict {
            let mut handler = DictHandler::new(py);
            conn.exec_first(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        } else {
            let mut handler = TupleHandler::new(py);
            conn.exec_first(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        }
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&self, query: String, params: Params) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        let mut cache = self.stmt_cache.write();
        if !cache.contains_key(&query) {
            let stmt = conn
                .prepare(&query)
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            cache.insert(query.clone(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = cache.get_mut(&query).unwrap();

        let mut handler = DropHandler::default();
        let params_adapter = ParamsAdapter::new(&params);
        conn.exec(stmt, params_adapter, &mut handler)?;

        *self.affected_rows.write() = handler.affected_rows;
        *self.last_insert_id.write() = handler.last_insert_id;
        Ok(())
    }

    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&self, query: String, params_list: Vec<Params>) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        let mut cache = self.stmt_cache.write();
        if !cache.contains_key(&query) {
            let stmt = conn
                .prepare(&query)
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            cache.insert(query.clone(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = cache.get_mut(&query).unwrap();

        for params in params_list {
            let mut handler = DropHandler::default();
            let params_adapter = ParamsAdapter::new(&params);
            conn.exec(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows;
            *self.last_insert_id.write() = handler.last_insert_id;
        }
        Ok(())
    }

    #[pyo3(signature = (query, params_list=vec![], *, as_dict=false))]
    fn exec_bulk_insert_or_update<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params_list: Vec<Params>,
        as_dict: bool,
    ) -> PyroResult<Py<PyList>> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        let mut cache = self.stmt_cache.write();
        if !cache.contains_key(&query) {
            let stmt = conn
                .prepare(&query)
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            cache.insert(query.clone(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = cache.get_mut(&query).unwrap();

        let params_adapters: Vec<ParamsAdapter> =
            params_list.iter().map(ParamsAdapter::new).collect();
        let bulk_params = BulkParamsSetAdapter::new(params_adapters);
        let flags = zero_mysql::protocol::command::bulk_exec::BulkFlags::SEND_TYPES_TO_SERVER;

        if as_dict {
            let mut handler = DictHandler::new(py);
            conn.exec_bulk(stmt, bulk_params, flags, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            Ok(handler.into_rows())
        } else {
            let mut handler = TupleHandler::new(py);
            conn.exec_bulk(stmt, bulk_params, flags, &mut handler)?;
            *self.affected_rows.write() = handler.affected_rows();
            *self.last_insert_id.write() = handler.last_insert_id();
            Ok(handler.into_rows())
        }
    }

    pub fn close(&self) {
        *self.inner.write() = None;
    }

    fn reset(&self) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        conn.reset()?;
        self.stmt_cache.write().clear();
        Ok(())
    }

    fn server_version(&self) -> PyroResult<String> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
        let version_bytes = conn.server_version();
        Ok(String::from_utf8_lossy(version_bytes).to_string())
    }
}

// Public methods for internal use (not exposed to Python via #[pymethods])
impl SyncConn {
    pub fn query_drop_internal(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        let mut handler = DropHandler::default();
        conn.query(&query, &mut handler)?;

        *self.affected_rows.write() = handler.affected_rows;
        *self.last_insert_id.write() = handler.last_insert_id;
        Ok(())
    }
}
