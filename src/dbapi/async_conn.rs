// PEP 249 – Python Database API Specification v2.0 (Async version)

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use tokio::sync::RwLock;
use zero_mysql::PreparedStatement;
use zero_mysql::tokio::Conn;

use crate::{
    r#async::handler::DropHandler,
    dbapi::{
        async_cursor::{AsyncCursor, new_async_cursor},
        error::DbApiResult,
    },
    error::{Error, PyroResult},
    params::Params,
    util::tokio_spawn_as_abort_on_drop,
    zero_params_adapter::ParamsAdapter,
};

/// Internal async connection wrapper for dbapi
pub struct DbApiAsyncZeroConn {
    pub inner: Conn,
    pub stmt_cache: HashMap<String, PreparedStatement>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl DbApiAsyncZeroConn {
    pub async fn new_with_opts(opts: zero_mysql::Opts) -> Result<Self, Error> {
        let inner = Conn::new(opts).await?;
        Ok(Self {
            inner,
            stmt_cache: HashMap::new(),
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    pub async fn ping(&mut self) -> Result<(), Error> {
        self.inner.ping().await?;
        Ok(())
    }

    pub async fn query_drop(&mut self, query: String) -> Result<(), Error> {
        let mut handler = DropHandler::default();
        self.inner.query(&query, &mut handler).await?;
        self.affected_rows = handler.affected_rows;
        self.last_insert_id = handler.last_insert_id;
        Ok(())
    }

    pub async fn exec_drop(&mut self, query: String, params: Params) -> Result<(), Error> {
        if !self.stmt_cache.contains_key(&query) {
            let stmt = self.inner.prepare(&query).await?;
            self.stmt_cache.insert(query.clone(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = self.stmt_cache.get_mut(&query).unwrap();

        let mut handler = DropHandler::default();
        let params_adapter = ParamsAdapter::new(&params);
        self.inner.exec(stmt, params_adapter, &mut handler).await?;
        self.affected_rows = handler.affected_rows;
        self.last_insert_id = handler.last_insert_id;
        Ok(())
    }
}

#[pyclass(module = "pyro_mysql.dbapi_async", name = "Connection")]
pub struct AsyncDbApiConn(pub Arc<RwLock<Option<DbApiAsyncZeroConn>>>);

// Protocols:
// - sqlalchemy.connectors.AsyncAdapt_terminate
// - sqlalchemy.connectors.AsyncAdapt_dbapi_connection

#[pymethods]
impl AsyncDbApiConn {
    // ─── Pep249 ──────────────────────────────────────────────────────────

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            *arc.write().await = None;
            Ok(())
        })
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = arc.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            conn.query_drop("COMMIT".to_string()).await?;
            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = arc.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            conn.query_drop("ROLLBACK".to_string()).await?;
            Ok(())
        })
    }

    /// Cursor instances hold a reference to the python connection object.
    fn cursor(slf: Py<AsyncDbApiConn>) -> AsyncCursor {
        new_async_cursor(slf)
    }

    // ─── Helper ──────────────────────────────────────────────────────────

    pub async fn set_autocommit(&self, on: bool) -> PyroResult<()> {
        let arc = Arc::clone(&self.0);
        let mut guard = arc.write().await;
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let query = if on {
            "SET autocommit=1"
        } else {
            "SET autocommit=0"
        };
        conn.query_drop(query.to_string()).await?;
        Ok(())
    }

    async fn ping(&self) -> DbApiResult<()> {
        let arc = Arc::clone(&self.0);
        tokio_spawn_as_abort_on_drop(async move {
            let mut guard = arc.write().await;
            let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
            conn.ping().await?;
            PyroResult::Ok(())
        })
        .await??;
        Ok(())
    }

    /// Returns 0 if there was no last insert id.
    fn last_insert_id<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = arc.read().await;
            let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;

            let id = conn.last_insert_id();
            Ok(Some(id))
        })
    }

    fn is_closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arc = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = arc.read().await;
            Ok(guard.is_none()) // Fixed: is_none for closed, not is_some
        })
    }
}
