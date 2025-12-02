// PEP 249 – Python Database API Specification v2.0

use std::collections::HashMap;

use either::Either;
use parking_lot::RwLock;
use pyo3::{prelude::*, types::PyList};
use zero_mysql::PreparedStatement;
use zero_mysql::sync::Conn;

use crate::{
    dbapi::{cursor::Cursor, error::DbApiResult, zero_handler::DbApiHandler},
    error::Error,
    opts::Opts as PyroOpts,
    params::Params,
    zero_params_adapter::ParamsAdapter,
};

use pyo3::types::PyTuple;

/// Internal connection wrapper for dbapi sync
pub struct DbApiZeroConn {
    pub inner: Conn,
    pub stmt_cache: HashMap<String, PreparedStatement>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl DbApiZeroConn {
    pub fn new(url: &str) -> Result<Self, Error> {
        let opts: zero_mysql::Opts = url.try_into().map_err(Error::ZeroMysqlError)?;
        Self::new_with_opts(opts)
    }

    pub fn new_with_opts(opts: zero_mysql::Opts) -> Result<Self, Error> {
        let inner = Conn::new(opts)?;
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

    pub fn ping(&mut self) -> Result<(), Error> {
        self.inner.ping()?;
        Ok(())
    }

    pub fn exec_drop(&mut self, query: String, params: Params) -> Result<(), Error> {
        use crate::sync::handler::DropHandler;

        if !self.stmt_cache.contains_key(&query) {
            let stmt = self.inner.prepare(&query)?;
            self.stmt_cache.insert(query.clone(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = self.stmt_cache.get_mut(&query).unwrap();

        let mut handler = DropHandler::default();
        let params_adapter = ParamsAdapter::new(&params);
        self.inner.exec(stmt, params_adapter, &mut handler)?;
        self.affected_rows = handler.affected_rows;
        self.last_insert_id = handler.last_insert_id;
        Ok(())
    }
}

#[pyclass(module = "pyro_mysql.dbapi", name = "Connection")]
pub struct DbApiConn {
    conn: RwLock<Option<DbApiZeroConn>>,
}

pub struct DbApiRow(pub Py<PyTuple>);

impl DbApiRow {
    pub fn to_tuple<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        Ok(self.0.bind(py).clone())
    }
}

pub enum DbApiExecResult {
    WithDescription {
        rows: Vec<DbApiRow>,
        description: Py<PyList>,
        affected_rows: u64,
    },
    NoDescription {
        affected_rows: u64,
        last_insert_id: u64,
    },
}

impl DbApiConn {
    pub fn new(url_or_opts: Either<String, PyRef<PyroOpts>>) -> DbApiResult<Self> {
        let conn = match url_or_opts {
            Either::Left(url) => DbApiZeroConn::new(&url)?,
            Either::Right(opts) => DbApiZeroConn::new_with_opts(opts.inner.clone())?,
        };
        Ok(Self {
            conn: RwLock::new(Some(conn)),
        })
    }

    fn with_conn<T, F>(&self, f: F) -> DbApiResult<T>
    where
        F: FnOnce(&mut DbApiZeroConn) -> DbApiResult<T>,
    {
        let mut guard = self.conn.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        f(conn)
    }

    pub fn exec(&self, query: &str, params: Params) -> DbApiResult<DbApiExecResult> {
        self.with_conn(|conn| {
            log::debug!("execute {query}");

            // Prepare the statement (with caching)
            if !conn.stmt_cache.contains_key(query) {
                let stmt = conn.inner.prepare(query).map_err(Error::ZeroMysqlError)?;
                conn.stmt_cache.insert(query.to_string(), stmt);
            }
            #[expect(clippy::unwrap_used)]
            let stmt = conn.stmt_cache.get_mut(query).unwrap();

            // Execute with custom handler that captures description
            let result: DbApiExecResult = Python::attach(|py| {
                let mut handler = DbApiHandler::new(py);
                let params_adapter = ParamsAdapter::new(&params);

                log::debug!("About to call conn.inner.exec with stmt_id={}", stmt.id());
                let exec_result = conn.inner.exec(stmt, params_adapter, &mut handler);
                log::debug!("conn.inner.exec returned: {:?}", exec_result.is_ok());
                exec_result.map_err(|e| {
                    log::debug!("exec error: {:?}", e);
                    Error::ZeroMysqlError(e)
                })?;

                Ok::<_, Error>(handler.into_result())
            })?;

            Ok(result)
        })
    }

    fn exec_drop(&self, query: &str, params: Params) -> DbApiResult<()> {
        self.with_conn(|conn| {
            log::debug!("execute {query}");
            conn.exec_drop(query.to_string(), params)?;
            Ok(())
        })
    }

    pub fn exec_batch(&self, query: &str, params: Vec<Params>) -> DbApiResult<u64> {
        self.with_conn(|conn| {
            log::debug!("execute {query}");
            let mut affected = 0;
            for params in params {
                conn.exec_drop(query.to_string(), params)?;
                affected += conn.affected_rows();
            }
            Ok(affected)
        })
    }
}

#[pymethods]
impl DbApiConn {
    // ─── Pep249 ──────────────────────────────────────────────────────────

    pub fn close(&self) {
        // TODO: consider raising if already closed
        *self.conn.write() = None;
    }

    fn commit(&self) -> DbApiResult<()> {
        self.exec_drop("COMMIT", Params::default())
    }

    fn rollback(&self) -> DbApiResult<()> {
        self.exec_drop("ROLLBACK", Params::default())
    }

    /// Cursor instances hold a reference to the python connection object.
    fn cursor(slf: Py<DbApiConn>) -> Cursor {
        Cursor::new(slf)
    }

    // ─── Helper ──────────────────────────────────────────────────────────

    pub fn set_autocommit(&self, on: bool) -> DbApiResult<()> {
        if on {
            self.exec_drop("SET autocommit=1", Params::default())
        } else {
            self.exec_drop("SET autocommit=0", Params::default())
        }
    }

    fn ping(&self) -> DbApiResult<()> {
        self.with_conn(|conn| {
            conn.ping()?;
            Ok(())
        })
    }

    pub fn last_insert_id(&self) -> DbApiResult<Option<u64>> {
        let guard = self.conn.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
        let id = conn.last_insert_id();
        Ok(if id == 0 { None } else { Some(id) })
    }

    pub fn is_closed(&self) -> bool {
        self.conn.read().is_some()
    }
}
