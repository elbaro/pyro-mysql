// PEP 249 – Python Database API Specification v2.0

use either::Either;
use parking_lot::RwLock;
use pyo3::{prelude::*, types::PyList};

use crate::{
    dbapi::{cursor::Cursor, error::DbApiResult, zero_handler::DbApiHandler},
    error::Error,
    opts::Opts as PyroOpts,
    params::Params,
    sync::backend::zero_mysql::params_adapter::ParamsAdapter,
    sync::backend::ZeroMysqlConn,
};

use pyo3::types::PyTuple;

#[pyclass(module = "pyro_mysql.dbapi", name = "Connection")]
pub struct DbApiConn {
    conn: RwLock<Option<ZeroMysqlConn>>,
}

/// A row from zero_mysql backend (already a Python tuple)
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
            Either::Left(url) => ZeroMysqlConn::new(&url)?,
            Either::Right(opts) => ZeroMysqlConn::new_with_opts(opts.inner.clone())?,
        };
        Ok(Self {
            conn: RwLock::new(Some(conn)),
        })
    }

    fn with_conn<T, F>(&self, f: F) -> DbApiResult<T>
    where
        F: FnOnce(&mut ZeroMysqlConn) -> DbApiResult<T>,
    {
        let mut guard = self.conn.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        f(conn)
    }

    pub fn exec(&self, query: &str, params: Params) -> DbApiResult<DbApiExecResult> {
        self.with_conn(|conn| {
            log::debug!("execute {query}");

            // Prepare the statement (with caching)
            let stmt_id = if let Some(&cached_id) = conn.stmt_cache.get(query) {
                cached_id
            } else {
                let stmt_id = conn
                    .inner
                    .prepare(query)
                    .map_err(Error::ZeroMysqlError)?;
                conn.stmt_cache.insert(query.to_string(), stmt_id);
                stmt_id
            };

            // Execute with custom handler that captures description
            let result: DbApiExecResult = Python::attach(|py| {
                let mut handler = DbApiHandler::new(py);
                let params_adapter = ParamsAdapter::new(&params);

                log::debug!("About to call conn.inner.exec with stmt_id={}", stmt_id);
                let exec_result = conn.inner
                    .exec(stmt_id, params_adapter, &mut handler);
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
