// PEP 249 – Python Database API Specification v2.0

use either::Either;
use mysql::{Opts, prelude::Queryable};
use parking_lot::RwLock;
use pyo3::prelude::*;

use crate::{
    dbapi::cursor::Cursor,
    error::{Error, PyroResult},
    params::Params,
    row::Row,
    sync::opts::SyncOpts,
};

#[pyclass(module = "pyro_mysql.dbapi", name = "Connection")]
pub struct DbApiConn(RwLock<Option<mysql::Conn>>);

impl DbApiConn {
    pub fn new(url_or_opts: Either<String, PyRef<SyncOpts>>) -> PyroResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url)?,
            Either::Right(opts) => opts.opts.clone(),
        };
        let conn = mysql::Conn::new(opts)?;
        Ok(Self(RwLock::new(Some(conn))))
    }

    pub fn exec(&self, query: &str, params: Params) -> PyroResult<Vec<Row>> {
        let mut guard = self.0.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("execute {query}");
        Ok(conn.exec(query, params)?)
    }

    fn exec_drop(&self, query: &str, params: Params) -> PyroResult<()> {
        let mut guard = self.0.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("execute {query}");
        Ok(conn.exec_drop(query, params)?)
    }

    pub fn exec_batch(&self, query: &str, params: Vec<Params>) -> PyroResult<()> {
        let mut guard = self.0.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("execute {query}");
        Ok(conn.exec_batch(query, params)?)
    }
}

#[pymethods]
impl DbApiConn {
    // ─── Pep249 ──────────────────────────────────────────────────────────

    pub fn close(&self) {
        // TODO: consdier raising if already closed
        *self.0.write() = None;
    }

    fn commit(&self) -> PyroResult<()> {
        self.exec_drop("COMMIT", Params::default())
    }

    fn rollback(&self) -> PyroResult<()> {
        self.exec_drop("ROLLBACK", Params::default())
    }

    /// Cursor instances hold a reference to the python connection object.
    fn cursor(slf: Py<DbApiConn>) -> Cursor {
        Cursor::new(slf)
    }

    // ─── Helper ──────────────────────────────────────────────────────────

    pub fn set_autocommit(&self, on: bool) -> PyroResult<()> {
        if on {
            self.exec_drop("SET autocommit=1", Params::default())
        } else {
            self.exec_drop("SET autocommit=0", Params::default())
        }
    }

    fn ping(&self) -> PyroResult<()> {
        let mut guard = self.0.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.ping()?)
    }
}
