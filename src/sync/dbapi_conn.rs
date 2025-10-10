// PEP 249 – Python Database API Specification v2.0

use either::Either;
use mysql::{Opts, prelude::Queryable};
use parking_lot::RwLock;
use pyo3::prelude::*;

use crate::{
    error::{Error, PyroResult},
    params::Params,
    sync::opts::SyncOpts,
};

#[pyclass]
pub struct SyncDbApiConn(RwLock<Option<mysql::Conn>>);

impl SyncDbApiConn {
    pub fn new(url_or_opts: Either<String, PyRef<SyncOpts>>) -> PyroResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url)?,
            Either::Right(opts) => opts.opts.clone(),
        };
        let conn = mysql::Conn::new(opts)?;
        Ok(Self(RwLock::new(Some(conn))))
    }

    fn exec_drop(&self, query: &str, params: Params) -> PyroResult<()> {
        let mut guard = self.0.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("execute {query}");
        Ok(conn.exec_drop(query, params)?)
    }
}

#[pymethods]
impl SyncDbApiConn {
    // ─── Pep249 ──────────────────────────────────────────────────────────

    fn close(&self) {
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
    fn cursor(&self) {
        todo!()
    }

    // ─── Helper ──────────────────────────────────────────────────────────

    pub fn set_autocommit(&self, on: bool) -> PyroResult<()> {
        if on {
            self.exec_drop("SET autocommit=1", Params::default())
        } else {
            self.exec_drop("SET autocommit=0", Params::default())
        }
    }
}
