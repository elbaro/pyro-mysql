// PEP 249 – Python Database API Specification v2.0

use either::Either;
use mysql::{Opts, prelude::Queryable};
use parking_lot::RwLock;
use pyo3::{prelude::*, types::PyList};

use crate::{
    dbapi::{cursor::Cursor, error::DbApiResult},
    error::Error,
    params::Params,
    row::Row,
    sync::opts::SyncOpts,
};

#[pyclass(module = "pyro_mysql.dbapi", name = "Connection")]
pub struct DbApiConn(RwLock<Option<mysql::Conn>>);

pub enum DbApiExecResult {
    WithDescription {
        rows: Vec<Row>,
        description: Py<PyList>,
        affected_rows: u64,
    },
    NoDescription {
        affected_rows: u64,
    },
}

impl DbApiConn {
    pub fn new(url_or_opts: Either<String, PyRef<SyncOpts>>) -> DbApiResult<Self> {
        let opts = match url_or_opts {
            Either::Left(url) => Opts::from_url(&url).map_err(Error::from)?,
            Either::Right(opts) => opts.opts.clone(),
        };
        let conn = mysql::Conn::new(opts).map_err(Error::from)?;
        Ok(Self(RwLock::new(Some(conn))))
    }

    pub fn exec(&self, query: &str, params: Params) -> DbApiResult<DbApiExecResult> {
        let mut guard = self.0.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("execute {query}");

        let mut query_result = conn.exec_iter(query, params).map_err(Error::from)?;
        let affected_rows = query_result.affected_rows();
        if query_result.columns().as_ref().iter().count() > 0 {
            let description = Python::attach(|py| {
                PyList::new(
                    py,
                    query_result.columns().as_ref().iter().map(|col|
                        // tuple of 7 items
                        (
                            col.name_str(),          // name
                            col.column_type() as u8, // type_code
                            col.column_length(),     // display_size
                            None::<Option<()>>,      // internal_size
                            None::<Option<()>>,      // precision
                            None::<Option<()>>,      // scale
                            None::<Option<()>>,      // null_ok
                        )
                        .into_pyobject(py).unwrap()),
                )
                .map(|bound| bound.unbind())
            })?;

            let rows = query_result
                .try_fold(Vec::new(), |mut vec, row| {
                    row.map(|row| {
                        vec.push(mysql::from_row(row));
                        vec
                    })
                })
                .map_err(Error::from)?;

            Ok(DbApiExecResult::WithDescription {
                rows,
                description,
                affected_rows,
            })
        } else {
            // no result set (different from empty set)
            Ok(DbApiExecResult::NoDescription { affected_rows })
        }
    }

    fn exec_drop(&self, query: &str, params: Params) -> DbApiResult<()> {
        let mut guard = self.0.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("execute {query}");
        Ok(conn.exec_drop(query, params).map_err(Error::from)?)
    }

    pub fn exec_batch(&self, query: &str, params: Vec<Params>) -> DbApiResult<()> {
        let mut guard = self.0.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("execute {query}");
        Ok(conn.exec_batch(query, params).map_err(Error::from)?)
    }
}

#[pymethods]
impl DbApiConn {
    // ─── Pep249 ──────────────────────────────────────────────────────────

    pub fn close(&self) {
        // TODO: consdier raising if already closed
        *self.0.write() = None;
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
        let mut guard = self.0.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.ping().map_err(Error::from)?)
    }

    /// Returns 0 if there was no last insert id.
    pub fn last_insert_id(&self) -> DbApiResult<Option<u64>> {
        let guard = self.0.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;

        let id = conn.last_insert_id();
        Ok(if id == 0 { None } else { Some(id) })
    }
}
