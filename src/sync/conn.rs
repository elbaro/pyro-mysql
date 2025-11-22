use std::sync::atomic::AtomicBool;

use either::Either;
use mysql::{Opts as MysqlOpts, prelude::*};
use parking_lot::RwLock;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::opts::Opts;
use crate::params::Params;
use crate::row::Row;
use crate::sync::multi_conn::MultiSyncConn;
use crate::sync::transaction::SyncTransaction;

#[pyclass(module = "pyro_mysql.sync", name = "Conn")]
pub struct SyncConn {
    pub inner: RwLock<Option<MultiSyncConn>>,
    pub in_transaction: AtomicBool,
}

#[pymethods]
impl SyncConn {
    #[new]
    #[pyo3(signature = (url_or_opts, backend="mysql"))]
    pub fn new(url_or_opts: Either<String, PyRef<Opts>>, backend: &str) -> PyroResult<Self> {
        match backend {
            "mysql" => {
                let opts = match url_or_opts {
                    Either::Left(url) => MysqlOpts::from_url(&url)?,
                    Either::Right(opts) => opts.to_mysql_opts(),
                };
                let conn = crate::sync::backend::MysqlConn::new(opts)?;

                Ok(Self {
                    inner: RwLock::new(Some(MultiSyncConn::Mysql(conn))),
                    in_transaction: AtomicBool::new(false),
                })
            }
            "diesel" => {
                let url = match url_or_opts {
                    Either::Left(url) => url,
                    Either::Right(_opts) => {
                        return Err(crate::error::Error::IncorrectApiUsageError(
                            "Diesel backend currently only supports URL strings",
                        ));
                    }
                };
                let conn = crate::sync::backend::DieselConn::new(&url)?;

                Ok(Self {
                    inner: RwLock::new(Some(MultiSyncConn::Diesel(conn))),
                    in_transaction: AtomicBool::new(false),
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
                let conn = crate::sync::backend::ZeroMysqlConn::new_with_opts(opts)?;

                Ok(Self {
                    inner: RwLock::new(Some(MultiSyncConn::ZeroMysql(conn))),
                    in_transaction: AtomicBool::new(false),
                })
            }
            _ => Err(crate::error::Error::IncorrectApiUsageError(
                "Unknown backend. Supported backends: 'mysql', 'diesel', 'zero'",
            )),
        }
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
        Ok(conn.id())
    }

    fn affected_rows(&self) -> PyResult<u64> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Connection is not available")
        })?;
        Ok(conn.affected_rows())
    }

    fn last_insert_id(&self) -> PyroResult<Option<u64>> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.last_insert_id())
    }

    fn ping(&self) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        conn.ping()
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
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        match multi_conn {
            MultiSyncConn::Mysql(conn) => {
                let rows: Vec<Row> = conn.inner.query(query)?;
                // Convert rows to either tuples or dicts
                let result: Vec<Py<PyAny>> = if as_dict {
                    rows.iter()
                        .map(|row| row.to_dict(py).map(|d| d.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                } else {
                    rows.iter()
                        .map(|row| row.to_tuple(py).map(|t| t.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                };
                Ok(result)
            }
            MultiSyncConn::Diesel(conn) => {
                // Diesel handles as_dict internally
                conn.query(query, as_dict)
            }
            MultiSyncConn::ZeroMysql(conn) => {
                let rows = conn.query(py, query, as_dict)?;
                // ZeroMysql returns PyList, convert to Vec<Py<PyAny>>
                let result: Vec<Py<PyAny>> =
                    rows.bind(py).iter().map(|item| item.unbind()).collect();
                Ok(result)
            }
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
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        match multi_conn {
            MultiSyncConn::Mysql(conn) => {
                let row: Option<Row> = conn.inner.query_first(query)?;
                // Convert row to either tuple or dict
                match row {
                    Some(r) => {
                        let result: Py<PyAny> = if as_dict {
                            r.to_dict(py)?.into_any().unbind()
                        } else {
                            r.to_tuple(py)?.into_any().unbind()
                        };
                        Ok(Some(result))
                    }
                    None => Ok(None),
                }
            }
            MultiSyncConn::Diesel(conn) => {
                // Diesel handles as_dict internally
                conn.query_first(query, as_dict)
            }
            MultiSyncConn::ZeroMysql(conn) => {
                let rows = conn.query(py, query, as_dict)?;
                // Get first row if any
                Ok(if rows.bind(py).len() > 0 {
                    Some(rows.bind(py).get_item(0)?.unbind())
                } else {
                    None
                })
            }
        }
    }

    #[pyo3(signature = (query))]
    fn query_drop(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        match multi_conn {
            MultiSyncConn::Mysql(conn) => Ok(conn.inner.query_drop(query)?),
            MultiSyncConn::Diesel(conn) => conn.query_drop(query),
            MultiSyncConn::ZeroMysql(conn) => conn.query_drop(query),
        }
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
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        match multi_conn {
            MultiSyncConn::Mysql(conn) => {
                // log::debug!("exec {query}");
                let rows: Vec<Row> =
                    conn.inner
                        .exec_fold(query, params, Vec::new(), |mut acc, row| {
                            acc.push(mysql::from_row::<Row>(row));
                            acc
                        })?;

                // Convert rows to either tuples or dicts
                let result: Vec<Py<PyAny>> = if as_dict {
                    rows.iter()
                        .map(|row| row.to_dict(py).map(|d| d.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                } else {
                    rows.iter()
                        .map(|row| row.to_tuple(py).map(|t| t.into_any().unbind()))
                        .collect::<PyResult<_>>()?
                };
                Ok(PyList::new(py, result).unwrap().unbind())
            }
            MultiSyncConn::Diesel(conn) => {
                // Diesel handles as_dict internally
                Ok(PyList::new(py, conn.exec(query, params, as_dict)?)
                    .unwrap()
                    .unbind())
            }
            MultiSyncConn::ZeroMysql(conn) => conn.exec(py, query, params, as_dict)
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
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

        match multi_conn {
            MultiSyncConn::Mysql(conn) => {
                // log::debug!("exec_first {query}");
                let row: Option<Row> = conn.inner.exec_first(query, params)?;

                // Convert row to either tuple or dict
                match row {
                    Some(r) => {
                        let result: Py<PyAny> = if as_dict {
                            r.to_dict(py)?.into_any().unbind()
                        } else {
                            r.to_tuple(py)?.into_any().unbind()
                        };
                        Ok(Some(result))
                    }
                    None => Ok(None),
                }
            }
            MultiSyncConn::Diesel(conn) => {
                // Diesel handles as_dict internally
                conn.exec_first(query, params, as_dict)
            }
            MultiSyncConn::ZeroMysql(conn) => conn.exec_first(py, query, params, as_dict),
        }
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&self, query: String, params: Params) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        match multi_conn {
            MultiSyncConn::Mysql(conn) => {
                // log::debug!("exec_drop {query}");
                Ok(conn.inner.exec_drop(query, params)?)
            }
            MultiSyncConn::Diesel(conn) => conn.exec_drop(query, params),
            MultiSyncConn::ZeroMysql(conn) => conn.exec_drop(query, params),
        }
    }

    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&self, query: String, params_list: Vec<Params>) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        match multi_conn {
            MultiSyncConn::Mysql(conn) => {
                // log::debug!("exec_batch {query}");
                Ok(conn.inner.exec_batch(query, params_list)?)
            }
            MultiSyncConn::Diesel(conn) => conn.exec_batch(query, params_list),
            MultiSyncConn::ZeroMysql(conn) => {
                // Execute each params set
                for params in params_list {
                    conn.exec_drop(query.clone(), params)?;
                }
                Ok(())
            }
        }
    }

    pub fn close(&self) {
        *self.inner.write() = None;
    }

    fn reset(&self) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        conn.reset()
    }

    fn server_version(&self) -> PyroResult<String> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.server_version())
    }
}

// Public methods for internal use (not exposed to Python via #[pymethods])
impl SyncConn {
    /// Execute a query and discard results. Used internally by Transaction.
    pub fn query_drop_internal(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        match multi_conn {
            MultiSyncConn::Mysql(conn) => Ok(conn.inner.query_drop(query)?),
            MultiSyncConn::Diesel(conn) => conn.query_drop(query),
            MultiSyncConn::ZeroMysql(conn) => conn.query_drop(query),
        }
    }
}
