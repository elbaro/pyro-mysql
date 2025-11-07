use either::Either;
use mysql::{AccessMode, Opts, prelude::*};
use parking_lot::RwLock;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::params::Params;
use crate::row::Row;
use crate::sync::iterator::ResultSetIterator;
use crate::sync::multi_conn::MultiSyncConn;
use crate::sync::opts::SyncOpts;
use crate::sync::transaction::SyncTransaction;

#[pyclass(module = "pyro_mysql.sync", name = "Conn")]
pub struct SyncConn {
    pub inner: RwLock<Option<MultiSyncConn>>,
}

#[pymethods]
impl SyncConn {
    #[new]
    #[pyo3(signature = (url_or_opts, backend=None))]
    pub fn new(
        url_or_opts: Either<String, PyRef<SyncOpts>>,
        backend: Option<&str>,
    ) -> PyroResult<Self> {
        let backend_name = backend.unwrap_or("mysql");

        match backend_name {
            "mysql" => {
                let opts = match url_or_opts {
                    Either::Left(url) => Opts::from_url(&url)?,
                    Either::Right(opts) => opts.opts.clone(),
                };
                let conn = crate::sync::backend::MysqlConn::new(opts)?;

                Ok(Self {
                    inner: RwLock::new(Some(MultiSyncConn::Mysql(conn))),
                })
            }
            "diesel" => {
                let url = match url_or_opts {
                    Either::Left(url) => url,
                    Either::Right(opts) => {
                        // For diesel, we need a URL string
                        // This is a simplified conversion - in production, you'd need proper URL construction
                        return Err(crate::error::Error::IncorrectApiUsageError(
                            "Diesel backend requires a URL string, not SyncOpts",
                        ));
                    }
                };
                let conn = crate::sync::backend::DieselConn::new(&url)?;

                Ok(Self {
                    inner: RwLock::new(Some(MultiSyncConn::Diesel(conn))),
                })
            }
            _ => Err(crate::error::Error::IncorrectApiUsageError(
                "Unknown backend. Supported backends: 'mysql', 'diesel'",
            )),
        }
    }

    #[pyo3(signature=(consistent_snapshot=false, isolation_level=None, readonly=None))]
    fn start_transaction(
        slf: Py<Self>,
        py: Python,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> PyroResult<SyncTransaction> {
        let isolation_level: Option<mysql::IsolationLevel> =
            isolation_level.map(|l| mysql::IsolationLevel::from(&l));
        let opts = mysql::TxOpts::default()
            .set_with_consistent_snapshot(consistent_snapshot)
            .set_isolation_level(isolation_level)
            .set_access_mode(readonly.map(|flag| {
                if flag {
                    AccessMode::ReadOnly
                } else {
                    AccessMode::ReadWrite
                }
            }));

        Ok(SyncTransaction::new(Either::Left(slf.clone_ref(py)), opts))
    }

    fn id(&self) -> PyroResult<u32> {
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

    #[pyo3(signature = (query))]
    fn query(&self, query: String) -> PyroResult<Vec<Row>> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let conn = match multi_conn {
            MultiSyncConn::Mysql(conn) => &mut conn.inner,
            MultiSyncConn::Diesel(_) => {
                return Err(Error::IncorrectApiUsageError(
                    "Query operations are not yet supported for Diesel backend",
                ))
            }
        };
        Ok(conn.query(query)?)
    }

    #[pyo3(signature = (query))]
    fn query_first(&self, query: String) -> PyroResult<Option<Row>> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let conn = match multi_conn {
            MultiSyncConn::Mysql(conn) => &mut conn.inner,
            MultiSyncConn::Diesel(_) => {
                return Err(Error::IncorrectApiUsageError(
                    "Query operations are not yet supported for Diesel backend",
                ))
            }
        };
        Ok(conn.query_first(query)?)
    }

    #[pyo3(signature = (query))]
    fn query_drop(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let conn = match multi_conn {
            MultiSyncConn::Mysql(conn) => &mut conn.inner,
            MultiSyncConn::Diesel(_) => {
                return Err(Error::IncorrectApiUsageError(
                    "Query operations are not yet supported for Diesel backend",
                ))
            }
        };
        Ok(conn.query_drop(query)?)
    }
    #[pyo3(signature = (query))]
    fn query_iter(slf: Py<Self>, py: Python, query: String) -> PyroResult<ResultSetIterator> {
        let slf_ref = slf.borrow(py);
        let mut guard = slf_ref.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let conn = match multi_conn {
            MultiSyncConn::Mysql(conn) => &mut conn.inner,
            MultiSyncConn::Diesel(_) => {
                return Err(Error::IncorrectApiUsageError(
                    "Query operations are not yet supported for Diesel backend",
                ))
            }
        };
        let query_result = conn.query_iter(query)?;

        Ok(ResultSetIterator {
            owner: slf.clone_ref(py).into_any(),
            inner: Either::Left(unsafe {
                std::mem::transmute::<
                    mysql::QueryResult<'_, '_, '_, mysql::Text>,
                    mysql::QueryResult<'_, '_, '_, mysql::Text>,
                >(query_result)
            }),
        })
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyroResult<Bound<'py, PyList>> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let conn = match multi_conn {
            MultiSyncConn::Mysql(conn) => &mut conn.inner,
            MultiSyncConn::Diesel(_) => {
                return Err(Error::IncorrectApiUsageError(
                    "Exec operations are not yet supported for Diesel backend",
                ))
            }
        };
        // log::debug!("exec {query}");
        Ok(
            conn.exec_fold(query, params, PyList::empty(py), |acc, row| {
                acc.append(mysql::from_row::<Row>(row)).unwrap();
                acc
            })?,
        )
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first(&self, query: String, params: Params) -> PyroResult<Option<Row>> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let conn = match multi_conn {
            MultiSyncConn::Mysql(conn) => &mut conn.inner,
            MultiSyncConn::Diesel(_) => {
                return Err(Error::IncorrectApiUsageError(
                    "Exec operations are not yet supported for Diesel backend",
                ))
            }
        };
        // log::debug!("exec_first {query}");
        Ok(conn.exec_first(query, params)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&self, query: String, params: Params) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let conn = match multi_conn {
            MultiSyncConn::Mysql(conn) => &mut conn.inner,
            MultiSyncConn::Diesel(_) => {
                return Err(Error::IncorrectApiUsageError(
                    "Exec operations are not yet supported for Diesel backend",
                ))
            }
        };
        // log::debug!("exec_drop {query}");
        Ok(conn.exec_drop(query, params)?)
    }

    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&self, query: String, params_list: Vec<Params>) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let multi_conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        let conn = match multi_conn {
            MultiSyncConn::Mysql(conn) => &mut conn.inner,
            MultiSyncConn::Diesel(_) => {
                return Err(Error::IncorrectApiUsageError(
                    "Exec operations are not yet supported for Diesel backend",
                ))
            }
        };
        // log::debug!("exec_batch {query}");
        Ok(conn.exec_batch(query, params_list)?)
    }

    // #[pyo3(signature = (query, params=Params::default()))]
    // fn exec_iter(
    //     slf: Py<Self>,
    //     py: Python,
    //     query: String,
    //     params: Params,
    // ) -> PyroResult<ResultSetIterator> {
    //     let slf_ref = slf.borrow(py);
    //     let mut guard = slf_ref.inner.write();
    //     let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;

    //     log::debug!("exec_iter {query}");
    //     let query_result = conn.exec_iter(query, params)?;
    //     Ok(ResultSetIterator {
    //         owner: slf.clone_ref(py).into_any(),
    //         inner: Either::Right(unsafe {
    //             std::mem::transmute::<
    //                 mysql::QueryResult<'_, '_, '_, mysql::Binary>,
    //                 mysql::QueryResult<'_, '_, '_, mysql::Binary>,
    //             >(query_result)
    //         }),
    //     })
    // }

    pub fn close(&self) {
        *self.inner.write() = None;
    }

    fn reset(&self) -> PyroResult<()> {
        let mut guard = self.inner.write();
        let conn = guard.as_mut().ok_or_else(|| Error::ConnectionClosedError)?;
        conn.reset()
    }

    fn server_version(&self) -> PyroResult<(u16, u16, u16)> {
        let guard = self.inner.read();
        let conn = guard.as_ref().ok_or_else(|| Error::ConnectionClosedError)?;
        Ok(conn.server_version())
    }
}
