use std::pin::Pin;

use either::Either;
use mysql::prelude::Queryable;
use mysql::{Transaction, TxOpts};
use pyo3::prelude::*;

use crate::error::{Error, PyroResult};
use crate::sync::backend::MysqlConn;
use crate::sync::iterator::ResultSetIterator;
use crate::sync::multi_conn::MultiSyncConn;
use crate::sync::conn::SyncConn;
use crate::{params::Params, row::Row};

#[pyclass(module = "pyro_mysql.sync", name = "Transaction")]
pub struct SyncTransaction {
    // Hold a reference to the connection Python object to prevent it from being GC-ed
    conn: Py<SyncConn>,
    opts: TxOpts,

    // initialized and reset in __enter__ and __exit__
    inner: Option<mysql::Transaction<'static>>,
    conn_inner: Option<Pin<Box<MysqlConn>>>, // Transaction takes the ownership of the Rust Conn struct from the Python Conn object
}

impl SyncTransaction {
    pub fn new(conn: Py<SyncConn>, opts: TxOpts) -> Self {
        SyncTransaction {
            conn,
            opts,
            inner: None,
            conn_inner: None,
        }
    }
}

#[pymethods]
impl SyncTransaction {
    pub fn __enter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyroResult<Bound<'py, Self>> {
        let slf_ref = slf.borrow();
        let py_conn = &slf_ref.conn;
        let mut conn = {
            let conn_mut: PyRefMut<SyncConn> = py_conn.borrow_mut(py);
            let inner = &conn_mut.inner;
            let multi_conn = py.detach(|| inner.write())
                .take()
                .ok_or_else(|| Error::ConnectionClosedError)?;
            // Extract inner mysql::Conn from MultiSyncConn
            let mysql_conn = match multi_conn {
                MultiSyncConn::Mysql(conn) => conn,
                MultiSyncConn::Diesel(_) => {
                    return Err(Error::IncorrectApiUsageError(
                        "Transactions are not supported for Diesel backend",
                    ))
                }
                MultiSyncConn::ZeroMysql(_) => {
                    return Err(Error::IncorrectApiUsageError(
                        "Transactions are not supported for Zero-MySQL backend",
                    ))
                }
            };
            Box::pin(mysql_conn)
        };

        let tx = conn.inner.start_transaction(slf_ref.opts)?;
        let tx =
            unsafe { std::mem::transmute::<Transaction<'_>, Transaction<'static>>(tx) };

        drop(slf_ref);
        {
            let mut slf_mut = slf.borrow_mut();
            slf_mut.inner = Some(tx);
            slf_mut.conn_inner = Some(conn);
        }
        Ok(slf)
    }

    pub fn __exit__(
        slf: &Bound<'_, Self>,
        py: Python,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyroResult<bool> {
        // Check reference count of the transaction object
        let refcnt = slf.get_refcnt();
        if refcnt != 2 {
            return Err(Error::IncorrectApiUsageError(
                "The transaction is still referenced in __exit__. Make sure not to store the transaction outside the with-clause",
            ));
        }

        let mut slf_mut = slf.borrow_mut();
        // If there's an uncaught exception or transaction wasn't explicitly committed/rolled back, roll back
        if slf_mut.inner.is_some() {
            log::warn!("commit() or 1() is not called. rolling back.");
            slf_mut.rollback()?;
            slf_mut.inner.take();
        }
        let conn_inner = slf_mut.conn_inner.take();
        let py_conn = &slf_mut.conn;
        let conn_mut: PyRefMut<SyncConn> = py_conn.borrow_mut(py);
        let inner = &conn_mut.inner;
        let mysql_conn = *Pin::into_inner(conn_inner.unwrap());
        *py.detach(|| inner.write()) = Some(MultiSyncConn::Mysql(mysql_conn));

        Ok(false) // Don't suppress exceptions
    }

    fn commit(&mut self) -> PyroResult<()> {
        let inner = self
            .inner
            .take()
            .ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("commit");
        Ok(inner.commit()?)
    }

    fn rollback(&mut self) -> PyroResult<()> {
        let inner = self
            .inner
            .take()
            .ok_or_else(|| Error::ConnectionClosedError)?;
        log::debug!("rollback");
        Ok(inner.rollback()?)
    }

    fn affected_rows(&self) -> PyResult<u64> {
        let inner = self.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Connection is not available")
        })?;
        Ok(inner.affected_rows())
    }

    // ─── Text Protocol ───────────────────────────────────────────────────

    #[pyo3(signature = (query, *, as_dict=false))]
    fn query<'py>(&mut self, py: Python<'py>, query: String, as_dict: bool) -> PyroResult<Vec<Py<PyAny>>> {
        let rows: Vec<Row> = self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query(query)?;

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

    #[pyo3(signature = (query, *, as_dict=false))]
    fn query_first<'py>(&mut self, py: Python<'py>, query: String, as_dict: bool) -> PyroResult<Option<Py<PyAny>>> {
        let row: Option<Row> = self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query_first(query)?;

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
            None => Ok(None)
        }
    }

    fn query_drop(&mut self, query: String) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query_drop(query)?)
    }

    fn query_iter(slf: Py<Self>, py: Python, query: String) -> PyroResult<ResultSetIterator> {
        let mut slf_ref = slf.borrow_mut(py);
        let query_result = slf_ref
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query_iter(query)?;

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

    #[pyo3(signature = (query, params=Params::default(), *, as_dict=false))]
    fn exec<'py>(&mut self, py: Python<'py>, query: String, params: Params, as_dict: bool) -> PyroResult<Vec<Py<PyAny>>> {
        log::debug!("exec {query}");
        let rows: Vec<Row> = self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec(query, params.to_mysql_params())?;

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
    #[pyo3(signature = (query, params=Params::default(), *, as_dict=false))]
    fn exec_first<'py>(&mut self, py: Python<'py>, query: String, params: Params, as_dict: bool) -> PyroResult<Option<Py<PyAny>>> {
        log::debug!("exec_first {query}");
        let row: Option<Row> = self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec_first(query, params.to_mysql_params())?;

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
            None => Ok(None)
        }
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&mut self, query: String, params: Params) -> PyroResult<()> {
        log::debug!("exec_drop {query} {params:?}");
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec_drop(query, params.to_mysql_params())?)
    }
    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&mut self, query: String, params_list: Vec<Params>) -> PyroResult<()> {
        log::debug!("exec_batch {query}");
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec_batch(query, params_list)?)
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_iter(
        slf: Py<Self>,
        py: Python,
        query: String,
        params: Params,
    ) -> PyroResult<ResultSetIterator> {
        log::debug!("exec_iter {query}");

        let mut slf_ref = slf.borrow_mut(py);
        let query_result = slf_ref
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec_iter(query, params)?;

        Ok(ResultSetIterator {
            owner: slf.clone_ref(py).into_any(),
            inner: Either::Right(unsafe {
                std::mem::transmute::<
                    mysql::QueryResult<'_, '_, '_, mysql::Binary>,
                    mysql::QueryResult<'_, '_, '_, mysql::Binary>,
                >(query_result)
            }),
        })
    }
}
