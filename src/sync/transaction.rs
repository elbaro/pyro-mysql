use std::pin::Pin;

use either::Either;
use mysql::prelude::Queryable;
use mysql::{Transaction, TxOpts};
use pyo3::prelude::*;

use crate::error::{Error, PyroResult};
use crate::sync::iterator::ResultSetIterator;
use crate::sync::{conn::SyncConn, pooled_conn::SyncPooledConn};
use crate::{params::Params, row::Row};

#[pyclass(module = "pyro_mysql.sync", name = "Transaction")]
pub struct SyncTransaction {
    // Hold a reference to the connection Python object to prevent it from being GC-ed
    conn: Either<Py<SyncConn>, Py<SyncPooledConn>>,
    opts: TxOpts,

    // initialized and reset in __enter__ and __exit__
    inner: Option<mysql::Transaction<'static>>,
    conn1: Option<Pin<Box<mysql::Conn>>>, // Transaction takes the ownership of the Rust Conn struct from the Python Conn object
    conn2: Option<Pin<Box<mysql::PooledConn>>>,
}

impl SyncTransaction {
    pub fn new(conn: Either<Py<SyncConn>, Py<SyncPooledConn>>, opts: TxOpts) -> Self {
        SyncTransaction {
            conn,
            opts,
            inner: None,
            conn1: None,
            conn2: None,
        }
    }
}

#[pymethods]
impl SyncTransaction {
    pub fn __enter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyroResult<Bound<'py, Self>> {
        let slf_ref = slf.borrow();
        let (conn1, conn2, tx) = match &slf_ref.conn {
            Either::Left(py_conn) => {
                let mut conn = {
                    let conn_mut = py_conn.borrow_mut(py);
                    let inner = &conn_mut.inner;
                    Box::pin(
                        py.detach(|| inner.write())
                            .take()
                            .ok_or_else(|| Error::ConnectionClosedError)?,
                    )
                };

                let tx = conn.start_transaction(slf_ref.opts)?;
                let tx =
                    unsafe { std::mem::transmute::<Transaction<'_>, Transaction<'static>>(tx) };
                (Some(conn), None, tx)
            }
            Either::Right(py_conn) => {
                let mut conn = {
                    let conn_mut = py_conn.borrow_mut(py);
                    let inner = &conn_mut.inner;
                    Box::pin(
                        py.detach(|| inner.write())
                            .take()
                            .ok_or_else(|| Error::ConnectionClosedError)?,
                    )
                };

                let tx = conn.start_transaction(slf_ref.opts)?;
                let tx =
                    unsafe { std::mem::transmute::<Transaction<'_>, Transaction<'static>>(tx) };
                (None, Some(conn), tx)
            }
        };
        drop(slf_ref);
        {
            let mut slf_mut = slf.borrow_mut();
            slf_mut.inner = Some(tx);
            slf_mut.conn1 = conn1;
            slf_mut.conn2 = conn2;
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
        let conn1 = slf_mut.conn1.take();
        let conn2 = slf_mut.conn2.take();
        match &slf_mut.conn {
            Either::Left(py_conn) => {
                let conn_mut = py_conn.borrow_mut(py);
                let inner = &conn_mut.inner;
                *py.detach(|| inner.write()) = Some(*Pin::into_inner(conn1.unwrap()));
            }
            Either::Right(py_conn) => {
                let conn_mut = py_conn.borrow_mut(py);
                let inner = &conn_mut.inner;
                *py.detach(|| inner.write()) = Some(*Pin::into_inner(conn2.unwrap()));
            }
        }
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

    fn query(&mut self, query: String) -> PyroResult<Vec<Row>> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query(query)?)
    }

    fn query_first(&mut self, query: String) -> PyroResult<Option<Row>> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query_first(query)?)
    }

    fn query_drop(&mut self, query: String) -> PyroResult<()> {
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .query_drop(query)?)
    }

    fn query_iter(slf: Py<Self>, query: String) -> PyroResult<ResultSetIterator> {
        Python::attach(|py| {
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
        })
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec(&mut self, query: String, params: Params) -> PyroResult<Vec<Row>> {
        log::debug!("exec {query}");
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec(query, params.inner)?)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_first(&mut self, query: String, params: Params) -> PyroResult<Option<Row>> {
        log::debug!("exec_first {query}");
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec_first(query, params.inner)?)
    }
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&mut self, query: String, params: Params) -> PyroResult<()> {
        log::debug!("exec_drop {query} {params:?}");
        Ok(self
            .inner
            .as_mut()
            .ok_or_else(|| Error::TransactionClosedError)?
            .exec_drop(query, params.inner)?)
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
    fn exec_iter(slf: Py<Self>, query: String, params: Params) -> PyroResult<ResultSetIterator> {
        log::debug!("exec_iter {query}");
        Python::attach(|py| {
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
        })
    }
}
