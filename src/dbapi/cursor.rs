use std::collections::VecDeque;

use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};

use crate::{
    dbapi::{
        conn::{DbApiConn, DbApiExecResult},
        error::{DbApiError, DbApiResult},
    },
    error::Error,
    params::Params,
    row::Row,
};

#[pyclass(module = "pyro_mysql.dbapi", name = "Cursor")]
pub struct Cursor {
    conn: Option<Py<DbApiConn>>,
    result: Option<VecDeque<Row>>, // TODO: add a lock

    #[pyo3(get, set)]
    arraysize: usize,

    #[pyo3(get)]
    description: Option<Py<PyList>>,

    #[pyo3(get)]
    rowcount: i64,

    #[pyo3(get)]
    lastrowid: Option<u64>,
}

impl Cursor {
    pub fn new(conn: Py<DbApiConn>) -> Self {
        Self {
            conn: Some(conn),
            result: None,
            arraysize: 1,
            description: None,
            rowcount: -1,
            lastrowid: None,
        }
    }
}

#[pymethods]
impl Cursor {
    // TODO: optional
    // fn callproc(&self) {
    //     todo!()
    // }

    /// Closes the cursor. The connection is still alive
    fn close(&mut self) {
        self.conn = None;
        self.result = None;
        self.description = None;
    }

    // TODO: parameter style?
    #[pyo3(signature = (query, params=Params::default()))]
    fn execute(&mut self, py: Python, query: &str, params: Params) -> DbApiResult<()> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .borrow(py);
        // println!("### query: {query}");
        // println!("### params: {params:?}");
        match conn.exec(query, params)? {
            DbApiExecResult::WithDescription {
                rows,
                description,
                affected_rows,
            } => {
                self.description = Some(description);
                self.rowcount = affected_rows as i64;
                self.result = Some(rows.into());
                self.lastrowid = None;
            }
            DbApiExecResult::NoDescription { affected_rows } => {
                self.description = None;
                self.rowcount = affected_rows as i64;
                self.result = None;
                self.lastrowid = conn.last_insert_id()?; // TODO: in multi-threads, exec and last_insert_id() should be called at once
            }
        }

        Ok(())
    }

    fn executemany(&mut self, py: Python, query: &str, params: Vec<Params>) -> DbApiResult<()> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .borrow(py);
        let affected = conn.exec_batch(query, params)?;
        self.description = None;
        self.rowcount = affected as i64;
        self.result = None;
        self.lastrowid = None;
        Ok(())
    }
    fn fetchone<'py>(&mut self, py: Python<'py>) -> DbApiResult<Option<Bound<'py, PyTuple>>> {
        if let Some(result) = &mut self.result {
            if let Some(row) = result.pop_front() {
                Ok(Some(row.to_tuple(py)?))
            } else {
                Ok(None)
            }
        } else {
            Err(DbApiError::no_result_set())
        }
    }

    #[pyo3(signature=(size=None))]
    fn fetchmany<'py>(
        &mut self,
        py: Python<'py>,
        size: Option<usize>,
    ) -> DbApiResult<Vec<Bound<'py, PyTuple>>> {
        let size = size.unwrap_or(self.arraysize);
        if let Some(result) = &mut self.result {
            let mut vec = vec![];
            for row in result.drain(..size.min(result.len())) {
                vec.push(row.to_tuple(py)?);
            }
            Ok(vec)
        } else {
            Err(DbApiError::no_result_set())
        }
    }
    fn fetchall<'py>(&mut self, py: Python<'py>) -> DbApiResult<Vec<Bound<'py, PyTuple>>> {
        if let Some(result) = self.result.take() {
            self.result = Some(VecDeque::new());
            let mut vec = vec![];
            for row in result.into_iter() {
                vec.push(row.to_tuple(py)?);
            }
            Ok(vec)
        } else {
            Err(DbApiError::no_result_set())
        }
    }

    // TODO: optional
    // fn nextset(&self) {}

    // Implementations are free to have this method do nothing and users are free to not use it.
    fn setinputsizes(&self) {}

    // Implementations are free to have this method do nothing and users are free to not use it.
    fn setoutputsize(&self) {}

    // Iterator protocol for PEP 249 compliance
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> DbApiResult<Option<Bound<'py, PyTuple>>> {
        // When fetchone returns Ok(None), PyO3 automatically raises StopIteration
        // When fetchone returns an error (no result set), we propagate it
        self.fetchone(py)
    }
}
