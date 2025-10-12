use std::collections::VecDeque;

use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};

use crate::{
    dbapi::{
        conn::DbApiConn,
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
}

impl Cursor {
    pub fn new(conn: Py<DbApiConn>) -> Self {
        Self {
            conn: Some(conn),
            result: None,
            arraysize: 1,
            description: None,
            rowcount: -1,
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

        if let Some((rows, description)) = conn.exec(query, params)? {
            self.description = Some(description);
            self.rowcount = rows.len() as i64;
            self.result = Some(rows.into());
        } else {
            self.rowcount = -1;
            self.result = None;
            self.description = None;
        }
        Ok(())
    }

    fn executemany(&mut self, py: Python, query: &str, params: Vec<Params>) -> DbApiResult<()> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .borrow(py);
        conn.exec_batch(query, params)?;
        self.rowcount = -1;
        self.result = None;
        self.description = None;
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
            for row in result.drain(..size) {
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

    // ─── Optional Extensions For Sqlalchemy ──────────────────────────────
    #[getter]
    fn lastrowid(&self, py: Python) -> DbApiResult<Option<u64>> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .borrow(py);
        conn.last_insert_id()
    }
}
