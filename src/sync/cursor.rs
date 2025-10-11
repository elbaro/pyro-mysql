use std::collections::VecDeque;

use pyo3::{prelude::*, types::PyList};

use crate::{dbapi_error, params::Params, row::Row, sync::SyncDbApiConn};

#[pyclass]
pub struct SyncCursor {
    conn: Py<SyncDbApiConn>,
    result: Option<VecDeque<Row>>, // TODO: add a lock

    #[pyo3(get, set)]
    arraysize: usize,

    // ─── Pep249 size
    #[pyo3(get)]
    description: Option<Py<PyList>>,

    #[pyo3(get)]
    rowcount: i64,
}

impl SyncCursor {
    pub fn new(conn: Py<SyncDbApiConn>) -> Self {
        Self {
            conn,
            result: None,
            arraysize: 1,
            description: None,
            rowcount: -1,
        }
    }
}

#[pymethods]
impl SyncCursor {
    // TODO: optional
    // fn callproc(&self) {
    //     todo!()
    // }

    fn close(&self, py: Python) {
        let conn = self.conn.borrow(py);
        conn.close();
    }

    // TODO: parameter style?
    fn execute(&mut self, py: Python, query: &str, params: Params) -> PyResult<()> {
        let conn = self.conn.borrow(py);
        let rows = conn.exec(query, params)?;
        if rows.is_empty() {
            self.rowcount = 0;
            self.result = None;
            self.description = None;
        } else {
            self.description = Some(
                PyList::new(
                    py,
                    rows[0].inner.columns_ref().iter().map(|col|
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
                )?
                .unbind(),
            );
            self.rowcount = rows.len() as i64;
            self.result = Some(rows.into());
        }
        Ok(())
    }
    fn executemany(&mut self, py: Python, query: &str, params: Vec<Params>) -> PyResult<()> {
        let conn = self.conn.borrow(py);
        conn.exec_batch(query, params)?;
        self.result = None;
        self.rowcount = -1;
        Ok(())
    }
    fn fetchone(&mut self) -> PyResult<Option<Row>> {
        if let Some(result) = &mut self.result {
            Ok(result.pop_front())
        } else {
            Err(dbapi_error::Error::new_err(
                "the previous call to .execute*() did not produce any result set or no call was issued yet",
            ))
        }
    }

    #[pyo3(signature=(size=None))]
    fn fetchmany(&mut self, size: Option<usize>) -> PyResult<Vec<Row>> {
        let size = size.unwrap_or(self.arraysize);
        if let Some(result) = &mut self.result {
            Ok(result.drain(..size).collect())
        } else {
            Err(dbapi_error::Error::new_err(
                "the previous call to .execute*() did not produce any result set or no call was issued yet",
            ))
        }
    }
    fn fetchall(&mut self) -> PyResult<Vec<Row>> {
        if let Some(result) = self.result.take() {
            self.result = Some(VecDeque::new());
            Ok(Vec::from(result))
        } else {
            Err(dbapi_error::Error::new_err(
                "the previous call to .execute*() did not produce any result set or no call was issued yet",
            ))
        }
    }

    // TODO: optional
    // fn nextset(&self) {}

    // Implementations are free to have this method do nothing and users are free to not use it.
    fn setinputsizes(&self) {}

    // Implementations are free to have this method do nothing and users are free to not use it.
    fn setoutputsize(&self) {}
}
