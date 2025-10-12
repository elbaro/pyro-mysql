use std::collections::VecDeque;

use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};

use crate::{
    dbapi::{conn::DbApiConn, error::Error},
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
    fn execute(&mut self, py: Python, query: &str, params: Params) -> PyResult<()> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| Error::new_err("The cursor is closed"))?
            .borrow(py);
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
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| Error::new_err("The cursor is closed"))?
            .borrow(py);
        conn.exec_batch(query, params)?;
        self.description = None;
        self.result = None;
        self.rowcount = -1;
        Ok(())
    }
    fn fetchone<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        if let Some(result) = &mut self.result {
            if let Some(row) = result.pop_front() {
                Ok(Some(row.to_tuple(py)?))
            } else {
                Ok(None)
            }
        } else {
            Err(Error::new_err(
                "the previous call to .execute*() did not produce any result set or no call was issued yet",
            ))
        }
    }

    #[pyo3(signature=(size=None))]
    fn fetchmany<'py>(
        &mut self,
        py: Python<'py>,
        size: Option<usize>,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let size = size.unwrap_or(self.arraysize);
        if let Some(result) = &mut self.result {
            let mut vec = vec![];
            for row in result.drain(..size) {
                vec.push(row.to_tuple(py)?);
            }
            Ok(vec)
        } else {
            Err(Error::new_err(
                "the previous call to .execute*() did not produce any result set or no call was issued yet",
            ))
        }
    }
    fn fetchall(&mut self) -> PyResult<Vec<Row>> {
        if let Some(result) = self.result.take() {
            self.result = Some(VecDeque::new());
            Ok(Vec::from(result))
        } else {
            Err(Error::new_err(
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
