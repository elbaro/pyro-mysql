use pyo3::prelude::*;
use pyo3::types::PyList;
use zero_mysql::col::{ColumnDefinitionBytes, ColumnTypeAndFlags};
use zero_mysql::error::Result;
use zero_mysql::protocol::packet::OkPayloadBytes;
use zero_mysql::protocol::r#trait::ResultSetHandler;
use zero_mysql::row::RowPayload;

use crate::util::PyTupleBuilder;
use crate::zero_mysql_util::decode_bytes_to_python;

/// Handler that collects rows as PyTuples
pub struct TupleHandler<'a> {
    py: Python<'a>,
    cols: Vec<ColumnTypeAndFlags>,
    rows: Py<PyList>,
}

impl<'a> TupleHandler<'a> {
    pub fn new(py: Python<'a>) -> Self {
        Self {
            py,
            cols: Vec::new(),
            rows: PyList::empty(py).unbind(),
        }
    }

    pub fn into_rows(self) -> Py<PyList> {
        self.rows
    }
}

impl<'a> ResultSetHandler<'a> for TupleHandler<'a> {
    fn no_result_set(&mut self, _ok: OkPayloadBytes) -> Result<()> {
        Ok(())
    }

    fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
        self.cols.clear();
        self.cols.reserve(num_columns);
        Ok(())
    }

    fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()> {
        self.cols.push(col.tail()?.type_and_flags()?);
        Ok(())
    }

    fn row(&mut self, row: &RowPayload) -> Result<()> {
        let mut bytes = row.values();
        let tuple = PyTupleBuilder::new(self.py, self.cols.len());

        for i in 0..self.cols.len() {
            if row.null_bitmap().is_null(i) {
                tuple.set(i, self.py.None().into_bound(self.py));
            } else {
                let py_value;
                (py_value, bytes) =
                    decode_bytes_to_python(self.py, &self.cols[i], bytes).map_err(|e| {
                        zero_mysql::error::Error::LibraryBug(format!(
                            "Python conversion error: {}",
                            e
                        ))
                    })?;
                tuple.set(i, py_value);
            }
        }

        self.rows
            .bind(self.py)
            .append(tuple.build(self.py))
            .unwrap();
        Ok(())
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> Result<()> {
        Ok(())
    }
}
