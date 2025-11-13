use pyo3::{prelude::*, types::PyTuple};
use zero_mysql::col::{ColumnDefinitionBytes, ColumnTypeAndFlags};
use zero_mysql::error::Result;
use zero_mysql::protocol::packet::OkPayloadBytes;
use zero_mysql::protocol::r#trait::ResultSetHandler;
use zero_mysql::protocol::value::Value;
use zero_mysql::row::RowPayload;

use crate::zero_mysql_util::zero_mysql_value_to_python;

/// Raw row data collected during async operation
/// Stores the entire row bytes with null information to be parsed later with GIL
pub struct RawRow {
    /// Raw bytes containing all column values
    pub bytes: Vec<u8>,
    /// Boolean vector indicating which columns are null
    pub is_null: Vec<bool>,
}

/// Handler that collects rows as raw byte data (not Python objects yet)
pub struct TupleHandler {
    cols: Vec<ColumnTypeAndFlags>,
    rows: Vec<RawRow>,
}

impl TupleHandler {
    pub fn new() -> Self {
        Self {
            cols: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Convert collected raw rows to Python tuples
    /// This must be called with the GIL held, after the async operation completes
    pub fn into_py_rows(self, py: Python) -> PyResult<Vec<Py<PyTuple>>> {
        self.rows
            .into_iter()
            .map(|raw_row| {
                let mut values = Vec::with_capacity(self.cols.len());
                let mut bytes = &raw_row.bytes[..];

                // Parse each column from the raw bytes
                for i in 0..self.cols.len() {
                    if raw_row.is_null[i] {
                        values.push(py.None().into_bound(py));
                    } else {
                        let value;
                        (value, bytes) = Value::parse(&self.cols[i], bytes).map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                                "Failed to parse value: {}",
                                e
                            ))
                        })?;

                        let py_value = zero_mysql_value_to_python(py, value)?;
                        values.push(py_value);
                    }
                }

                Ok(PyTuple::new(py, values)?.unbind())
            })
            .collect()
    }
}

impl<'a> ResultSetHandler<'a> for TupleHandler {
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
        // Copy the entire row bytes
        let bytes = row.values().to_vec();

        // Extract null information for each column
        let is_null: Vec<bool> = (0..self.cols.len())
            .map(|i| row.null_bitmap().is_null(i))
            .collect();

        self.rows.push(RawRow { bytes, is_null });
        Ok(())
    }

    fn resultset_end(&mut self, _eof: OkPayloadBytes) -> Result<()> {
        Ok(())
    }
}
