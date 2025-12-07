// Async DB-API handler for zero_mysql that captures column metadata (description)
// This handler stores raw bytes during async operation and converts to Python later

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use zero_mysql::constant::ColumnFlags;
use zero_mysql::error::Result;
use zero_mysql::protocol::command::{ColumnDefinition, ColumnDefinitionTail};
use zero_mysql::protocol::response::{OkPayload, OkPayloadBytes};
use zero_mysql::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler};
use zero_mysql::protocol::{BinaryRowPayload, TextRowPayload};

use crate::from_raw_value::PyValue;
use crate::util::PyTupleBuilder;
use crate::zero_mysql_util::decode_text_value_to_python;
use zero_mysql::raw::parse_value;

/// Column info for building DB-API description
struct ColumnInfo {
    name: String,
    type_code: u8,
    column_length: u32,
    null_ok: bool,
}

/// Raw row data stored during async operation
enum RawRow {
    Binary { bytes: Vec<u8>, is_null: Vec<bool> },
    Text(Vec<u8>),
}

struct ResultSet {
    cols: Vec<ColumnDefinitionTail>,
    rows: Vec<RawRow>,
}

/// Handler that collects rows and column metadata for async DB-API
/// Stores raw bytes during async operation, converts to Python later
#[derive(Default)]
pub struct AsyncDbApiHandler {
    col_infos: Vec<ColumnInfo>,
    result_sets: Vec<ResultSet>,
    affected_rows: u64,
    last_insert_id: u64,
    has_result_set: bool,
}

impl AsyncDbApiHandler {
    pub fn clear(&mut self) {
        self.col_infos.clear();
        self.result_sets.clear();
        self.affected_rows = 0;
        self.last_insert_id = 0;
        self.has_result_set = false;
    }

    pub fn has_result_set(&self) -> bool {
        self.has_result_set
    }

    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    /// Build the DB-API description as a PyList (uses last result set's columns)
    /// Must be called with the GIL held
    pub fn build_description(&self, py: Python) -> PyResult<Py<PyList>> {
        PyList::new(
            py,
            self.col_infos.iter().map(|info| {
                (
                    info.name.as_str(), // name
                    info.type_code,     // type_code
                    info.column_length, // display_size
                    None::<Option<()>>, // internal_size
                    None::<Option<()>>, // precision
                    None::<Option<()>>, // scale
                    if info.null_ok {
                        Some(true)
                    } else {
                        Some(false)
                    }, // null_ok
                )
                    .into_pyobject(py)
                    .unwrap()
            }),
        )
        .map(|bound| bound.unbind())
    }

    /// Convert collected raw rows to Python tuples
    /// Must be called with the GIL held, after the async operation completes
    pub fn rows_to_python(&self, py: Python) -> PyResult<Vec<Py<PyTuple>>> {
        let mut result = Vec::new();
        for rs in &self.result_sets {
            for raw_row in &rs.rows {
                let tuple = PyTupleBuilder::new(py, rs.cols.len());

                match raw_row {
                    RawRow::Binary { bytes, is_null } => {
                        let mut bytes_slice = &bytes[..];
                        for (i, &is_null_val) in is_null.iter().enumerate() {
                            let (py_value, rest) =
                                parse_value::<PyValue>(&rs.cols[i], is_null_val, bytes_slice)
                                    .map_err(|e| {
                                        PyErr::new::<pyo3::exceptions::PyException, _>(
                                            e.to_string(),
                                        )
                                    })?;
                            tuple.set(i, py_value.0.bind(py).clone());
                            bytes_slice = rest;
                        }
                    }
                    RawRow::Text(bytes) => {
                        use zero_mysql::protocol::primitive::read_string_lenenc;
                        let mut data = &bytes[..];

                        for i in 0..rs.cols.len() {
                            if !data.is_empty() && data[0] == 0xFB {
                                tuple.set(i, py.None().into_bound(py));
                                data = &data[1..];
                            } else {
                                let (value_bytes, rest) =
                                    read_string_lenenc(data).map_err(|_| {
                                        PyErr::new::<pyo3::exceptions::PyException, _>(
                                            "Failed to read string",
                                        )
                                    })?;
                                let py_value =
                                    decode_text_value_to_python(py, &rs.cols[i], value_bytes)?;
                                tuple.set(i, py_value);
                                data = rest;
                            }
                        }
                    }
                }

                result.push(tuple.build(py).unbind());
            }
        }
        Ok(result)
    }
}

impl BinaryResultSetHandler for AsyncDbApiHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        log::debug!("AsyncDbApiHandler::no_result_set called");
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        self.has_result_set = false;
        Ok(())
    }

    fn resultset_start(&mut self, cols: &[ColumnDefinition<'_>]) -> Result<()> {
        log::debug!(
            "AsyncDbApiHandler::resultset_start called with {} columns",
            cols.len()
        );
        self.has_result_set = true;

        // Update col_infos for DB-API description (uses last result set)
        self.col_infos.clear();
        self.col_infos.reserve(cols.len());
        for col in cols {
            let tail = col.tail;
            let name = String::from_utf8_lossy(col.name_alias).to_string();
            let flags = tail.flags()?;
            let null_ok = !flags.contains(ColumnFlags::NOT_NULL_FLAG);

            self.col_infos.push(ColumnInfo {
                name,
                type_code: tail.column_type()? as u8,
                column_length: tail.column_length(),
                null_ok,
            });
        }

        // Push new result set for row storage
        self.result_sets.push(ResultSet {
            cols: cols.iter().map(|c| *c.tail).collect(),
            rows: Vec::new(),
        });

        Ok(())
    }

    fn row(&mut self, _cols: &[ColumnDefinition<'_>], row: BinaryRowPayload<'_>) -> Result<()> {
        let rs = self.result_sets.last_mut().unwrap();
        let is_null: Vec<bool> = (0..rs.cols.len())
            .map(|i| row.null_bitmap().is_null(i))
            .collect();
        rs.rows.push(RawRow::Binary {
            bytes: row.values().to_vec(),
            is_null,
        });
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}

impl TextResultSetHandler for AsyncDbApiHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        log::debug!("AsyncDbApiHandler::no_result_set (text) called");
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        self.has_result_set = false;
        Ok(())
    }

    fn resultset_start(&mut self, cols: &[ColumnDefinition<'_>]) -> Result<()> {
        log::debug!(
            "AsyncDbApiHandler::resultset_start (text) called with {} columns",
            cols.len()
        );
        self.has_result_set = true;

        // Update col_infos for DB-API description (uses last result set)
        self.col_infos.clear();
        self.col_infos.reserve(cols.len());
        for col in cols {
            let tail = col.tail;
            let name = String::from_utf8_lossy(col.name_alias).to_string();
            let flags = tail.flags()?;
            let null_ok = !flags.contains(ColumnFlags::NOT_NULL_FLAG);

            self.col_infos.push(ColumnInfo {
                name,
                type_code: tail.column_type()? as u8,
                column_length: tail.column_length(),
                null_ok,
            });
        }

        // Push new result set for row storage
        self.result_sets.push(ResultSet {
            cols: cols.iter().map(|c| *c.tail).collect(),
            rows: Vec::new(),
        });

        Ok(())
    }

    fn row(&mut self, _cols: &[ColumnDefinition<'_>], row: TextRowPayload<'_>) -> Result<()> {
        let rs = self.result_sets.last_mut().unwrap();
        rs.rows.push(RawRow::Text(row.0.to_vec()));
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}
