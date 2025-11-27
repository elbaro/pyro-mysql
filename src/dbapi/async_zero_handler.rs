// Async DB-API handler for zero_mysql that captures column metadata (description)
// This handler stores raw bytes during async operation and converts to Python later

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use zero_mysql::constant::ColumnFlags;
use zero_mysql::error::Result;
use zero_mysql::protocol::command::{
    ColumnDefinition, ColumnDefinitionBytes, ColumnDefinitionTail,
};
use zero_mysql::protocol::response::{OkPayload, OkPayloadBytes};
use zero_mysql::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler};
use zero_mysql::protocol::{BinaryRowPayload, TextRowPayload};

use crate::util::PyTupleBuilder;
use crate::zero_mysql_util::{decode_binary_bytes_to_python, decode_text_value_to_python};

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

/// Handler that collects rows and column metadata for async DB-API
/// Stores raw bytes during async operation, converts to Python later
#[derive(Default)]
pub struct AsyncDbApiHandler {
    cols: Vec<ColumnDefinitionTail>,
    col_infos: Vec<ColumnInfo>,
    rows: Vec<RawRow>,
    affected_rows: u64,
    last_insert_id: u64,
    has_result_set: bool,
}

impl AsyncDbApiHandler {
    pub fn clear(&mut self) {
        self.cols.clear();
        self.col_infos.clear();
        self.rows.clear();
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

    /// Build the DB-API description as a PyList
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
        self.rows
            .iter()
            .map(|raw_row| {
                let tuple = PyTupleBuilder::new(py, self.cols.len());

                match raw_row {
                    RawRow::Binary { bytes, is_null } => {
                        let mut bytes_slice = &bytes[..];
                        for (i, &is_null_val) in is_null.iter().enumerate() {
                            if is_null_val {
                                tuple.set(i, py.None().into_bound(py));
                            } else {
                                let py_value;
                                (py_value, bytes_slice) =
                                    decode_binary_bytes_to_python(py, &self.cols[i], bytes_slice)?;
                                tuple.set(i, py_value);
                            }
                        }
                    }
                    RawRow::Text(bytes) => {
                        use zero_mysql::protocol::primitive::read_string_lenenc;
                        let mut data = &bytes[..];

                        for i in 0..self.cols.len() {
                            if !data.is_empty() && data[0] == 0xFB {
                                // NULL marker
                                tuple.set(i, py.None().into_bound(py));
                                data = &data[1..];
                            } else {
                                // Read length-encoded string
                                let (value_bytes, rest) =
                                    read_string_lenenc(data).map_err(|_| {
                                        PyErr::new::<pyo3::exceptions::PyException, _>(
                                            "Failed to read string",
                                        )
                                    })?;
                                let py_value =
                                    decode_text_value_to_python(py, &self.cols[i], value_bytes)?;
                                tuple.set(i, py_value);
                                data = rest;
                            }
                        }
                    }
                }

                Ok(tuple.build(py).unbind())
            })
            .collect()
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

    fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
        log::debug!(
            "AsyncDbApiHandler::resultset_start called with {} columns",
            num_columns
        );
        self.cols.clear();
        self.cols.reserve(num_columns);
        self.col_infos.clear();
        self.col_infos.reserve(num_columns);
        self.has_result_set = true;
        Ok(())
    }

    fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()> {
        log::debug!("AsyncDbApiHandler::col called");
        // Parse full column definition for name and other metadata
        let col_def = ColumnDefinition::try_from(col)?;
        let tail = col_def.tail;

        // Extract column info for description
        let name = String::from_utf8_lossy(col_def.name_alias).to_string();
        let flags = tail.flags()?;
        let null_ok = !flags.contains(ColumnFlags::NOT_NULL_FLAG);

        self.col_infos.push(ColumnInfo {
            name,
            type_code: tail.type_and_flags()?.column_type as u8,
            column_length: tail.column_length(),
            null_ok,
        });

        // Store the full tail for charset info during decoding
        self.cols.push(*tail);

        Ok(())
    }

    fn row(&mut self, row: &BinaryRowPayload) -> Result<()> {
        // Copy the values bytes (only non-NULL values)
        let bytes = row.values().to_vec();

        // Extract null bitmap for each column
        let is_null: Vec<bool> = (0..self.cols.len())
            .map(|i| row.null_bitmap().is_null(i))
            .collect();

        self.rows.push(RawRow::Binary { bytes, is_null });
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

    fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
        log::debug!(
            "AsyncDbApiHandler::resultset_start (text) called with {} columns",
            num_columns
        );
        self.cols.clear();
        self.cols.reserve(num_columns);
        self.col_infos.clear();
        self.col_infos.reserve(num_columns);
        self.has_result_set = true;
        Ok(())
    }

    fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()> {
        log::debug!("AsyncDbApiHandler::col (text) called");
        // Parse full column definition for name and other metadata
        let col_def = ColumnDefinition::try_from(col)?;
        let tail = col_def.tail;

        // Extract column info for description
        let name = String::from_utf8_lossy(col_def.name_alias).to_string();
        let flags = tail.flags()?;
        let null_ok = !flags.contains(ColumnFlags::NOT_NULL_FLAG);

        self.col_infos.push(ColumnInfo {
            name,
            type_code: tail.type_and_flags()?.column_type as u8,
            column_length: tail.column_length(),
            null_ok,
        });

        // Store the full tail for charset info during decoding
        self.cols.push(*tail);

        Ok(())
    }

    fn row(&mut self, row: &TextRowPayload) -> Result<()> {
        let bytes = row.0.to_vec();
        self.rows.push(RawRow::Text(bytes));
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}
