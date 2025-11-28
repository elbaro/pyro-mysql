use pyo3::types::PyDict;
use pyo3::{prelude::*, types::PyTuple};
use zero_mysql::error::Result;
use zero_mysql::protocol::command::{ColumnDefinition, ColumnDefinitionTail};
use zero_mysql::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler};
use zero_mysql::protocol::response::{OkPayload, OkPayloadBytes};
use zero_mysql::protocol::{BinaryRowPayload, TextRowPayload};

use crate::zero_mysql_util::{decode_binary_bytes_to_python, decode_text_value_to_python};

enum RawRow {
    Binary { bytes: Vec<u8>, is_null: Vec<bool> },
    Text(Vec<u8>),
}

#[derive(Default)]
pub struct TupleHandler {
    cols: Vec<ColumnDefinitionTail>,
    rows: Vec<RawRow>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl TupleHandler {
    pub fn clear(&mut self) {
        self.cols.clear();
        self.rows.clear();
        self.affected_rows = 0;
        self.last_insert_id = 0;
    }

    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    /// Convert collected raw rows to Python tuples
    /// This must be called with the GIL held, after the async operation completes
    pub fn rows_to_python(&mut self, py: Python) -> PyResult<Vec<Py<PyTuple>>> {
        self.rows
            .iter()
            .map(|raw_row| {
                let mut values = Vec::with_capacity(self.cols.len());

                match raw_row {
                    RawRow::Binary { bytes, is_null } => {
                        // Binary protocol: parse from continuous byte stream with null bitmap
                        let mut bytes_slice = &bytes[..];
                        for (i, &is_null_val) in is_null.iter().enumerate() {
                            if is_null_val {
                                values.push(py.None().into_bound(py));
                            } else {
                                let py_value;
                                (py_value, bytes_slice) =
                                    decode_binary_bytes_to_python(py, &self.cols[i], bytes_slice)?;
                                values.push(py_value);
                            }
                        }
                    }
                    RawRow::Text(bytes) => {
                        // Text protocol: parse length-encoded values with 0xFB NULL markers
                        use zero_mysql::protocol::primitive::read_string_lenenc;
                        let mut data = &bytes[..];

                        for i in 0..self.cols.len() {
                            // Check for NULL (0xFB)
                            if !data.is_empty() && data[0] == 0xFB {
                                values.push(py.None().into_bound(py));
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
                                values.push(py_value);
                                data = rest;
                            }
                        }
                    }
                }

                Ok(PyTuple::new(py, values)?.unbind())
            })
            .collect()
    }
}

impl BinaryResultSetHandler for TupleHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start<'stmt>(&mut self, cols: &'stmt [ColumnDefinition<'stmt>]) -> Result<()> {
        self.cols.clear();
        self.cols.reserve(cols.len());
        for col in cols {
            self.cols.push(*col.tail);
        }
        Ok(())
    }

    fn row(&mut self, row: &BinaryRowPayload) -> Result<()> {
        let bytes = row.values().to_vec();
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

impl TextResultSetHandler for TupleHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start<'stmt>(&mut self, cols: &'stmt [ColumnDefinition<'stmt>]) -> Result<()> {
        self.cols.clear();
        self.cols.reserve(cols.len());
        for col in cols {
            self.cols.push(*col.tail);
        }
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

#[derive(Default)]
pub struct DropHandler {
    pub affected_rows: u64,
    pub last_insert_id: u64,
}

impl BinaryResultSetHandler for DropHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start<'stmt>(&mut self, _cols: &'stmt [ColumnDefinition<'stmt>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, _row: &BinaryRowPayload) -> Result<()> {
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}

impl TextResultSetHandler for DropHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start<'stmt>(&mut self, _cols: &'stmt [ColumnDefinition<'stmt>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, _row: &TextRowPayload) -> Result<()> {
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}

#[derive(Default)]
pub struct DictHandler {
    cols: Vec<ColumnDefinitionTail>,
    col_names: Vec<String>,
    rows: Vec<RawRow>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl DictHandler {
    pub fn clear(&mut self) {
        self.cols.clear();
        self.col_names.clear();
        self.rows.clear();
        self.affected_rows = 0;
        self.last_insert_id = 0;
    }

    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    /// Convert collected raw rows to Python dicts
    /// This must be called with the GIL held, after the async operation completes
    pub fn rows_to_python(&mut self, py: Python) -> PyResult<Vec<Py<PyDict>>> {
        self.rows
            .iter()
            .map(|raw_row| {
                let dict = PyDict::new(py);

                match raw_row {
                    RawRow::Binary { bytes, is_null } => {
                        // Binary protocol: parse from continuous byte stream with null bitmap
                        let mut bytes_slice = &bytes[..];
                        for (i, &is_null_val) in is_null.iter().enumerate() {
                            let py_value = if is_null_val {
                                py.None().into_bound(py)
                            } else {
                                let val;
                                (val, bytes_slice) =
                                    decode_binary_bytes_to_python(py, &self.cols[i], bytes_slice)?;
                                val
                            };
                            dict.set_item(&self.col_names[i], py_value)?;
                        }
                    }
                    RawRow::Text(bytes) => {
                        // Text protocol: parse length-encoded values with 0xFB NULL markers
                        use zero_mysql::protocol::primitive::read_string_lenenc;
                        let mut data = &bytes[..];

                        for i in 0..self.cols.len() {
                            let py_value = if !data.is_empty() && data[0] == 0xFB {
                                // NULL marker
                                data = &data[1..];
                                py.None().into_bound(py)
                            } else {
                                // Read length-encoded string
                                let (value_bytes, rest) =
                                    read_string_lenenc(data).map_err(|_| {
                                        PyErr::new::<pyo3::exceptions::PyException, _>(
                                            "Failed to read string",
                                        )
                                    })?;
                                let val =
                                    decode_text_value_to_python(py, &self.cols[i], value_bytes)?;
                                data = rest;
                                val
                            };
                            dict.set_item(&self.col_names[i], py_value)?;
                        }
                    }
                }

                Ok(dict.unbind())
            })
            .collect()
    }
}

impl BinaryResultSetHandler for DictHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start<'stmt>(&mut self, cols: &'stmt [ColumnDefinition<'stmt>]) -> Result<()> {
        self.cols.clear();
        self.cols.reserve(cols.len());
        self.col_names.clear();
        self.col_names.reserve(cols.len());
        for col in cols {
            self.col_names
                .push(String::from_utf8_lossy(col.name_alias).to_string());
            self.cols.push(*col.tail);
        }
        Ok(())
    }

    fn row(&mut self, row: &BinaryRowPayload) -> Result<()> {
        let bytes = row.values().to_vec();
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

impl TextResultSetHandler for DictHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start<'stmt>(&mut self, cols: &'stmt [ColumnDefinition<'stmt>]) -> Result<()> {
        self.cols.clear();
        self.cols.reserve(cols.len());
        self.col_names.clear();
        self.col_names.reserve(cols.len());
        for col in cols {
            self.col_names
                .push(String::from_utf8_lossy(col.name_alias).to_string());
            self.cols.push(*col.tail);
        }
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
