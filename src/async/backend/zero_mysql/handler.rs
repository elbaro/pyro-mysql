use pyo3::types::PyDict;
use pyo3::{prelude::*, types::PyTuple};
use zero_mysql::error::Result;
use zero_mysql::protocol::command::{
    ColumnDefinition, ColumnDefinitionBytes, ColumnDefinitionTail,
};
use zero_mysql::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler};
use zero_mysql::protocol::response::{OkPayload, OkPayloadBytes};
use zero_mysql::protocol::{BinaryRowPayload, TextRowPayload};

use crate::zero_mysql_util::{decode_binary_bytes_to_python, decode_text_value_to_python};

enum RawRow {
    Binary { bytes: Vec<u8>, is_null: Vec<bool> },
    Text(Vec<u8>),
}

pub struct TupleHandler {
    cols: Vec<ColumnDefinitionTail>,
    rows: Vec<RawRow>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl TupleHandler {
    pub fn new() -> Self {
        Self {
            cols: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            last_insert_id: 0,
        }
    }

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
                        for i in 0..self.cols.len() {
                            if is_null[i] {
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

impl<'a> BinaryResultSetHandler for TupleHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
        self.cols.clear();
        self.cols.reserve(num_columns);
        Ok(())
    }

    fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()> {
        self.cols.push(col.tail()?.clone());
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

impl<'a> TextResultSetHandler for TupleHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
        self.cols.clear();
        self.cols.reserve(num_columns);
        Ok(())
    }

    fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()> {
        self.cols.push(col.tail()?.clone());
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

pub struct DropHandler {
    pub affected_rows: u64,
    pub last_insert_id: u64,
}

impl DropHandler {
    pub fn new() -> Self {
        Self {
            affected_rows: 0,
            last_insert_id: 0,
        }
    }
}

impl<'a> BinaryResultSetHandler for DropHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, _num_columns: usize) -> Result<()> {
        Ok(())
    }

    fn col(&mut self, _col: ColumnDefinitionBytes) -> Result<()> {
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

impl<'a> TextResultSetHandler for DropHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, _num_columns: usize) -> Result<()> {
        Ok(())
    }

    fn col(&mut self, _col: ColumnDefinitionBytes) -> Result<()> {
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

pub struct DictHandler {
    cols: Vec<ColumnDefinitionTail>,
    col_names: Vec<String>,
    rows: Vec<RawRow>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl DictHandler {
    pub fn new() -> Self {
        Self {
            cols: Vec::new(),
            col_names: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            last_insert_id: 0,
        }
    }

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
                        for i in 0..self.cols.len() {
                            let py_value = if is_null[i] {
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

impl<'a> BinaryResultSetHandler for DictHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
        self.cols.clear();
        self.cols.reserve(num_columns);
        self.col_names.clear();
        self.col_names.reserve(num_columns);
        Ok(())
    }

    fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()> {
        let col_def = ColumnDefinition::try_from(col)?;
        self.col_names
            .push(String::from_utf8_lossy(col_def.name).to_string());
        self.cols.push(col_def.tail.clone());
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

impl<'a> TextResultSetHandler for DictHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, num_columns: usize) -> Result<()> {
        self.cols.clear();
        self.cols.reserve(num_columns);
        self.col_names.clear();
        self.col_names.reserve(num_columns);
        Ok(())
    }

    fn col(&mut self, col: ColumnDefinitionBytes) -> Result<()> {
        let col_def = ColumnDefinition::try_from(col)?;
        self.col_names
            .push(String::from_utf8_lossy(col_def.name).to_string());
        self.cols.push(col_def.tail.clone());
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
