use pyo3::types::PyDict;
use pyo3::{prelude::*, types::PyTuple};
use zero_mysql::error::Result;
use zero_mysql::protocol::command::{ColumnDefinition, ColumnDefinitionTail};
use zero_mysql::protocol::response::{OkPayload, OkPayloadBytes};
use zero_mysql::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler};
use zero_mysql::protocol::{BinaryRowPayload, TextRowPayload};

use crate::from_raw_value::PyValue;
use crate::zero_mysql_util::decode_text_value_to_python;
use zero_mysql::raw::parse_value;

enum RawRow {
    Binary { bytes: Vec<u8>, is_null: Vec<bool> },
    Text(Vec<u8>),
}

struct ResultSet {
    cols: Vec<ColumnDefinitionTail>,
    rows: Vec<RawRow>,
}

#[derive(Default)]
pub struct TupleHandler {
    result_sets: Vec<ResultSet>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl TupleHandler {
    pub fn clear(&mut self) {
        self.result_sets.clear();
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
        let mut result = Vec::new();
        for rs in &self.result_sets {
            for raw_row in &rs.rows {
                let mut values = Vec::with_capacity(rs.cols.len());

                match raw_row {
                    RawRow::Binary { bytes, is_null } => {
                        let mut bytes_slice = &bytes[..];
                        for (i, &is_null_val) in is_null.iter().enumerate() {
                            let (py_value, rest) =
                                parse_value::<PyValue>(&rs.cols[i], is_null_val, bytes_slice)
                                    .map_err(|e| {
                                        PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string())
                                    })?;
                            values.push(py_value.0.bind(py).clone());
                            bytes_slice = rest;
                        }
                    }
                    RawRow::Text(bytes) => {
                        use zero_mysql::protocol::primitive::read_string_lenenc;
                        let mut data = &bytes[..];

                        for i in 0..rs.cols.len() {
                            if !data.is_empty() && data[0] == 0xFB {
                                values.push(py.None().into_bound(py));
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
                                values.push(py_value);
                                data = rest;
                            }
                        }
                    }
                }

                result.push(PyTuple::new(py, values)?.unbind());
            }
        }
        Ok(result)
    }
}

impl BinaryResultSetHandler for TupleHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, cols: &[ColumnDefinition<'_>]) -> Result<()> {
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

impl TextResultSetHandler for TupleHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, cols: &[ColumnDefinition<'_>]) -> Result<()> {
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

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, _cols: &[ColumnDefinition<'_>], _row: BinaryRowPayload<'_>) -> Result<()> {
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

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, _cols: &[ColumnDefinition<'_>], _row: TextRowPayload<'_>) -> Result<()> {
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}

struct DictResultSet {
    cols: Vec<ColumnDefinitionTail>,
    col_names: Vec<String>,
    rows: Vec<RawRow>,
}

#[derive(Default)]
pub struct DictHandler {
    result_sets: Vec<DictResultSet>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl DictHandler {
    pub fn clear(&mut self) {
        self.result_sets.clear();
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
        let mut result = Vec::new();
        for rs in &self.result_sets {
            for raw_row in &rs.rows {
                let dict = PyDict::new(py);

                match raw_row {
                    RawRow::Binary { bytes, is_null } => {
                        let mut bytes_slice = &bytes[..];
                        for (i, &is_null_val) in is_null.iter().enumerate() {
                            let (py_value, rest) =
                                parse_value::<PyValue>(&rs.cols[i], is_null_val, bytes_slice)
                                    .map_err(|e| {
                                        PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string())
                                    })?;
                            dict.set_item(&rs.col_names[i], py_value.0.bind(py).clone())?;
                            bytes_slice = rest;
                        }
                    }
                    RawRow::Text(bytes) => {
                        use zero_mysql::protocol::primitive::read_string_lenenc;
                        let mut data = &bytes[..];

                        for i in 0..rs.cols.len() {
                            let py_value = if !data.is_empty() && data[0] == 0xFB {
                                data = &data[1..];
                                py.None().into_bound(py)
                            } else {
                                let (value_bytes, rest) =
                                    read_string_lenenc(data).map_err(|_| {
                                        PyErr::new::<pyo3::exceptions::PyException, _>(
                                            "Failed to read string",
                                        )
                                    })?;
                                let val =
                                    decode_text_value_to_python(py, &rs.cols[i], value_bytes)?;
                                data = rest;
                                val
                            };
                            dict.set_item(&rs.col_names[i], py_value)?;
                        }
                    }
                }

                result.push(dict.unbind());
            }
        }
        Ok(result)
    }
}

impl BinaryResultSetHandler for DictHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, cols: &[ColumnDefinition<'_>]) -> Result<()> {
        self.result_sets.push(DictResultSet {
            cols: cols.iter().map(|c| *c.tail).collect(),
            col_names: cols
                .iter()
                .map(|c| String::from_utf8_lossy(c.name_alias).to_string())
                .collect(),
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

impl TextResultSetHandler for DictHandler {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, cols: &[ColumnDefinition<'_>]) -> Result<()> {
        self.result_sets.push(DictResultSet {
            cols: cols.iter().map(|c| *c.tail).collect(),
            col_names: cols
                .iter()
                .map(|c| String::from_utf8_lossy(c.name_alias).to_string())
                .collect(),
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
