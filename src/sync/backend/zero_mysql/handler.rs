use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use zero_mysql::error::Result;
use zero_mysql::protocol::command::{
    ColumnDefinition, ColumnDefinitionBytes, ColumnDefinitionTail,
};
use zero_mysql::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler};
use zero_mysql::protocol::response::{OkPayload, OkPayloadBytes};
use zero_mysql::protocol::{BinaryRowPayload, TextRowPayload};

use crate::util::PyTupleBuilder;
use crate::zero_mysql_util::{decode_binary_bytes_to_python, decode_text_value_to_python};

pub struct TupleHandler<'a> {
    py: Python<'a>,
    cols: Vec<ColumnDefinitionTail>,
    rows: Py<PyList>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl<'a> TupleHandler<'a> {
    pub fn new(py: Python<'a>) -> Self {
        Self {
            py,
            cols: Vec::new(),
            rows: PyList::empty(py).unbind(),
            affected_rows: 0,
            last_insert_id: 0,
        }
    }

    pub fn into_rows(self) -> Py<PyList> {
        self.rows
    }

    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }
}

impl<'a> BinaryResultSetHandler for TupleHandler<'a> {
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
        let mut bytes = row.values();
        let tuple = PyTupleBuilder::new(self.py, self.cols.len());

        for i in 0..self.cols.len() {
            if row.null_bitmap().is_null(i) {
                tuple.set(i, self.py.None().into_bound(self.py));
            } else {
                let py_value;
                (py_value, bytes) = decode_binary_bytes_to_python(self.py, &self.cols[i], bytes)
                    .map_err(|_e| zero_mysql::error::Error::InvalidPacket)?;
                tuple.set(i, py_value);
            }
        }

        self.rows
            .bind(self.py)
            .append(tuple.build(self.py))
            .unwrap();
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}

impl<'a> TextResultSetHandler for TupleHandler<'a> {
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
        use zero_mysql::protocol::primitive::read_string_lenenc;

        let tuple = PyTupleBuilder::new(self.py, self.cols.len());
        let mut data = row.0;

        for i in 0..self.cols.len() {
            // Check for NULL (0xFB)
            if !data.is_empty() && data[0] == 0xFB {
                tuple.set(i, self.py.None().into_bound(self.py));
                data = &data[1..];
            } else {
                // Read length-encoded string
                let (value_bytes, rest) = read_string_lenenc(data)?;
                let py_value = decode_text_value_to_python(self.py, &self.cols[i], value_bytes)
                    .map_err(|_e| zero_mysql::error::Error::InvalidPacket)?;
                tuple.set(i, py_value);
                data = rest;
            }
        }

        self.rows
            .bind(self.py)
            .append(tuple.build(self.py))
            .unwrap();
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

impl TextResultSetHandler for DropHandler {
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

pub struct DictHandler<'a> {
    py: Python<'a>,
    cols: Vec<ColumnDefinitionTail>,
    col_names: Vec<String>,
    rows: Py<PyList>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl<'a> DictHandler<'a> {
    pub fn new(py: Python<'a>) -> Self {
        Self {
            py,
            cols: Vec::new(),
            col_names: Vec::new(),
            rows: PyList::empty(py).unbind(),
            affected_rows: 0,
            last_insert_id: 0,
        }
    }

    pub fn into_rows(self) -> Py<PyList> {
        self.rows
    }

    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }
}

impl<'a> BinaryResultSetHandler for DictHandler<'a> {
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
        let mut bytes = row.values();
        let dict = PyDict::new(self.py);

        for i in 0..self.cols.len() {
            let py_value = if row.null_bitmap().is_null(i) {
                self.py.None().into_bound(self.py)
            } else {
                let val;
                (val, bytes) = decode_binary_bytes_to_python(self.py, &self.cols[i], bytes)
                    .map_err(|_e| zero_mysql::error::Error::InvalidPacket)?;
                val
            };
            dict.set_item(&self.col_names[i], py_value).unwrap();
        }

        self.rows.bind(self.py).append(dict).unwrap();
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}

impl<'a> TextResultSetHandler for DictHandler<'a> {
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
        use zero_mysql::protocol::primitive::read_string_lenenc;

        let dict = PyDict::new(self.py);
        let mut data = row.0;

        for i in 0..self.cols.len() {
            let py_value = if !data.is_empty() && data[0] == 0xFB {
                // NULL marker
                data = &data[1..];
                self.py.None().into_bound(self.py)
            } else {
                // Read length-encoded string
                let (value_bytes, rest) = read_string_lenenc(data)?;
                let val = decode_text_value_to_python(self.py, &self.cols[i], value_bytes)
                    .map_err(|_e| zero_mysql::error::Error::InvalidPacket)?;
                data = rest;
                val
            };
            dict.set_item(&self.col_names[i], py_value).unwrap();
        }

        self.rows.bind(self.py).append(dict).unwrap();
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}
