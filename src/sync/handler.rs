use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use zero_mysql::error::Result;
use zero_mysql::protocol::command::ColumnDefinition;
use zero_mysql::protocol::response::{OkPayload, OkPayloadBytes};
use zero_mysql::protocol::r#trait::{BinaryResultSetHandler, TextResultSetHandler};
use zero_mysql::protocol::{BinaryRowPayload, TextRowPayload};

use crate::from_raw_value::PyValue;
use crate::util::PyTupleBuilder;
use crate::zero_mysql_util::decode_text_value_to_python;
use zero_mysql::raw::parse_value;

pub struct TupleHandler<'py> {
    py: Python<'py>,
    rows: Py<PyList>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl<'py> TupleHandler<'py> {
    pub fn new(py: Python<'py>) -> Self {
        Self {
            py,
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

impl<'py> BinaryResultSetHandler for TupleHandler<'py> {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, cols: &[ColumnDefinition<'_>], row: BinaryRowPayload<'_>) -> Result<()> {
        let mut bytes = row.values();
        let tuple = PyTupleBuilder::new(self.py, cols.len());

        for (i, col) in cols.iter().enumerate() {
            let is_null = row.null_bitmap().is_null(i);
            let (py_value, rest) = parse_value::<PyValue>(col.tail, is_null, bytes)?;
            tuple.set(i, py_value.0.bind(self.py).clone());
            bytes = rest;
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

impl<'py> TextResultSetHandler for TupleHandler<'py> {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, cols: &[ColumnDefinition<'_>], row: TextRowPayload<'_>) -> Result<()> {
        use zero_mysql::protocol::primitive::read_string_lenenc;

        let tuple = PyTupleBuilder::new(self.py, cols.len());
        let mut data = row.0;

        for (i, col) in cols.iter().enumerate() {
            if !data.is_empty() && data[0] == 0xFB {
                tuple.set(i, self.py.None().into_bound(self.py));
                data = &data[1..];
            } else {
                let (value_bytes, rest) = read_string_lenenc(data)?;
                let py_value = decode_text_value_to_python(self.py, col.tail, value_bytes)
                    .map_err(|e| zero_mysql::error::Error::LibraryBug(e.into()))?;
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

pub struct DictHandler<'py> {
    py: Python<'py>,
    rows: Py<PyList>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl<'py> DictHandler<'py> {
    pub fn new(py: Python<'py>) -> Self {
        Self {
            py,
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

impl<'py> BinaryResultSetHandler for DictHandler<'py> {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, cols: &[ColumnDefinition<'_>], row: BinaryRowPayload<'_>) -> Result<()> {
        let mut bytes = row.values();
        let dict = PyDict::new(self.py);

        for (i, col) in cols.iter().enumerate() {
            let is_null = row.null_bitmap().is_null(i);
            let (py_value, rest) = parse_value::<PyValue>(col.tail, is_null, bytes)?;
            dict.set_item(
                std::str::from_utf8(col.name_alias).unwrap_or(""),
                py_value.0.bind(self.py).clone(),
            )
            .unwrap();
            bytes = rest;
        }

        self.rows.bind(self.py).append(dict).expect("OOM");
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}

impl<'py> TextResultSetHandler for DictHandler<'py> {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows += ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }

    fn resultset_start(&mut self, _cols: &[ColumnDefinition<'_>]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, cols: &[ColumnDefinition<'_>], row: TextRowPayload<'_>) -> Result<()> {
        use zero_mysql::protocol::primitive::read_string_lenenc;

        let dict = PyDict::new(self.py);
        let mut data = row.0;

        for col in cols.iter() {
            let py_value = if !data.is_empty() && data[0] == 0xFB {
                data = &data[1..];
                self.py.None().into_bound(self.py)
            } else {
                let (value_bytes, rest) = read_string_lenenc(data)?;
                let val = decode_text_value_to_python(self.py, col.tail, value_bytes)
                    .map_err(|e| zero_mysql::error::Error::LibraryBug(e.into()))?;
                data = rest;
                val
            };
            dict.set_item(std::str::from_utf8(col.name_alias).unwrap_or(""), py_value)
                .unwrap();
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
