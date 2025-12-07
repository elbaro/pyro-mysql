// DB-API handler for zero_mysql that captures column metadata (description)

use pyo3::prelude::*;
use pyo3::types::PyList;
use zero_mysql::constant::ColumnFlags;
use zero_mysql::error::Result;
use zero_mysql::protocol::BinaryRowPayload;
use zero_mysql::protocol::command::{ColumnDefinition, ColumnDefinitionTail};
use zero_mysql::protocol::response::{OkPayload, OkPayloadBytes};
use zero_mysql::protocol::r#trait::BinaryResultSetHandler;
use zero_mysql::raw::parse_value;

use crate::dbapi::conn::{DbApiExecResult, DbApiRow};
use crate::from_raw_value::PyValue;
use crate::util::PyTupleBuilder;

/// Column info for building DB-API description
struct ColumnInfo {
    name: String,
    type_code: u8,
    column_length: u32,
    null_ok: bool,
}

/// Handler that collects rows and column metadata for DB-API
pub struct DbApiHandler<'a> {
    py: Python<'a>,
    cols: Vec<ColumnDefinitionTail>,
    col_infos: Vec<ColumnInfo>,
    rows: Vec<DbApiRow>,
    affected_rows: u64,
    last_insert_id: u64,
    has_result_set: bool,
}

impl<'a> DbApiHandler<'a> {
    pub fn new(py: Python<'a>) -> Self {
        Self {
            py,
            cols: Vec::new(),
            col_infos: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            last_insert_id: 0,
            has_result_set: false,
        }
    }

    pub fn into_result(self) -> DbApiExecResult {
        log::debug!(
            "DbApiHandler::into_result: has_result_set={}, rows={}, cols={}",
            self.has_result_set,
            self.rows.len(),
            self.col_infos.len()
        );
        if self.has_result_set {
            // Build description as PyList
            let description = Python::attach(|py| {
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
            })
            .expect("Failed to create description");

            DbApiExecResult::WithDescription {
                rows: self.rows,
                description,
                affected_rows: self.affected_rows,
            }
        } else {
            DbApiExecResult::NoDescription {
                affected_rows: self.affected_rows,
                last_insert_id: self.last_insert_id,
            }
        }
    }
}

impl<'a> BinaryResultSetHandler for DbApiHandler<'a> {
    fn no_result_set(&mut self, ok: OkPayloadBytes) -> Result<()> {
        log::debug!("DbApiHandler::no_result_set called");
        let ok_payload = OkPayload::try_from(ok)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        self.has_result_set = false;
        Ok(())
    }

    fn resultset_start(&mut self, cols: &[ColumnDefinition<'_>]) -> Result<()> {
        log::debug!(
            "DbApiHandler::resultset_start called with {} columns",
            cols.len()
        );
        self.cols.clear();
        self.cols.reserve(cols.len());
        self.col_infos.clear();
        self.col_infos.reserve(cols.len());
        self.has_result_set = true;

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

            self.cols.push(*tail);
        }

        Ok(())
    }

    fn row(&mut self, _cols: &[ColumnDefinition<'_>], row: BinaryRowPayload<'_>) -> Result<()> {
        let mut bytes = row.values();
        let tuple = PyTupleBuilder::new(self.py, self.cols.len());

        for i in 0..self.cols.len() {
            let is_null = row.null_bitmap().is_null(i);
            let (py_value, rest) = parse_value::<PyValue>(&self.cols[i], is_null, bytes)?;
            tuple.set(i, py_value.0.bind(self.py).clone());
            bytes = rest;
        }

        self.rows.push(DbApiRow(tuple.build(self.py).unbind()));
        Ok(())
    }

    fn resultset_end(&mut self, eof: OkPayloadBytes) -> Result<()> {
        let ok_payload = OkPayload::try_from(eof)?;
        self.affected_rows = ok_payload.affected_rows;
        self.last_insert_id = ok_payload.last_insert_id;
        Ok(())
    }
}
