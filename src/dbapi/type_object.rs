use pyo3::prelude::*;
use zero_mysql::constant::ColumnType;

#[pyclass(module = "pyro_mysql.dbapi")]
pub struct TypeObject(&'static [ColumnType]);

#[pymethods]
impl TypeObject {
    fn __eq__(&self, other: u8) -> bool {
        self.0.iter().any(|x| (*x as u8) == other)
    }
}

pub const STRING: TypeObject = TypeObject(&[
    ColumnType::MYSQL_TYPE_ENUM,
    ColumnType::MYSQL_TYPE_STRING,
    ColumnType::MYSQL_TYPE_VAR_STRING,
    ColumnType::MYSQL_TYPE_VARCHAR,
    ColumnType::MYSQL_TYPE_SET,
    ColumnType::MYSQL_TYPE_JSON,
]);

pub const BINARY: TypeObject = TypeObject(&[
    ColumnType::MYSQL_TYPE_BLOB,
    ColumnType::MYSQL_TYPE_TINY_BLOB,
    ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
    ColumnType::MYSQL_TYPE_LONG_BLOB,
    ColumnType::MYSQL_TYPE_BIT,
    ColumnType::MYSQL_TYPE_GEOMETRY,
]);

pub const NUMBER: TypeObject = TypeObject(&[
    ColumnType::MYSQL_TYPE_DECIMAL,
    ColumnType::MYSQL_TYPE_NEWDECIMAL,
    ColumnType::MYSQL_TYPE_TINY,
    ColumnType::MYSQL_TYPE_SHORT,
    ColumnType::MYSQL_TYPE_LONG,
    ColumnType::MYSQL_TYPE_LONGLONG,
    ColumnType::MYSQL_TYPE_INT24,
    ColumnType::MYSQL_TYPE_FLOAT,
    ColumnType::MYSQL_TYPE_DOUBLE,
    ColumnType::MYSQL_TYPE_YEAR,
]);

pub const DATETIME: TypeObject = TypeObject(&[
    ColumnType::MYSQL_TYPE_TIMESTAMP,
    ColumnType::MYSQL_TYPE_DATETIME,
    ColumnType::MYSQL_TYPE_DATE,
    ColumnType::MYSQL_TYPE_TIME,
]);

pub const ROWID: TypeObject = TypeObject(&[]);
