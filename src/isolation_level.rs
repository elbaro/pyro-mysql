use pyo3::prelude::*;

#[pyclass(frozen, module = "pyro_mysql")]
#[derive(Clone)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[pymethods]
impl IsolationLevel {
    fn as_str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ_UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ_COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE_READ",
            IsolationLevel::Serializable => "SERIALIZBLE",
        }
    }
}

impl From<&IsolationLevel> for mysql_async::IsolationLevel {
    fn from(value: &IsolationLevel) -> Self {
        match value {
            IsolationLevel::ReadUncommitted => mysql_async::IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted => mysql_async::IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead => mysql_async::IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable => mysql_async::IsolationLevel::Serializable,
        }
    }
}

impl From<&IsolationLevel> for mysql::IsolationLevel {
    fn from(value: &IsolationLevel) -> Self {
        match value {
            IsolationLevel::ReadUncommitted => mysql::IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted => mysql::IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead => mysql::IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable => mysql::IsolationLevel::Serializable,
        }
    }
}
