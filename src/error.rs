use pyo3::{PyErr, create_exception, exceptions::PyException};
use thiserror::Error;
use zero_mysql::constant::ColumnType;

pub type PyroResult<T> = std::result::Result<T, Error>;

create_exception!(pyro_mysql.error, IncorrectApiUsageError, PyException);
create_exception!(pyro_mysql.error, UrlError, PyException);
create_exception!(pyro_mysql.error, MysqlError, PyException);
create_exception!(pyro_mysql.error, ConnectionClosedError, PyException);
create_exception!(pyro_mysql.error, TransactionClosedError, PyException);
create_exception!(pyro_mysql.error, BuilderConsumedError, PyException);
create_exception!(pyro_mysql.error, DecodeError, PyException);
create_exception!(pyro_mysql.error, PoisonError, PyException);
create_exception!(pyro_mysql.error, PythonObjectCreationError, PyException);

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    IncorrectApiUsageError(&'static str),

    #[error("Connection is already closed")]
    ConnectionClosedError,
    #[error("Transaction is already closed")]
    TransactionClosedError,
    #[error("Builder is already consumed")]
    BuilderConsumedError,

    #[error("The future is cancelled")]
    PythonCancelledError,

    #[error("The lock is poisoned: {0}")]
    PoisonError(String),

    #[error(
        "Failed to decode the received value: ColumnType = {column_type:?}, encoded = {encoded}"
    )]
    DecodeError {
        column_type: ColumnType,
        encoded: String,
    },

    #[error("Failed to create a new Python object: {0}")]
    PythonObjectCreationError(#[from] PyErr),

    #[error("IO Error: {0}")]
    IoError(String),

    #[error("{0}")]
    ZeroMysqlError(#[from] zero_mysql::error::Error),
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(value: std::sync::PoisonError<T>) -> Self {
        Self::PoisonError(value.to_string())
    }
}

impl Error {
    pub fn decode_error(column_type: ColumnType, value: impl std::fmt::Debug) -> Self {
        Self::DecodeError {
            column_type,
            encoded: format!("{:?}", value),
        }
    }
}

impl From<Error> for pyo3::PyErr {
    fn from(err: Error) -> Self {
        match err {
            Error::IncorrectApiUsageError(s) => IncorrectApiUsageError::new_err(s),
            Error::ConnectionClosedError => ConnectionClosedError::new_err(err.to_string()),
            Error::TransactionClosedError => TransactionClosedError::new_err(err.to_string()),
            Error::BuilderConsumedError => BuilderConsumedError::new_err(err.to_string()),
            Error::PythonCancelledError => pyo3::exceptions::asyncio::CancelledError::new_err(()),
            Error::DecodeError { .. } => DecodeError::new_err(err.to_string()),
            Error::PoisonError(s) => PoisonError::new_err(s),
            Error::PythonObjectCreationError(e) => {
                PythonObjectCreationError::new_err(e.to_string())
            }
            Error::IoError(s) => MysqlError::new_err(format!("IO Error: {}", s)),
            Error::ZeroMysqlError(e) => MysqlError::new_err(format!("{:?}", e)),
        }
    }
}
