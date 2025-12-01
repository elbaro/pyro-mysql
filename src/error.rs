use mysql::consts::ColumnType;
use pyo3::{create_exception, exceptions::PyException, PyErr};
use thiserror::Error;

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
    #[error("{0}")]
    SyncUrlError(#[from] mysql::UrlError),
    #[error("{0}")]
    AsyncUrlError(#[from] mysql_async::UrlError),
    #[error("{0}")]
    SyncError(#[from] mysql::Error),
    #[error("{0}")]
    AsyncError(#[from] mysql_async::Error),

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

    #[error("Wtx Error: {0}")]
    WtxError(String),

    #[error("{0}")]
    ZeroMysqlError(#[from] zero_mysql::error::Error),
    // #[error("")]
    // NetworkTimeoutError(String),
    // #[error("invalid header (expected {expected:?}, found {found:?})")]
    // InvalidHeader { expected: String, found: String },
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
        // TODO: track up sources and append to notes
        match err {
            Error::IncorrectApiUsageError(s) => IncorrectApiUsageError::new_err(s),
            Error::SyncUrlError(url_error) => UrlError::new_err(url_error.to_string()),
            Error::AsyncUrlError(url_error) => UrlError::new_err(url_error.to_string()),
            Error::SyncError(error) => MysqlError::new_err(error.to_string()),
            Error::AsyncError(error) => MysqlError::new_err(error.to_string()),
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
            Error::WtxError(s) => MysqlError::new_err(format!("Wtx Error: {}", s)),
            Error::ZeroMysqlError(e) => MysqlError::new_err(format!("{:?}", e)),
        }
    }
}
