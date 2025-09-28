use mysql::consts::ColumnType;
use pyo3::{create_exception, exceptions::PyException};
use thiserror::Error;

pub type PyroResult<T> = std::result::Result<T, Error>;

create_exception!(pyro_mysql.error, IncorrectApiUsageError, PyException);
create_exception!(pyro_mysql.error, UrlError, PyException);
create_exception!(pyro_mysql.error, ConnectionClosedError, PyException);
create_exception!(pyro_mysql.error, TransactionClosedError, PyException);
create_exception!(pyro_mysql.error, DecodeError, PyException);

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

    #[error("The future is cancelled")]
    PythonCancelledError,

    #[error(
        "Failed to decode the received value: ColumnType = {column_type:?}, encoded = {encoded}"
    )]
    DecodeError {
        column_type: ColumnType,
        encoded: String,
    },
    // #[error("")]
    // NetworkTimeoutError(String),
    // #[error("invalid header (expected {expected:?}, found {found:?})")]
    // InvalidHeader { expected: String, found: String },
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
            Error::IncorrectApiUsageError(s) => IncorrectApiUsageError::new_err(s).into(),
            Error::SyncUrlError(url_error) => UrlError::new_err(url_error.to_string()).into(),
            Error::AsyncUrlError(url_error) => UrlError::new_err(url_error.to_string()).into(),
            Error::SyncError(error) => UrlError::new_err(error.to_string()).into(),
            Error::AsyncError(error) => UrlError::new_err(error.to_string()).into(),
            Error::ConnectionClosedError => ConnectionClosedError::new_err(err.to_string()).into(),
            Error::TransactionClosedError => {
                TransactionClosedError::new_err(err.to_string()).into()
            }
            Error::PythonCancelledError => pyo3::exceptions::asyncio::CancelledError::new_err(()),
            Error::DecodeError { .. } => DecodeError::new_err(err.to_string()).into(),
        }
    }
}
