use pyo3::{PyErr, create_exception, exceptions::PyException};

// important warnings like data truncations while inserting, etc
create_exception!(pyro_mysql.dbapi, Warning, PyException);

// catch-all error except warnings
create_exception!(pyro_mysql.dbapi, Error, PyException);

// related to the database interface rather than the database itself
create_exception!(pyro_mysql.dbapi, InterfaceError, Error);

// division by zero, numeric value out of range, etc
create_exception!(pyro_mysql.dbapi, DatabaseError, Error);

create_exception!(pyro_mysql.dbapi, DataError, DatabaseError);

// not necessarily under the control of the programmer, e.g. an unexpected disconnect occurs, the data source name is not found, a transaction could not be processed, a memory allocation error occurred during processing, etc
create_exception!(pyro_mysql.dbapi, OperationalError, DatabaseError);

// the relational integrity of the database is affected, e.g. a foreign key check fails
create_exception!(pyro_mysql.dbapi, IntegrityError, DatabaseError);

// the database encounters an internal error, e.g. the cursor is not valid anymore, the transaction is out of sync, etc
create_exception!(pyro_mysql.dbapi, InternalError, DatabaseError);

// table not found or already exists, syntax error in the SQL statement, wrong number of parameters specified, etc
create_exception!(pyro_mysql.dbapi, ProgrammingError, DatabaseError);

// a method or database API was used which is not supported by the database
create_exception!(pyro_mysql.dbapi, NotSupportedError, DatabaseError);

pub struct DbApiError(PyErr);

impl DbApiError {
    pub fn no_result_set() -> Self {
        Self(Error::new_err(
            "the previous call to .execute*() did not produce any result set or no call was issued yet",
        ))
    }
}

pub type DbApiResult<T> = std::result::Result<T, DbApiError>;

impl From<crate::error::Error> for DbApiError {
    fn from(err: crate::error::Error) -> Self {
        Self(match err {
            crate::error::Error::IncorrectApiUsageError(s) => Error::new_err(s),
            crate::error::Error::SyncUrlError(url_error) => Error::new_err(url_error.to_string()),
            crate::error::Error::AsyncUrlError(url_error) => Error::new_err(url_error.to_string()),
            crate::error::Error::SyncError(error) => {
                if let mysql::Error::MySqlError(mysql_error) = &error {
                    map_mysql_error_to_dbapi(mysql_error, error.to_string())
                } else {
                    Error::new_err(error.to_string())
                }
            }
            crate::error::Error::AsyncError(error) => {
                if let mysql_async::Error::Server(server_error) = &error {
                    map_mysql_async_error_to_dbapi(server_error, error.to_string())
                } else {
                    Error::new_err(error.to_string())
                }
            }
            crate::error::Error::ConnectionClosedError => Error::new_err(err.to_string()),
            crate::error::Error::TransactionClosedError => Error::new_err(err.to_string()),
            crate::error::Error::BuilderConsumedError => Error::new_err(err.to_string()),
            crate::error::Error::PythonCancelledError => {
                pyo3::exceptions::asyncio::CancelledError::new_err(())
            }
            crate::error::Error::DecodeError { .. } => Error::new_err(err.to_string()),
            crate::error::Error::PoisonError(s) => Error::new_err(s),
            crate::error::Error::PythonObjectCreationError(e) => Error::new_err(e.to_string()),
            crate::error::Error::IoError(s) => Error::new_err(format!("IO Error: {}", s)),
            crate::error::Error::WtxError(s) => Error::new_err(format!("Wtx Error: {}", s)),
            crate::error::Error::ZeroMysqlError(e) => match &e {
                zero_mysql::error::Error::ServerError(err_payload) => map_server_error_to_dbapi(
                    &err_payload.sql_state,
                    err_payload.error_code,
                    e.to_string(),
                ),
                zero_mysql::error::Error::LibraryBug(_) => IntegrityError::new_err(e.to_string()),
                _ => Error::new_err(e.to_string()),
            },
        })
    }
}

fn map_server_error_to_dbapi(state: &str, code: u16, error_msg: String) -> PyErr {
    match state {
        "23000" => IntegrityError::new_err(error_msg),
        "22001" | "22003" | "22007" | "22012" => DataError::new_err(error_msg),
        "42000" | "42S02" | "42S22" => ProgrammingError::new_err(error_msg),
        "28000" | "08004" | "40001" => OperationalError::new_err(error_msg),
        "0A000" => NotSupportedError::new_err(error_msg),
        _ => {
            // Fallback to error code for unmapped SQLSTATEs
            match code {
                // Connection/disconnect related errors should be OperationalError
                1927 | 2006 | 2013 | 2014 | 2045 | 2055 | 4031 => {
                    OperationalError::new_err(error_msg)
                }
                code if code < 1000 => InternalError::new_err(error_msg),
                _ => OperationalError::new_err(error_msg),
            }
        }
    }
}

fn map_mysql_error_to_dbapi(mysql_error: &mysql::MySqlError, error_msg: String) -> PyErr {
    map_server_error_to_dbapi(mysql_error.state.as_str(), mysql_error.code, error_msg)
}

fn map_mysql_async_error_to_dbapi(
    server_error: &mysql_async::ServerError,
    error_msg: String,
) -> PyErr {
    map_server_error_to_dbapi(server_error.state.as_str(), server_error.code, error_msg)
}

impl From<PyErr> for DbApiError {
    fn from(value: PyErr) -> Self {
        Self(value)
    }
}

impl From<tokio::task::JoinError> for DbApiError {
    fn from(value: tokio::task::JoinError) -> Self {
        Self(Error::new_err(value.to_string()))
    }
}

impl From<DbApiError> for PyErr {
    fn from(value: DbApiError) -> Self {
        value.0
    }
}
