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
            crate::error::Error::SyncError(error) => Error::new_err(error.to_string()),
            crate::error::Error::AsyncError(error) => Error::new_err(error.to_string()),
            crate::error::Error::ConnectionClosedError => Error::new_err(err.to_string()),
            crate::error::Error::TransactionClosedError => Error::new_err(err.to_string()),
            crate::error::Error::BuilderConsumedError => Error::new_err(err.to_string()),
            crate::error::Error::PythonCancelledError => {
                pyo3::exceptions::asyncio::CancelledError::new_err(())
            }
            crate::error::Error::DecodeError { .. } => Error::new_err(err.to_string()),
            crate::error::Error::PoisonError(s) => Error::new_err(s),
        })
    }
}

impl From<PyErr> for DbApiError {
    fn from(value: PyErr) -> Self {
        Self(value)
    }
}

impl From<DbApiError> for PyErr {
    fn from(value: DbApiError) -> Self {
        value.0
    }
}
