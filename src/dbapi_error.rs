use pyo3::{create_exception, exceptions::PyException};

// important warnings like data truncations while inserting, etc
create_exception!(pyro_mysql.dbapi_error, Warning, PyException);

// catch-all error except warnings
create_exception!(pyro_mysql.dbapi_error, Error, PyException);

// related to the database interface rather than the database itself
create_exception!(pyro_mysql.dbapi_error, InterfaceError, Error);

// division by zero, numeric value out of range, etc
create_exception!(pyro_mysql.dbapi_error, DatabaseError, Error);

create_exception!(pyro_mysql.dbapi_error, DataError, DatabaseError);

// not necessarily under the control of the programmer, e.g. an unexpected disconnect occurs, the data source name is not found, a transaction could not be processed, a memory allocation error occurred during processing, etc
create_exception!(pyro_mysql.dbapi_error, OperationalError, DatabaseError);

// the relational integrity of the database is affected, e.g. a foreign key check fails
create_exception!(pyro_mysql.dbapi_error, IntegrityError, DatabaseError);

// the database encounters an internal error, e.g. the cursor is not valid anymore, the transaction is out of sync, etc
create_exception!(pyro_mysql.dbapi_error, InternalError, DatabaseError);

// table not found or already exists, syntax error in the SQL statement, wrong number of parameters specified, etc
create_exception!(pyro_mysql.dbapi_error, ProgrammingError, DatabaseError);

// a method or database API was used which is not supported by the database
create_exception!(pyro_mysql.dbapi_error, NotSupportedError, DatabaseError);
