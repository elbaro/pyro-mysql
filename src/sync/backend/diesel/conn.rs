use diesel::mysql::data_types::{MysqlTime, MysqlTimestampType};
use diesel::prelude::*;
use diesel::query_builder::{BoxedSqlQuery, SqlQuery};
use diesel::sql_query;
use diesel::sql_types::{
    BigInt, Binary, Double, Float, Nullable, Text, Time as SqlTime, Timestamp, Unsigned,
};
use pyo3::prelude::*;
use std::thread::ThreadId;

use crate::error::{Error, PyroResult};
use crate::params::Params;
use crate::row::{RowDict, RowTuple};

/// Bind parameters to a boxed SQL query
/// This is needed because Diesel's type system requires boxing when the number/types
/// of parameters vary at runtime. Each .bind() call changes the query's type.
pub fn bind_params(
    query_str: String,
    values: Vec<crate::value::Value>,
) -> BoxedSqlQuery<'static, diesel::mysql::Mysql, SqlQuery> {
    let mut boxed_query = diesel::sql_query(query_str).into_boxed();
    for value in values {
        boxed_query = match value {
            crate::value::Value::NULL => boxed_query.bind::<Nullable<Text>, _>(None::<String>),
            crate::value::Value::Bytes(b) => {
                let bytes_vec: Vec<u8> = b.as_ref().to_vec();
                boxed_query.bind::<Binary, _>(bytes_vec)
            }
            crate::value::Value::Str(s) => {
                let str_ref: &str = s.as_ref();
                let string: String = str_ref.to_string();
                boxed_query.bind::<Text, _>(string)
            }
            crate::value::Value::Int(v) => boxed_query.bind::<BigInt, _>(v),
            crate::value::Value::UInt(v) => boxed_query.bind::<Unsigned<BigInt>, _>(v),
            crate::value::Value::Float(v) => boxed_query.bind::<Float, _>(v),
            crate::value::Value::Double(v) => boxed_query.bind::<Double, _>(v),
            crate::value::Value::Date(year, month, day, hour, minute, second, micro) => {
                // Use MysqlTime for diesel Timestamp
                let mysql_time = MysqlTime::new(
                    year as u32,
                    month as u32,
                    day as u32,
                    hour as u32,
                    minute as u32,
                    second as u32,
                    micro as u64,
                    false,
                    MysqlTimestampType::MYSQL_TIMESTAMP_DATETIME,
                    0,
                );
                boxed_query.bind::<Timestamp, _>(mysql_time)
            }
            crate::value::Value::Time(is_negative, days, hours, minutes, seconds, micro) => {
                // Use MysqlTime for diesel Time
                // For TIME type, days are stored in the days field
                let mysql_time = MysqlTime::new(
                    0,
                    0,
                    days,
                    hours as u32,
                    minutes as u32,
                    seconds as u32,
                    micro as u64,
                    is_negative,
                    MysqlTimestampType::MYSQL_TIMESTAMP_TIME,
                    0,
                );
                boxed_query.bind::<SqlTime, _>(mysql_time)
            }
        };
    }
    boxed_query
}

/// Diesel synchronous connection wrapper
pub struct DieselConn {
    inner: MysqlConnection,
    // Track last statement stats
    affected_rows: u64,
    last_insert_id: Option<u64>,
    // Thread ID for thread safety verification
    thread_id: ThreadId,
}

// SAFETY: DieselConn is always used behind a RwLock in SyncConn,
// which ensures exclusive access when needed. Additionally, all methods
// verify thread ownership via check_thread() at runtime.
unsafe impl Sync for DieselConn {}
unsafe impl Send for DieselConn {}

impl DieselConn {
    /// Verify that the current thread is the same as the thread that created this connection
    fn check_thread(&self) -> PyroResult<()> {
        if std::thread::current().id() != self.thread_id {
            return Err(Error::IncorrectApiUsageError(
                "DieselConn can only be used from the thread that created it",
            ));
        }
        Ok(())
    }

    /// Create a new Diesel connection from a URL
    pub fn new(url: &str) -> PyroResult<Self> {
        let conn = MysqlConnection::establish(url)
            .map_err(|e| Error::IoError(format!("Failed to establish connection: {}", e)))?;

        Ok(DieselConn {
            inner: conn,
            affected_rows: 0,
            last_insert_id: None,
            thread_id: std::thread::current().id(),
        })
    }

    /// Get the connection ID
    /// Note: Diesel doesn't expose connection ID directly, we return 0
    pub fn id(&self) -> u32 {
        // Thread check not strictly necessary for reads, but included for consistency
        if self.check_thread().is_err() {
            return 0;
        }
        // Diesel doesn't provide connection ID
        // We could potentially run "SELECT CONNECTION_ID()" if needed
        0
    }

    /// Get the number of affected rows from the last query
    pub fn affected_rows(&self) -> u64 {
        // Thread check not strictly necessary for reads, but included for consistency
        if self.check_thread().is_err() {
            return 0;
        }
        self.affected_rows
    }

    /// Get the last insert ID
    pub fn last_insert_id(&self) -> Option<u64> {
        // Thread check not strictly necessary for reads, but included for consistency
        if self.check_thread().is_err() {
            return None;
        }
        self.last_insert_id
    }

    /// Get the server version
    /// Note: Diesel doesn't expose this directly
    pub fn server_version(&self) -> (u16, u16, u16) {
        // Thread check not strictly necessary for reads, but included for consistency
        if self.check_thread().is_err() {
            return (0, 0, 0);
        }
        // Could run "SELECT VERSION()" if needed
        (0, 0, 0)
    }

    /// Ping the server
    pub fn ping(&mut self) -> PyroResult<()> {
        self.check_thread()?;
        // Execute a simple query to test connection
        sql_query("SELECT 1")
            .execute(&mut self.inner)
            .map_err(|e| Error::IoError(format!("Ping failed: {}", e)))?;
        Ok(())
    }

    /// Reset the connection state
    pub fn reset(&mut self) -> PyroResult<()> {
        self.check_thread()?;
        // Diesel doesn't have a direct reset, but we can clear our state
        self.affected_rows = 0;
        self.last_insert_id = None;
        Ok(())
    }

    /// Execute a text protocol query and return all rows
    pub fn query(&mut self, query_str: String, as_dict: bool) -> PyroResult<Vec<Py<PyAny>>> {
        self.check_thread()?;

        if as_dict {
            // Load results as RowDict
            let results: Vec<RowDict> = sql_query(&query_str)
                .load(&mut self.inner)
                .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

            // Extract inner Py<PyDict> and convert to Py<PyAny>
            Ok(results
                .into_iter()
                .map(|row_dict| row_dict.0.into_any())
                .collect())
        } else {
            // Load results as RowTuple
            let results: Vec<RowTuple> = sql_query(&query_str)
                .load(&mut self.inner)
                .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

            // Extract inner Py<PyTuple> and convert to Py<PyAny>
            Ok(results
                .into_iter()
                .map(|row_tuple| row_tuple.0.into_any())
                .collect())
        }
    }

    /// Execute a text protocol query and return the first row
    pub fn query_first(
        &mut self,
        query_str: String,
        as_dict: bool,
    ) -> PyroResult<Option<Py<PyAny>>> {
        self.check_thread()?;

        if as_dict {
            // Get first result as RowDict
            let result: Option<RowDict> = sql_query(&query_str)
                .get_result(&mut self.inner)
                .optional()
                .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

            // Extract inner Py<PyDict> and convert to Py<PyAny>
            Ok(result.map(|row_dict| row_dict.0.into_any()))
        } else {
            // Get first result as RowTuple
            let result: Option<RowTuple> = sql_query(&query_str)
                .get_result(&mut self.inner)
                .optional()
                .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

            // Extract inner Py<PyTuple> and convert to Py<PyAny>
            Ok(result.map(|row_tuple| row_tuple.0.into_any()))
        }
    }

    /// Execute a text protocol query and drop the results
    pub fn query_drop(&mut self, query_str: String) -> PyroResult<()> {
        self.check_thread()?;
        let rows_affected = sql_query(&query_str)
            .execute(&mut self.inner)
            .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

        self.affected_rows = rows_affected as u64;
        Ok(())
    }

    /// Execute a prepared statement with parameters and return rows
    pub fn exec(
        &mut self,
        query_str: String,
        params: Params,
        as_dict: bool,
    ) -> PyroResult<Vec<Py<PyAny>>> {
        self.check_thread()?;

        match params {
            Params::Empty => {
                // No parameters, use text protocol query
                self.query(query_str, as_dict)
            }
            Params::Positional(values) => {
                let boxed_query = bind_params(query_str, values);

                if as_dict {
                    // Load results as RowDict
                    let results: Vec<RowDict> = boxed_query
                        .load(&mut self.inner)
                        .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

                    // Store affected rows
                    self.affected_rows = results.len() as u64;

                    // Extract inner Py<PyDict> and convert to Py<PyAny>
                    Ok(results
                        .into_iter()
                        .map(|row_dict| row_dict.0.into_any())
                        .collect())
                } else {
                    // Load results as RowTuple
                    let results: Vec<RowTuple> = boxed_query
                        .load(&mut self.inner)
                        .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

                    // Store affected rows
                    self.affected_rows = results.len() as u64;

                    // Extract inner Py<PyTuple> and convert to Py<PyAny>
                    Ok(results
                        .into_iter()
                        .map(|row_tuple| row_tuple.0.into_any())
                        .collect())
                }
            }
        }
    }

    /// Execute a prepared statement and return the first row
    pub fn exec_first(
        &mut self,
        query_str: String,
        params: Params,
        as_dict: bool,
    ) -> PyroResult<Option<Py<PyAny>>> {
        self.check_thread()?;

        match params {
            Params::Empty => {
                // No parameters, use text protocol query
                self.query_first(query_str, as_dict)
            }
            Params::Positional(values) => {
                let boxed_query = bind_params(query_str, values);

                if as_dict {
                    // Get first result as RowDict
                    let result: Option<RowDict> = boxed_query
                        .get_result(&mut self.inner)
                        .optional()
                        .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

                    // Extract inner Py<PyDict> and convert to Py<PyAny>
                    Ok(result.map(|row_dict| row_dict.0.into_any()))
                } else {
                    // Get first result as RowTuple
                    let result: Option<RowTuple> = boxed_query
                        .get_result(&mut self.inner)
                        .optional()
                        .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

                    // Extract inner Py<PyTuple> and convert to Py<PyAny>
                    Ok(result.map(|row_tuple| row_tuple.0.into_any()))
                }
            }
        }
    }

    /// Execute a prepared statement and drop the results
    pub fn exec_drop(&mut self, query_str: String, params: Params) -> PyroResult<()> {
        self.check_thread()?;

        match params {
            Params::Empty => self.query_drop(query_str),
            Params::Positional(values) => {
                let boxed_query = bind_params(query_str, values);
                let rows_affected = boxed_query
                    .execute(&mut self.inner)
                    .map_err(|e| Error::IoError(format!("Query failed: {}", e)))?;

                self.affected_rows = rows_affected as u64;
                Ok(())
            }
        }
    }

    /// Execute a batch of prepared statements
    pub fn exec_batch(&mut self, query_str: String, params_list: Vec<Params>) -> PyroResult<()> {
        self.check_thread()?;

        // Execute each statement in sequence
        // Note: This doesn't use a transaction automatically, unlike the mysql crate
        // If the user wants transactional behavior, they should use start_transaction()
        let mut total_affected = 0u64;

        for params in params_list {
            match params {
                Params::Empty => {
                    self.query_drop(query_str.clone())?;
                    total_affected += self.affected_rows;
                }
                Params::Positional(values) => {
                    let boxed_query = bind_params(query_str.clone(), values);
                    let rows_affected = boxed_query
                        .execute(&mut self.inner)
                        .map_err(|e| Error::IoError(format!("Batch query failed: {}", e)))?;

                    total_affected += rows_affected as u64;
                }
            }
        }

        self.affected_rows = total_affected;
        Ok(())
    }
}
