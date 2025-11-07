use crate::error::{Error, PyroResult};

/// Diesel synchronous connection wrapper
/// Note: This is a placeholder implementation. Diesel requires additional setup
/// and schema definitions to be fully functional.
pub struct DieselConn {
    // Placeholder for diesel connection
    // In a real implementation, this would be:
    // pub inner: diesel::MysqlConnection,
    _placeholder: (),
}

impl DieselConn {
    /// Create a new Diesel connection from a URL
    /// Note: This is a placeholder that returns an error
    pub fn new(_url: &str) -> PyroResult<Self> {
        Err(Error::IncorrectApiUsageError(
            "Diesel backend is not yet implemented. Please use the 'mysql' backend instead.",
        ))
    }

    /// Get the connection ID
    /// Note: Diesel doesn't expose connection ID in the same way as mysql crate
    pub fn id(&self) -> u32 {
        0
    }

    /// Get the number of affected rows from the last query
    /// Note: Diesel handles this differently
    pub fn affected_rows(&self) -> u64 {
        0
    }

    /// Get the last insert ID
    /// Note: Diesel doesn't track this automatically
    pub fn last_insert_id(&self) -> Option<u64> {
        None
    }

    /// Get the server version
    /// Note: Diesel doesn't expose this directly
    pub fn server_version(&self) -> (u16, u16, u16) {
        (0, 0, 0)
    }

    /// Ping the server
    pub fn ping(&mut self) -> PyroResult<()> {
        Err(Error::IncorrectApiUsageError(
            "Diesel backend is not yet implemented",
        ))
    }

    /// Reset the connection state
    pub fn reset(&mut self) -> PyroResult<()> {
        Err(Error::IncorrectApiUsageError(
            "Diesel backend is not yet implemented",
        ))
    }
}
