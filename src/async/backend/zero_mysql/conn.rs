use crate::r#async::backend::zero_mysql::handler::TupleHandler;
use crate::error::{Error, PyroResult};
use crate::params::Params;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use zero_mysql::r#async::Conn;

/// Zero-MySQL asynchronous connection wrapper
pub struct ZeroMysqlConn {
    pub inner: Conn,
    /// Cache to store statement IDs for prepared statements
    /// In production, this should use LRU cache or similar
    stmt_cache: std::collections::HashMap<String, u32>,
}

impl ZeroMysqlConn {
    /// Create a new Zero-MySQL connection from URL
    pub async fn new(url: &str) -> PyroResult<Self> {
        println!("pyro-mysql creating zero conn");
        let conn = Conn::new(url)
            .await
            .map_err(|_e| Error::IncorrectApiUsageError("Failed to connect with zero-mysql"))?;
        println!("pyro-mysql creating zero conn - done");

        Ok(ZeroMysqlConn {
            inner: conn,
            stmt_cache: std::collections::HashMap::new(),
        })
    }

    /// Get the connection ID
    /// Note: zero-mysql doesn't expose connection_id yet, so we return 0
    pub fn id(&self) -> u32 {
        // TODO: Extract connection_id from handshake
        0
    }

    /// Get the number of affected rows from the last query
    /// Note: zero-mysql doesn't track this yet
    pub fn affected_rows(&self) -> u64 {
        // TODO: Track affected_rows from OK packet
        0
    }

    /// Get the last insert ID
    /// Note: zero-mysql doesn't track this yet
    pub fn last_insert_id(&self) -> Option<u64> {
        // TODO: Track last_insert_id from OK packet
        None
    }

    /// Get the server version
    pub fn server_version(&self) -> (u16, u16, u16) {
        // Parse server version string (e.g., "8.0.33")
        let version_str = self.inner.server_version();
        let parts: Vec<&str> = version_str.split('.').collect();

        let major = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
        let minor = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let patch = parts
            .get(2)
            .and_then(|s| {
                // Handle versions like "8.0.33-0ubuntu0.22.04.4"
                s.split('-').next()?.parse().ok()
            })
            .unwrap_or(0);

        (major, minor, patch)
    }

    /// Ping the server to keep the connection alive
    /// Note: zero-mysql doesn't have ping yet
    pub async fn ping(&mut self) -> PyroResult<()> {
        // TODO: Implement COM_PING
        Ok(())
    }

    /// Reset the connection state
    /// Note: zero-mysql doesn't have reset yet
    pub async fn reset(&mut self) -> PyroResult<()> {
        // TODO: Implement COM_RESET_CONNECTION
        Ok(())
    }

    /// Disconnect from the server
    pub async fn disconnect(self) -> PyroResult<()> {
        // zero-mysql doesn't have an explicit disconnect method, drop will handle cleanup
        Ok(())
    }

    /// Execute a query using the text protocol
    /// For zero-mysql, we use prepared statements instead
    pub async fn query(&mut self, query: String) -> PyroResult<Vec<Py<PyTuple>>> {
        // For zero-mysql, convert text query to prepared statement
        // This is not ideal but zero-mysql focuses on binary protocol

        // Check if we have a cached statement
        let stmt_id = if let Some(&cached_id) = self.stmt_cache.get(&query) {
            cached_id
        } else {
            let stmt_id = self
                .inner
                .prepare(&query)
                .await
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare statement"))?;
            self.stmt_cache.insert(query.clone(), stmt_id);
            stmt_id
        };

        // Create handler and buffer without Python GIL
        let mut handler = TupleHandler::new();
        let mut buffer = Vec::new();

        // Execute with empty params (for text protocol queries)
        self.inner
            .exec_fold(stmt_id, &(), &mut handler, &mut buffer)
            .await
            .map_err(|e| {
                println!("error in query: {:?}", e);
                Error::IncorrectApiUsageError("Failed to execute query")
            })?;

        // Convert raw rows to Python objects with the GIL
        Python::attach(|py| {
            handler.into_py_rows(py).map_err(|_e| {
                Error::IncorrectApiUsageError("Failed to convert rows to Python objects")
            })
        })
    }

    /// Execute a prepared statement with parameters
    pub async fn exec(&mut self, query: String, params: Params) -> PyroResult<Vec<Py<PyTuple>>> {
        use super::params_adapter::ParamsAdapter;

        // Check if we have a cached statement
        let stmt_id = if let Some(&cached_id) = self.stmt_cache.get(&query) {
            cached_id
        } else {
            let stmt_id = self.inner.prepare(&query).await.map_err(|e| {
                println!("--- error from zero: {:?}", e);
                Error::IncorrectApiUsageError("Failed to prepare query")
            })?;
            self.stmt_cache.insert(query.clone(), stmt_id);
            stmt_id
        };

        // Create handler and buffer without Python GIL
        let mut handler = TupleHandler::new();
        let mut buffer = Vec::new();

        // Convert Params to zero-mysql params format
        let params_adapter = ParamsAdapter::new(&params);
        self.inner
            .exec_fold(stmt_id, &params_adapter, &mut handler, &mut buffer)
            .await
            .map_err(|e| {
                println!("error from zero: {:?}", e);
                Error::IncorrectApiUsageError("Failed to execute query")
            })?;

        // Convert raw rows to Python objects with the GIL
        Python::attach(|py| {
            handler.into_py_rows(py).map_err(|_e| {
                Error::IncorrectApiUsageError("Failed to convert rows to Python objects")
            })
        })
    }
}
