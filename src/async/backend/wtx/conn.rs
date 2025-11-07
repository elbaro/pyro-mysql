use std::collections::HashMap;
use crate::error::{Error, PyroResult};

// Type alias for wtx MySQL executor with tokio runtime
pub type WtxMysqlExecutor = wtx::database::client::mysql::MysqlExecutor<
    wtx::Error,
    wtx::database::client::mysql::ExecutorBuffer,
    tokio::net::TcpStream,
>;

/// WTX MySQL connection wrapper
pub struct WtxConn {
    pub executor: WtxMysqlExecutor,
    /// Client-side statement cache: query string -> statement ID (u64)
    pub stmt_cache: HashMap<String, u64>,
}

impl WtxConn {
    /// Create a new Wtx connection from a URL
    pub async fn new(
        url: &str,
        max_statements: Option<usize>,
        buffer_size: Option<(usize, usize, usize, usize, usize)>,
    ) -> PyroResult<Self> {
        use wtx::misc::Uri;
        use wtx::database::client::mysql::{Config, ExecutorBuffer, MysqlExecutor};
        use wtx::rng::SeedableRng;

        // Parse URL
        let uri = Uri::new(url);
        let config = Config::from_uri(&uri).map_err(|e| Error::WtxError(e.to_string()))?;

        // Create RNG for authentication
        let mut rng = wtx::rng::ChaCha20::from_os().map_err(|e| Error::WtxError(e.to_string()))?;

        // Create executor buffer with specified size or default to usize::MAX
        let max_capacity = max_statements.unwrap_or(usize::MAX);
        let eb = if let Some(buffer_caps) = buffer_size {
            // Use with_capacity when buffer sizes are specified
            ExecutorBuffer::with_capacity(
                buffer_caps,
                max_capacity,
                &mut rng,
            )
            .map_err(|e| Error::WtxError(e.to_string()))?
        } else {
            // Use default constructor
            ExecutorBuffer::new(max_capacity, &mut rng)
        };

        // Connect to MySQL server
        let addr = uri.hostname_with_implied_port();
        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| Error::IoError(e.to_string()))?;

        // Set TCP_NODELAY for better performance
        stream.set_nodelay(true)
            .map_err(|e| Error::IoError(e.to_string()))?;

        // Connect
        let executor = MysqlExecutor::connect(&config, eb, &mut rng, stream)
            .await
            .map_err(|e: wtx::Error| Error::WtxError(e.to_string()))?;

        Ok(WtxConn {
            executor,
            stmt_cache: HashMap::new(),
        })
    }

    /// Get the connection ID
    /// Note: Not implemented for wtx connections as wtx doesn't expose this information
    pub fn id(&self) -> u32 {
        unimplemented!("wtx backend does not expose connection ID")
    }

    /// Get the number of affected rows from the last query
    /// Note: Not implemented for wtx connections as wtx returns this per-query, not as connection state
    pub fn affected_rows(&self) -> u64 {
        unimplemented!("wtx backend returns affected rows per query, not as connection state")
    }

    /// Get the last insert ID
    /// Note: Not implemented for wtx connections as wtx doesn't expose this as connection state
    pub fn last_insert_id(&self) -> Option<u64> {
        unimplemented!("wtx backend does not expose last_insert_id as connection state")
    }

    /// Get the server version
    /// Note: Not implemented for wtx connections as wtx doesn't expose this information
    pub fn server_version(&self) -> (u16, u16, u16) {
        unimplemented!("wtx backend does not expose server version")
    }

    /// Disconnect from the server
    pub async fn disconnect(self) -> PyroResult<()> {
        // wtx doesn't have an explicit disconnect method, drop will handle cleanup
        Ok(())
    }

    /// Reset the connection state
    /// Note: For wtx connections, this uses "RESET CONNECTION" SQL command (MySQL 5.7.3+)
    pub async fn reset(&mut self) -> PyroResult<()> {
        // Reset connection using MySQL RESET CONNECTION statement (MySQL 5.7.3+)
        // This clears temporary tables, user variables, prepared statements, etc.
        use wtx::database::Executor;
        self.executor.execute("RESET CONNECTION", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
            .await
            .map_err(|e| Error::WtxError(e.to_string()))?;
        // Clear statement cache as RESET CONNECTION invalidates prepared statements
        self.stmt_cache.clear();
        Ok(())
    }

    /// Ping the server to keep the connection alive
    pub async fn ping(&mut self) -> PyroResult<()> {
        // Use COM_PING or just a simple query
        use wtx::database::Executor;
        self.executor.execute("SELECT 1", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
            .await
            .map_err(|e| Error::WtxError(e.to_string()))?;
        Ok(())
    }
}
