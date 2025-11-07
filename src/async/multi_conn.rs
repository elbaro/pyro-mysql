use mysql_async::prelude::Queryable;

use crate::error::PyroResult;
use crate::r#async::backend::WtxConn;

/// Multi-backend async connection enum
pub enum MultiAsyncConn {
    MysqlAsync(mysql_async::Conn),
    Wtx(WtxConn),
}

impl MultiAsyncConn {
    /// Create a new Wtx connection from a URL
    pub async fn new_wtx(
        url: &str,
        max_statements: Option<usize>,
        buffer_size: Option<(usize, usize, usize, usize, usize)>,
    ) -> PyroResult<Self> {
        let wtx_conn = WtxConn::new(url, max_statements, buffer_size).await?;
        Ok(MultiAsyncConn::Wtx(wtx_conn))
    }

    /// Get the connection ID
    /// Note: Returns 0 for wtx connections as wtx doesn't expose this information
    pub fn id(&self) -> u32 {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.id(),
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.id(),
        }
    }

    /// Get the number of affected rows from the last query
    /// Note: Returns 0 for wtx connections as wtx returns this per-query, not as connection state
    pub fn affected_rows(&self) -> u64 {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.affected_rows(),
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.affected_rows(),
        }
    }

    /// Get the last insert ID
    /// Note: Returns None for wtx connections as wtx doesn't expose this as connection state
    pub fn last_insert_id(&self) -> Option<u64> {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.last_insert_id(),
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.last_insert_id(),
        }
    }

    /// Get the server version
    /// Note: Returns (0, 0, 0) for wtx connections as wtx doesn't expose this information
    pub fn server_version(&self) -> (u16, u16, u16) {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.server_version(),
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.server_version(),
        }
    }

    /// Disconnect from the server
    pub async fn disconnect(self) -> PyroResult<()> {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => {
                conn.disconnect().await?;
                Ok(())
            }
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.disconnect().await,
        }
    }

    /// Reset the connection state
    /// Note: For wtx connections, this uses "RESET CONNECTION" SQL command (MySQL 5.7.3+)
    pub async fn reset(&mut self) -> PyroResult<()> {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => {
                conn.reset().await?;
                Ok(())
            }
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.reset().await,
        }
    }

    /// Ping the server to keep the connection alive
    pub async fn ping(&mut self) -> PyroResult<()> {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => {
                conn.ping().await?;
                Ok(())
            }
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.ping().await,
        }
    }
}
