use mysql_async::prelude::Queryable;

use crate::r#async::backend::{WtxConn, ZeroMysqlConn};
use crate::error::PyroResult;

/// Multi-backend async connection enum
pub enum MultiAsyncConn {
    MysqlAsync(mysql_async::Conn),
    Wtx(WtxConn),
    ZeroMysql(ZeroMysqlConn),
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

    /// Create a new ZeroMysql connection from a URL
    pub async fn new_zero_mysql(url: &str) -> PyroResult<Self> {
        let zero_conn = ZeroMysqlConn::new(url).await?;
        Ok(MultiAsyncConn::ZeroMysql(zero_conn))
    }

    /// Create a new ZeroMysql connection from Opts
    pub async fn new_zero_mysql_with_opts(opts: zero_mysql::Opts) -> PyroResult<Self> {
        let zero_conn = ZeroMysqlConn::new_with_opts(opts).await?;
        Ok(MultiAsyncConn::ZeroMysql(zero_conn))
    }

    /// Get the connection ID
    /// Note: Returns 0 for wtx connections as they don't expose this information
    pub fn id(&self) -> u64 {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.id() as u64,
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.id() as u64,
            MultiAsyncConn::ZeroMysql(zero_conn) => zero_conn.id(),
        }
    }

    /// Get the number of affected rows from the last query
    /// Note: Returns 0 for wtx and zero_mysql connections as they return this per-query, not as connection state
    pub fn affected_rows(&self) -> u64 {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.affected_rows(),
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.affected_rows(),
            MultiAsyncConn::ZeroMysql(zero_conn) => zero_conn.affected_rows(),
        }
    }

    /// Get the last insert ID
    /// Note: Returns None for wtx and zero_mysql connections as they don't expose this as connection state
    pub fn last_insert_id(&self) -> Option<u64> {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.last_insert_id(),
            MultiAsyncConn::Wtx(wtx_conn) => wtx_conn.last_insert_id(),
            MultiAsyncConn::ZeroMysql(zero_conn) => Some(zero_conn.last_insert_id()),
        }
    }

    /// Get the server version as a string
    /// Note: Returns "0.0.0" for wtx connections as wtx doesn't expose this information
    pub fn server_version(&self) -> String {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => {
                let (major, minor, patch) = conn.server_version();
                format!("{}.{}.{}", major, minor, patch)
            }
            MultiAsyncConn::Wtx(wtx_conn) => {
                let (major, minor, patch) = wtx_conn.server_version();
                format!("{}.{}.{}", major, minor, patch)
            }
            MultiAsyncConn::ZeroMysql(zero_conn) => {
                let version_bytes = zero_conn.server_version();
                String::from_utf8_lossy(version_bytes).to_string()
            }
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
            MultiAsyncConn::ZeroMysql(zero_conn) => zero_conn.disconnect().await,
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
            MultiAsyncConn::ZeroMysql(zero_conn) => zero_conn.reset().await,
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
            MultiAsyncConn::ZeroMysql(zero_conn) => zero_conn.ping().await,
        }
    }
}
