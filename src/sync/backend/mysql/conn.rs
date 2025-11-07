use crate::error::PyroResult;

/// MySQL synchronous connection wrapper
pub struct MysqlConn {
    pub inner: mysql::Conn,
}

impl MysqlConn {
    /// Create a new MySQL connection from options
    pub fn new(opts: mysql::Opts) -> PyroResult<Self> {
        let conn = mysql::Conn::new(opts)?;
        Ok(MysqlConn { inner: conn })
    }

    /// Get the connection ID
    pub fn id(&self) -> u32 {
        self.inner.connection_id()
    }

    /// Get the number of affected rows from the last query
    pub fn affected_rows(&self) -> u64 {
        self.inner.affected_rows()
    }

    /// Get the last insert ID
    pub fn last_insert_id(&self) -> Option<u64> {
        let id = self.inner.last_insert_id();
        if id == 0 {
            None
        } else {
            Some(id)
        }
    }

    /// Get the server version
    pub fn server_version(&self) -> (u16, u16, u16) {
        self.inner.server_version()
    }

    /// Ping the server to keep the connection alive
    pub fn ping(&mut self) -> PyroResult<()> {
        self.inner.ping()?;
        Ok(())
    }

    /// Reset the connection state
    pub fn reset(&mut self) -> PyroResult<()> {
        self.inner.reset()?;
        Ok(())
    }
}
