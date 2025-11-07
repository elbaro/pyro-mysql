use crate::error::PyroResult;

/// Multi-backend sync connection enum
/// Currently only supports MySQL, but designed to allow future backends
pub enum MultiSyncConn {
    Mysql(mysql::Conn),
}

impl MultiSyncConn {
    /// Get the connection ID
    pub fn id(&self) -> u32 {
        match self {
            MultiSyncConn::Mysql(conn) => conn.connection_id(),
        }
    }

    /// Get the number of affected rows from the last query
    pub fn affected_rows(&self) -> u64 {
        match self {
            MultiSyncConn::Mysql(conn) => conn.affected_rows(),
        }
    }

    /// Get the last insert ID
    pub fn last_insert_id(&self) -> Option<u64> {
        match self {
            MultiSyncConn::Mysql(conn) => {
                let id = conn.last_insert_id();
                if id == 0 {
                    None
                } else {
                    Some(id)
                }
            }
        }
    }

    /// Get the server version
    pub fn server_version(&self) -> (u16, u16, u16) {
        match self {
            MultiSyncConn::Mysql(conn) => conn.server_version(),
        }
    }

    /// Ping the server to keep the connection alive
    pub fn ping(&mut self) -> PyroResult<()> {
        match self {
            MultiSyncConn::Mysql(conn) => {
                conn.ping()?;
                Ok(())
            }
        }
    }

    /// Reset the connection state
    pub fn reset(&mut self) -> PyroResult<()> {
        match self {
            MultiSyncConn::Mysql(conn) => {
                conn.reset()?;
                Ok(())
            }
        }
    }
}
