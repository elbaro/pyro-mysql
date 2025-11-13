use crate::error::PyroResult;
use crate::sync::backend::{DieselConn, MysqlConn, ZeroMysqlConn};

/// Multi-backend sync connection enum
pub enum MultiSyncConn {
    Mysql(MysqlConn),
    Diesel(DieselConn),
    ZeroMysql(ZeroMysqlConn),
}

impl MultiSyncConn {
    /// Get the connection ID
    pub fn id(&self) -> u32 {
        match self {
            MultiSyncConn::Mysql(conn) => conn.id(),
            MultiSyncConn::Diesel(conn) => conn.id(),
            MultiSyncConn::ZeroMysql(conn) => conn.id(),
        }
    }

    /// Get the number of affected rows from the last query
    pub fn affected_rows(&self) -> u64 {
        match self {
            MultiSyncConn::Mysql(conn) => conn.affected_rows(),
            MultiSyncConn::Diesel(conn) => conn.affected_rows(),
            MultiSyncConn::ZeroMysql(conn) => conn.affected_rows(),
        }
    }

    /// Get the last insert ID
    pub fn last_insert_id(&self) -> Option<u64> {
        match self {
            MultiSyncConn::Mysql(conn) => conn.last_insert_id(),
            MultiSyncConn::Diesel(conn) => conn.last_insert_id(),
            MultiSyncConn::ZeroMysql(conn) => conn.last_insert_id(),
        }
    }

    /// Get the server version
    pub fn server_version(&self) -> (u16, u16, u16) {
        match self {
            MultiSyncConn::Mysql(conn) => conn.server_version(),
            MultiSyncConn::Diesel(conn) => conn.server_version(),
            MultiSyncConn::ZeroMysql(conn) => conn.server_version(),
        }
    }

    /// Ping the server to keep the connection alive
    pub fn ping(&mut self) -> PyroResult<()> {
        match self {
            MultiSyncConn::Mysql(conn) => conn.ping(),
            MultiSyncConn::Diesel(conn) => conn.ping(),
            MultiSyncConn::ZeroMysql(conn) => conn.ping(),
        }
    }

    /// Reset the connection state
    pub fn reset(&mut self) -> PyroResult<()> {
        match self {
            MultiSyncConn::Mysql(conn) => conn.reset(),
            MultiSyncConn::Diesel(conn) => conn.reset(),
            MultiSyncConn::ZeroMysql(conn) => conn.reset(),
        }
    }
}
