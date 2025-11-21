use crate::r#async::backend::zero_mysql::handler::{DropHandler, TupleHandler};
use crate::error::{Error, PyroResult};
use crate::params::Params;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use zero_mysql::tokio::Conn;

pub struct ZeroMysqlConn {
    pub inner: Conn,
    stmt_cache: std::collections::HashMap<String, u32>,
    handler: TupleHandler,
    affected_rows: u64,
    last_insert_id: u64,
}

impl ZeroMysqlConn {
    /// Create a new Zero-MySQL connection from URL
    pub async fn new(url: &str) -> PyroResult<Self> {
        let conn = Conn::new(url)
            .await
            .map_err(|_e| Error::IncorrectApiUsageError("Failed to connect with zero-mysql"))?;

        Ok(ZeroMysqlConn {
            inner: conn,
            stmt_cache: std::collections::HashMap::new(),
            handler: TupleHandler::new(),
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    pub async fn new_with_opts(opts: zero_mysql::Opts) -> PyroResult<Self> {
        let conn = Conn::new(opts)
            .await
            .map_err(|_e| Error::IncorrectApiUsageError("Failed to connect with zero-mysql"))?;

        Ok(ZeroMysqlConn {
            inner: conn,
            stmt_cache: std::collections::HashMap::new(),
            handler: TupleHandler::new(),
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// Get the connection ID
    pub fn id(&self) -> u64 {
        self.inner.connection_id()
    }

    /// Get the status flags from the last packet
    pub fn status_flags(&self) -> zero_mysql::constant::ServerStatusFlags {
        self.inner.status_flags()
    }

    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    /// Get the server version string as bytes
    pub fn server_version(&self) -> &[u8] {
        self.inner.server_version()
    }

    /// Ping the server to keep the connection alive
    pub async fn ping(&mut self) -> PyroResult<()> {
        self.inner.ping().await?;
        Ok(())
    }

    /// Reset the connection state
    /// This clears temporary tables, user variables, prepared statements, etc.
    pub async fn reset(&mut self) -> PyroResult<()> {
        self.inner.reset().await?;
        // Clear statement cache as reset invalidates prepared statements
        self.stmt_cache.clear();
        Ok(())
    }

    /// Disconnect from the server
    pub async fn disconnect(self) -> PyroResult<()> {
        // zero-mysql doesn't have an explicit disconnect method, drop will handle cleanup
        Ok(())
    }

    pub async fn query(&mut self, query: String) -> PyroResult<Vec<Py<PyTuple>>> {
        self.handler.clear();

        self.inner
            .query(&query, &mut self.handler)
            .await
            .map_err(|e| {
                println!("error in query: {:?}", e);
                Error::IncorrectApiUsageError("Failed to execute query")
            })?;

        self.affected_rows = self.handler.affected_rows();
        self.last_insert_id = self.handler.last_insert_id();

        Python::attach(|py| {
            self.handler.rows_to_python(py).map_err(|_e| {
                Error::IncorrectApiUsageError("Failed to convert rows to Python objects")
            })
        })
    }

    pub async fn query_drop(&mut self, query: String) -> PyroResult<()> {
        let mut handler = DropHandler::new();
        self.inner
            .query(&query, &mut handler)
            .await
            .map_err(|_e| Error::IncorrectApiUsageError("Failed to execute query"))?;

        self.affected_rows = handler.affected_rows;
        self.last_insert_id = handler.last_insert_id;
        Ok(())
    }

    pub async fn exec(&mut self, query: String, params: Params) -> PyroResult<Vec<Py<PyTuple>>> {
        use super::params_adapter::ParamsAdapter;

        let stmt_id = if let Some(&cached_id) = self.stmt_cache.get(&query) {
            cached_id
        } else {
            let stmt_id = self
                .inner
                .prepare(&query)
                .await
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            self.stmt_cache.insert(query.clone(), stmt_id);
            stmt_id
        };

        self.handler.clear();

        let params_adapter = ParamsAdapter::new(&params);
        self.inner
            .exec(stmt_id, params_adapter, &mut self.handler)
            .await
            .map_err(|e| {
                println!("error from zero: {:?}", e);
                Error::IncorrectApiUsageError("Failed to execute query")
            })?;

        self.affected_rows = self.handler.affected_rows();
        self.last_insert_id = self.handler.last_insert_id();

        Python::attach(|py| {
            self.handler.rows_to_python(py).map_err(|_e| {
                Error::IncorrectApiUsageError("Failed to convert rows to Python objects")
            })
        })
    }

    pub async fn exec_first(
        &mut self,
        query: String,
        params: Params,
    ) -> PyroResult<Option<Py<PyTuple>>> {
        use super::params_adapter::ParamsAdapter;

        let stmt_id = if let Some(&cached_id) = self.stmt_cache.get(&query) {
            cached_id
        } else {
            let stmt_id = self
                .inner
                .prepare(&query)
                .await
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            self.stmt_cache.insert(query.clone(), stmt_id);
            stmt_id
        };

        self.handler.clear();

        let params_adapter = ParamsAdapter::new(&params);
        self.inner
            .exec_first(stmt_id, params_adapter, &mut self.handler)
            .await
            .map_err(|e| {
                println!("error from zero: {:?}", e);
                Error::IncorrectApiUsageError("Failed to execute query")
            })?;

        self.affected_rows = self.handler.affected_rows();
        self.last_insert_id = self.handler.last_insert_id();

        Python::attach(|py| {
            let rows = self.handler.rows_to_python(py).map_err(|_e| {
                Error::IncorrectApiUsageError("Failed to convert rows to Python objects")
            })?;
            Ok(rows.into_iter().next())
        })
    }

    pub async fn exec_drop(&mut self, query: String, params: Params) -> PyroResult<()> {
        use super::params_adapter::ParamsAdapter;

        let stmt_id = if let Some(&cached_id) = self.stmt_cache.get(&query) {
            cached_id
        } else {
            let stmt_id = self
                .inner
                .prepare(&query)
                .await
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            self.stmt_cache.insert(query.clone(), stmt_id);
            stmt_id
        };

        let mut handler = DropHandler::new();
        let params_adapter = ParamsAdapter::new(&params);
        self.inner
            .exec(stmt_id, params_adapter, &mut handler)
            .await
            .map_err(|e| {
                println!("error from zero: {:?}", e);
                Error::IncorrectApiUsageError("Failed to execute query")
            })?;

        self.affected_rows = handler.affected_rows;
        self.last_insert_id = handler.last_insert_id;
        Ok(())
    }
}
