use crate::error::{Error, PyroResult};
use crate::params::Params;
use crate::sync::backend::zero_mysql::handler::{DropHandler, TupleHandler};
use pyo3::prelude::*;
use pyo3::types::PyList;
use zero_mysql::sync::Conn;

pub struct ZeroMysqlConn {
    pub inner: Conn,
    stmt_cache: std::collections::HashMap<String, u32>,
    affected_rows: u64,
    last_insert_id: u64,
}

impl ZeroMysqlConn {
    /// Create a new Zero-MySQL connection from URL
    pub fn new(url: &str) -> PyroResult<Self> {
        let conn = Conn::new(url)
            .map_err(|_e| Error::IncorrectApiUsageError("Failed to connect with zero-mysql"))?;

        Ok(ZeroMysqlConn {
            inner: conn,
            stmt_cache: std::collections::HashMap::new(),
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    pub fn new_with_opts(opts: zero_mysql::Opts) -> PyroResult<Self> {
        let conn = Conn::new(opts)
            .map_err(|_e| Error::IncorrectApiUsageError("Failed to connect with zero-mysql"))?;

        Ok(ZeroMysqlConn {
            inner: conn,
            stmt_cache: std::collections::HashMap::new(),
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
    pub fn ping(&mut self) -> PyroResult<()> {
        self.inner.ping()?;
        Ok(())
    }

    /// Reset the connection state
    /// This clears temporary tables, user variables, prepared statements, etc.
    pub fn reset(&mut self) -> PyroResult<()> {
        self.inner.reset()?;
        self.stmt_cache.clear();
        Ok(())
    }

    pub fn query<'py>(&mut self, py: Python<'py>, query: String) -> PyroResult<Py<PyList>> {
        let mut handler = TupleHandler::new(py);

        self.inner.query(&query, &mut handler).map_err(|e| {
            println!("error in query: {:?}", e);
            Error::IncorrectApiUsageError("Failed to execute query")
        })?;

        self.affected_rows = handler.affected_rows();
        self.last_insert_id = handler.last_insert_id();
        Ok(handler.into_rows())
    }

    pub fn query_drop(&mut self, query: String) -> PyroResult<()> {
        let mut handler = DropHandler::new();

        self.inner.query(&query, &mut handler).map_err(|_e| {
            Error::IncorrectApiUsageError("Failed to execute query")
        })?;

        self.affected_rows = handler.affected_rows;
        self.last_insert_id = handler.last_insert_id;
        Ok(())
    }

    pub fn exec<'py>(
        &mut self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyroResult<Py<PyList>> {
        use super::params_adapter::ParamsAdapter;

        let stmt_id = if let Some(&cached_id) = self.stmt_cache.get(&query) {
            cached_id
        } else {
            let stmt_id = self
                .inner
                .prepare(&query)
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            self.stmt_cache.insert(query.clone(), stmt_id);
            stmt_id
        };

        let mut handler = TupleHandler::new(py);
        let params_adapter = ParamsAdapter::new(&params);
        self.inner
            .exec(stmt_id, params_adapter, &mut handler)
            .map_err(|_e| Error::IncorrectApiUsageError("Failed to execute query"))?;

        self.affected_rows = handler.affected_rows();
        self.last_insert_id = handler.last_insert_id();
        Ok(handler.into_rows())
    }

    pub fn exec_first<'py>(
        &mut self,
        py: Python<'py>,
        query: String,
        params: Params,
    ) -> PyroResult<Option<Py<PyAny>>> {
        use super::params_adapter::ParamsAdapter;

        let stmt_id = if let Some(&cached_id) = self.stmt_cache.get(&query) {
            cached_id
        } else {
            let stmt_id = self.inner.prepare(&query).map_err(|e| {
                println!("--- error from zero: {:?}", e);
                Error::IncorrectApiUsageError("Failed to prepare query")
            })?;
            self.stmt_cache.insert(query.clone(), stmt_id);
            stmt_id
        };

        let mut handler = TupleHandler::new(py);
        let params_adapter = ParamsAdapter::new(&params);
        self.inner
            .exec_first(stmt_id, params_adapter, &mut handler)
            .map_err(|e| {
                println!("error from zero: {:?}", e);
                Error::IncorrectApiUsageError("Failed to execute query")
            })?;

        self.affected_rows = handler.affected_rows();
        self.last_insert_id = handler.last_insert_id();
        let rows = handler.into_rows();
        Ok(if rows.bind(py).len() > 0 {
            Some(rows.bind(py).get_item(0)?.unbind())
        } else {
            None
        })
    }

    pub fn exec_drop(&mut self, query: String, params: Params) -> PyroResult<()> {
        use super::params_adapter::ParamsAdapter;

        let stmt_id = if let Some(&cached_id) = self.stmt_cache.get(&query) {
            cached_id
        } else {
            let stmt_id = self
                .inner
                .prepare(&query)
                .map_err(|_e| Error::IncorrectApiUsageError("Failed to prepare query"))?;
            self.stmt_cache.insert(query.clone(), stmt_id);
            stmt_id
        };

        let mut handler = DropHandler::new();
        let params_adapter = ParamsAdapter::new(&params);
        self.inner
            .exec(stmt_id, params_adapter, &mut handler)
            .map_err(|_e| Error::IncorrectApiUsageError("Failed to execute query"))?;

        self.affected_rows = handler.affected_rows;
        self.last_insert_id = handler.last_insert_id;
        Ok(())
    }
}
