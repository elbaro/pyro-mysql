use either::Either;
use mysql_async::prelude::Queryable as MysqlAsyncQueryable;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::r#async::opts::AsyncOpts;
use crate::r#async::queryable::Queryable;
use crate::r#async::transaction::AsyncTransaction;
use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::util::{PyroFuture, rust_future_into_py, url_error_to_pyerr};

// Type alias for wtx MySQL executor with tokio runtime
pub type WtxMysqlExecutor = wtx::database::client::mysql::MysqlExecutor<
    wtx::Error,
    wtx::database::client::mysql::ExecutorBuffer,
    tokio::net::TcpStream,
>;

/// Multi-backend async connection enum
pub enum MultiAsyncConn {
    MysqlAsync(mysql_async::Conn),
    Wtx {
        executor: WtxMysqlExecutor,
        /// Client-side statement cache: query string -> statement ID (u64)
        stmt_cache: HashMap<String, u64>,
    },
}

impl MultiAsyncConn {
    /// Create a new Wtx connection from a URL
    pub async fn new_wtx(
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

        Ok(MultiAsyncConn::Wtx {
            executor,
            stmt_cache: HashMap::new(),
        })
    }

    /// Get the connection ID
    /// Note: Returns 0 for wtx connections as wtx doesn't expose this information
    pub fn id(&self) -> u32 {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.id(),
            MultiAsyncConn::Wtx { .. } => {
                // wtx doesn't expose connection ID directly
                0
            }
        }
    }

    /// Get the number of affected rows from the last query
    /// Note: Returns 0 for wtx connections as wtx returns this per-query, not as connection state
    pub fn affected_rows(&self) -> u64 {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.affected_rows(),
            MultiAsyncConn::Wtx { .. } => {
                // wtx returns affected rows per query, not as connection state
                0
            }
        }
    }

    /// Get the last insert ID
    /// Note: Returns None for wtx connections as wtx doesn't expose this as connection state
    pub fn last_insert_id(&self) -> Option<u64> {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.last_insert_id(),
            MultiAsyncConn::Wtx { .. } => {
                // wtx doesn't expose last_insert_id as connection state
                None
            }
        }
    }

    /// Get the server version
    /// Note: Returns (0, 0, 0) for wtx connections as wtx doesn't expose this information
    pub fn server_version(&self) -> (u16, u16, u16) {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => conn.server_version(),
            MultiAsyncConn::Wtx { .. } => {
                // wtx doesn't expose server_version directly
                (0, 0, 0)
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
            MultiAsyncConn::Wtx { .. } => {
                // wtx doesn't have an explicit disconnect method, drop will handle cleanup
                Ok(())
            }
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
            MultiAsyncConn::Wtx { executor, stmt_cache } => {
                // Reset connection using MySQL RESET CONNECTION statement (MySQL 5.7.3+)
                // This clears temporary tables, user variables, prepared statements, etc.
                use wtx::database::Executor;
                executor.execute("RESET CONNECTION", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
                    .await
                    .map_err(|e| Error::WtxError(e.to_string()))?;
                // Clear statement cache as RESET CONNECTION invalidates prepared statements
                stmt_cache.clear();
                Ok(())
            }
        }
    }

    /// Ping the server to keep the connection alive
    pub async fn ping(&mut self) -> PyroResult<()> {
        match self {
            MultiAsyncConn::MysqlAsync(conn) => {
                conn.ping().await?;
                Ok(())
            }
            MultiAsyncConn::Wtx { executor, .. } => {
                // Use COM_PING or just a simple query
                use wtx::database::Executor;
                executor.execute("SELECT 1", |_: u64| -> Result<(), wtx::Error> { Ok(()) })
                    .await
                    .map_err(|e| Error::WtxError(e.to_string()))?;
                Ok(())
            }
        }
    }
}

#[pyclass(module = "pyro_mysql.async_", name = "Conn")]
pub struct AsyncConn {
    pub inner: Arc<RwLock<Option<MultiAsyncConn>>>,
}

#[pymethods]
impl AsyncConn {
    // ─── Connection Management ───────────────────────────────────────────
    #[new]
    fn _new() -> PyroResult<Self> {
        Err(Error::IncorrectApiUsageError(
            "use `await Conn.new(url) instead of Conn()`.",
        ))
    }

    #[allow(clippy::new_ret_no_self)]
    #[staticmethod]
    pub fn new<'py>(
        py: Python<'py>,
        url_or_opts: Either<String, PyRef<AsyncOpts>>,
    ) -> PyResult<Py<PyroFuture>> {
        let opts = match url_or_opts {
            Either::Left(url) => mysql_async::Opts::from_url(&url).map_err(url_error_to_pyerr)?,
            Either::Right(opts) => opts.opts.clone(),
        };
        rust_future_into_py(py, async move {
            let conn = mysql_async::Conn::new(opts).await?;
            Ok(Self {
                inner: Arc::new(RwLock::new(Some(MultiAsyncConn::MysqlAsync(conn)))),
            })
        })
    }

    #[allow(clippy::new_ret_no_self)]
    #[staticmethod]
    #[pyo3(signature = (url, max_statements=None, buffer_size=None))]
    pub fn new_wtx<'py>(py: Python<'py>, url: String, max_statements: Option<usize>, buffer_size: Option<(usize, usize, usize, usize, usize)>) -> PyResult<Py<PyroFuture>> {
        rust_future_into_py(py, async move {
            let multi_conn = MultiAsyncConn::new_wtx(&url, max_statements, buffer_size).await?;
            Ok(Self {
                inner: Arc::new(RwLock::new(Some(multi_conn))),
            })
        })
    }

    #[pyo3(signature = (consistent_snapshot=false, isolation_level=None, readonly=None))]
    fn start_transaction(
        &self,
        consistent_snapshot: bool,
        isolation_level: Option<PyRef<IsolationLevel>>,
        readonly: Option<bool>,
    ) -> AsyncTransaction {
        let isolation_level: Option<mysql_async::IsolationLevel> =
            isolation_level.map(|l| mysql_async::IsolationLevel::from(&*l));
        let mut opts = mysql_async::TxOpts::new();
        opts.with_consistent_snapshot(consistent_snapshot)
            .with_isolation_level(isolation_level)
            .with_readonly(readonly);
        AsyncTransaction::new(self.inner.clone(), opts)
    }

    async fn id(&self) -> PyResult<u32> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .id())
    }

    async fn affected_rows(&self) -> PyResult<u64> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .affected_rows())
    }

    async fn last_insert_id(&self) -> PyResult<Option<u64>> {
        Ok(self
            .inner
            .read()
            .await
            .as_ref()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .last_insert_id())
    }
    async fn close(&self) -> PyroResult<()> {
        let mut inner = self.inner.write().await;
        if let Some(conn) = inner.take() {
            conn.disconnect().await?;
        }
        Ok(())
    }
    async fn reset(&self) -> PyroResult<()> {
        let mut inner = self.inner.write().await;
        inner
            .as_mut()
            .ok_or_else(|| Error::ConnectionClosedError)?
            .reset()
            .await?;
        Ok(())
    }

    fn server_version<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            Ok(inner
                .read()
                .await
                .as_ref()
                .ok_or_else(|| Error::ConnectionClosedError)?
                .server_version())
        })
    }
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        self.inner.ping(py)
    }

    // ─── Text Protocol ───────────────────────────────────────────────────
    fn query<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.inner.query(py, query)
    }
    fn query_first<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.inner.query_first(py, query)
    }
    fn query_drop<'py>(&self, py: Python<'py>, query: String) -> PyResult<Py<PyroFuture>> {
        self.inner.query_drop(py, query)
    }

    // ─── Binary Protocol ─────────────────────────────────────────────────
    #[pyo3(signature = (query, params=None))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec(py, query, params)
    }
    #[pyo3(signature = (query, params=None))]
    fn exec_first<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec_first(py, query, params)
    }
    #[pyo3(signature = (query, params=None))]
    fn exec_drop<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params = params.unwrap_or_else(|| py.None());
        self.inner.exec_drop(py, query, params)
    }
    #[pyo3(signature = (query, params=vec![]))]
    fn exec_batch<'py>(
        &self,
        py: Python<'py>,
        query: PyBackedStr,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        self.inner.exec_batch(py, query, params)
    }
}
