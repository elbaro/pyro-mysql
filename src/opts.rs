use std::sync::Arc;

use pyo3::prelude::*;

use crate::error::PyroResult;

/// A pool of reusable buffers for MySQL connections.
///
/// Buffer pools reduce memory allocation overhead by reusing buffers across queries.
/// By default, connections use a global shared buffer pool. You can create a custom
/// pool for isolation or tuning.
#[pyclass(module = "pyro_mysql", name = "BufferPool")]
#[derive(Clone)]
pub struct BufferPool {
    pub inner: Arc<zero_mysql::BufferPool>,
}

#[pymethods]
impl BufferPool {
    /// Create a new BufferPool with the specified capacity.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of buffer sets to pool (default: 128)
    #[new]
    #[pyo3(signature = (capacity=None))]
    fn new(capacity: Option<usize>) -> Self {
        let cap = capacity.unwrap_or(128);
        Self {
            inner: Arc::new(zero_mysql::BufferPool::new(cap)),
        }
    }

    fn __repr__(&self) -> String {
        format!("BufferPool({:?})", Arc::as_ptr(&self.inner))
    }
}

/// Connection options for MySQL connections.
///
/// This class provides a builder API for configuring MySQL connection parameters.
/// Methods can be chained to configure multiple options, and the instance can be
/// passed directly to connection methods.
///
/// # Examples
/// ```python
/// # Create from URL
/// opts = Opts.from_url("mysql://user:pass@localhost:3306/mydb")
///
/// # Or build manually
/// opts = Opts().host("localhost").port(3306).user("root").password("secret").db("mydb")
/// ```
#[pyclass(module = "pyro_mysql", name = "Opts")]
#[derive(Clone, Debug, Default)]
pub struct Opts {
    pub inner: zero_mysql::Opts,
}

#[pymethods]
impl Opts {
    /// Create a new Opts instance.
    ///
    /// # Arguments
    /// * `url` - Optional MySQL connection URL. If provided, parses the URL.
    ///           If not provided, creates default opts.
    ///
    /// # URL Format
    /// ```text
    /// mysql://[username[:password]@]host[:port][/database]
    /// ```
    ///
    /// # Examples
    /// ```python
    /// # Create default opts
    /// opts = Opts()
    ///
    /// # Create from URL
    /// opts = Opts("mysql://root:password@localhost:3306/mydb")
    /// ```
    #[new]
    #[pyo3(signature = (url=None))]
    fn new(url: Option<&str>) -> PyroResult<Self> {
        if let Some(url) = url {
            let inner: zero_mysql::Opts = url.try_into()?;
            Ok(Self { inner })
        } else {
            Ok(Self::default())
        }
    }

    /// Set the hostname or IP address.
    ///
    /// # Arguments
    /// * `hostname` - The hostname or IP address to connect to
    fn host(mut self_: PyRefMut<Self>, hostname: String) -> PyRefMut<Self> {
        self_.inner.host = Some(hostname);
        self_
    }

    /// Set the TCP port number.
    ///
    /// # Arguments
    /// * `port` - The port number (default: 3306)
    fn port(mut self_: PyRefMut<Self>, port: u16) -> PyRefMut<Self> {
        self_.inner.port = port;
        self_
    }

    /// Set the Unix socket path for local connections.
    ///
    /// # Arguments
    /// * `path` - The path to the Unix socket file
    fn socket(mut self_: PyRefMut<Self>, path: Option<String>) -> PyRefMut<Self> {
        self_.inner.socket = path;
        self_
    }

    /// Set the username for authentication.
    ///
    /// # Arguments
    /// * `username` - The username (can be empty for anonymous connections)
    fn user(mut self_: PyRefMut<Self>, username: String) -> PyRefMut<Self> {
        self_.inner.user = username;
        self_
    }

    /// Set the password for authentication.
    ///
    /// # Arguments
    /// * `password` - The password
    fn password(mut self_: PyRefMut<Self>, password: Option<String>) -> PyRefMut<Self> {
        self_.inner.password = password;
        self_
    }

    /// Set the database name to connect to.
    ///
    /// # Arguments
    /// * `database` - The database name
    fn db(mut self_: PyRefMut<Self>, database: Option<String>) -> PyRefMut<Self> {
        self_.inner.db = database;
        self_
    }

    /// Enable or disable TCP_NODELAY socket option.
    ///
    /// When enabled, disables Nagle's algorithm for lower latency.
    /// Only affects TCP connections (Unix sockets are not affected).
    ///
    /// # Arguments
    /// * `enable` - Whether to enable TCP_NODELAY (default: true)
    fn tcp_nodelay(mut self_: PyRefMut<Self>, enable: bool) -> PyRefMut<Self> {
        self_.inner.tcp_nodelay = enable;
        self_
    }

    /// Enable or disable compression for the connection.
    ///
    /// # Arguments
    /// * `enable` - Whether to enable compression (default: false)
    fn compress(mut self_: PyRefMut<Self>, enable: bool) -> PyRefMut<Self> {
        self_.inner.compress = enable;
        self_
    }

    /// Enable or disable automatic upgrade from TCP to Unix socket.
    ///
    /// When enabled and connected via TCP, the driver will query `SELECT @@socket`
    /// and reconnect using the Unix socket for better performance.
    ///
    /// # Arguments
    /// * `enable` - Whether to enable upgrade to Unix socket (default: true)
    fn upgrade_to_unix_socket(mut self_: PyRefMut<Self>, enable: bool) -> PyRefMut<Self> {
        self_.inner.upgrade_to_unix_socket = enable;
        self_
    }

    /// Set an SQL command to execute immediately after connection is established.
    ///
    /// # Arguments
    /// * `command` - SQL command to execute on connect
    fn init_command(mut self_: PyRefMut<Self>, command: Option<String>) -> PyRefMut<Self> {
        self_.inner.init_command = command;
        self_
    }

    /// Set a custom buffer pool for connection.
    ///
    /// # Arguments
    /// * `pool` - A BufferPool instance to use for this connection
    fn buffer_pool(mut self_: PyRefMut<Self>, pool: BufferPool) -> PyRefMut<Self> {
        self_.inner.buffer_pool = pool.inner;
        self_
    }

    /// Set MySQL client capability flags.
    ///
    /// # Arguments
    /// * `capabilities` - Capability flags as a u32 bitmask
    fn capabilities(mut self_: PyRefMut<Self>, capabilities: u32) -> PyRefMut<Self> {
        self_.inner.capabilities =
            zero_mysql::constant::CapabilityFlags::from_bits_truncate(capabilities);
        self_
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

impl Opts {
    /// Convert to mysql_async::Opts
    pub fn to_mysql_async_opts(&self) -> mysql_async::Opts {
        let mut builder = mysql_async::OptsBuilder::default();

        if let Some(host) = &self.inner.host {
            builder = builder.ip_or_hostname(host);
        }
        builder = builder.tcp_port(self.inner.port);

        if let Some(socket) = &self.inner.socket {
            builder = builder.socket(Some(socket));
        }

        builder = builder.user(Some(&self.inner.user));

        if let Some(password) = &self.inner.password {
            builder = builder.pass(Some(password));
        }

        if let Some(db) = &self.inner.db {
            builder = builder.db_name(Some(db));
        }

        builder = builder.tcp_nodelay(self.inner.tcp_nodelay);

        if self.inner.compress {
            builder = builder.compression(Some(mysql_async::Compression::default()));
        }

        builder.into()
    }

    /// Convert to mysql::Opts
    pub fn to_mysql_opts(&self) -> mysql::Opts {
        let mut builder = mysql::OptsBuilder::default();

        if let Some(host) = &self.inner.host {
            builder = builder.ip_or_hostname(Some(host));
        }
        builder = builder.tcp_port(self.inner.port);

        if let Some(socket) = &self.inner.socket {
            builder = builder.socket(Some(socket));
        }

        builder = builder.user(Some(&self.inner.user));

        if let Some(password) = &self.inner.password {
            builder = builder.pass(Some(password));
        }

        if let Some(db) = &self.inner.db {
            builder = builder.db_name(Some(db));
        }

        builder = builder.tcp_nodelay(self.inner.tcp_nodelay);

        if self.inner.compress {
            builder = builder.compress(Some(mysql::Compression::default()));
        }

        builder.into()
    }
}
