use pyo3::prelude::*;
use std::time::Duration;

use crate::error::{Error, PyroResult};

#[pyclass(module = "pyro_mysql.async_", name = "Opts")]
#[derive(Clone)]
pub struct AsyncOpts {
    pub opts: mysql_async::Opts,
}

#[pymethods]
impl AsyncOpts {
    pub fn pool_opts(&self, pool_opts: crate::r#async::pool_opts::AsyncPoolOpts) -> Self {
        let builder = mysql_async::OptsBuilder::from_opts(self.opts.clone());
        let new_opts = builder.pool_opts(pool_opts.inner).into();
        Self { opts: new_opts }
    }
}

#[pyclass(module = "pyro_mysql.async_", name = "OptsBuilder")]
pub struct AsyncOptsBuilder {
    builder: Option<mysql_async::OptsBuilder>,
}

impl Default for AsyncOptsBuilder {
    fn default() -> Self {
        Self {
            builder: Some(mysql_async::OptsBuilder::default()),
        }
    }
}

#[pymethods]
impl AsyncOptsBuilder {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    #[staticmethod]
    fn from_opts(opts: &AsyncOpts) -> Self {
        Self {
            builder: Some(mysql_async::OptsBuilder::from_opts(opts.opts.clone())),
        }
    }

    #[staticmethod]
    fn from_url(url: &str) -> PyroResult<Self> {
        let opts = mysql_async::Opts::from_url(url)?;
        Ok(Self {
            builder: Some(mysql_async::OptsBuilder::from_opts(opts)),
        })
    }

    // Network/Connection Options
    fn ip_or_hostname(mut self_: PyRefMut<Self>, hostname: String) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.ip_or_hostname(hostname));
        Ok(self_)
    }

    fn tcp_port(mut self_: PyRefMut<Self>, port: u16) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.tcp_port(port));
        Ok(self_)
    }

    fn socket(mut self_: PyRefMut<Self>, path: Option<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.socket(path));
        Ok(self_)
    }

    // Note: bind_address is not available in mysql_async::OptsBuilder
    // Removed as it doesn't exist in the upstream crate

    // Authentication Options
    fn user(mut self_: PyRefMut<Self>, username: Option<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.user(username));
        Ok(self_)
    }

    fn password(mut self_: PyRefMut<Self>, password: Option<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.pass(password));
        Ok(self_)
    }

    fn db_name(mut self_: PyRefMut<Self>, database: Option<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.db_name(database));
        Ok(self_)
    }

    fn secure_auth(mut self_: PyRefMut<Self>, enable: bool) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.secure_auth(enable));
        Ok(self_)
    }

    fn wait_timeout(mut self_: PyRefMut<Self>, seconds: Option<usize>) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.wait_timeout(seconds));
        Ok(self_)
    }

    fn stmt_cache_size(mut self_: PyRefMut<Self>, size: usize) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.stmt_cache_size(size));
        Ok(self_)
    }

    // Additional Options
    fn tcp_nodelay(mut self_: PyRefMut<Self>, enable: bool) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.tcp_nodelay(enable));
        Ok(self_)
    }

    fn tcp_keepalive(
        mut self_: PyRefMut<Self>,
        keepalive_ms: Option<u32>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.tcp_keepalive(keepalive_ms));
        Ok(self_)
    }

    fn max_allowed_packet(
        mut self_: PyRefMut<Self>,
        max_allowed_packet: Option<usize>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.max_allowed_packet(max_allowed_packet));
        Ok(self_)
    }

    fn prefer_socket(mut self_: PyRefMut<Self>, prefer_socket: bool) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.prefer_socket(prefer_socket));
        Ok(self_)
    }

    fn init(mut self_: PyRefMut<Self>, commands: Vec<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.init(commands));
        Ok(self_)
    }

    // Note: connect_attrs is not available in mysql_async::OptsBuilder
    // Removed as it doesn't exist in the upstream crate

    fn compression(mut self_: PyRefMut<Self>, level: Option<u32>) -> PyroResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;

        if let Some(level) = level {
            if level > 9 {
                return Err(Error::IncorrectApiUsageError(
                    "the compress level should be 0-9",
                ));
            }
            self_.builder = Some(builder.compression(Some(mysql_async::Compression::new(level))));
        } else {
            self_.builder = Some(builder.compression(None));
        }
        Ok(self_)
    }

    fn ssl_opts(self_: PyRefMut<Self>, _opts: Option<Py<PyAny>>) -> PyRefMut<Self> {
        // Note: This would need a separate SslOpts wrapper class
        // For now, leaving as placeholder
        self_
    }

    fn local_infile_handler(_self_: PyRefMut<Self>, _handler: Option<Py<PyAny>>) -> PyRefMut<Self> {
        todo!()
    }

    fn pool_opts(
        mut self_: PyRefMut<Self>,
        opts: crate::r#async::pool_opts::AsyncPoolOpts,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.pool_opts(opts.inner));
        Ok(self_)
    }

    fn enable_cleartext_plugin(
        mut self_: PyRefMut<Self>,
        enable: bool,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.enable_cleartext_plugin(enable));
        Ok(self_)
    }

    fn client_found_rows(mut self_: PyRefMut<Self>, enable: bool) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.client_found_rows(enable));
        Ok(self_)
    }

    fn conn_ttl(mut self_: PyRefMut<Self>, ttl_seconds: Option<f64>) -> PyResult<PyRefMut<Self>> {
        let duration = ttl_seconds.map(Duration::from_secs_f64);
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.conn_ttl(duration));
        Ok(self_)
    }

    fn setup(mut self_: PyRefMut<Self>, commands: Vec<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        self_.builder = Some(builder.setup(commands));
        Ok(self_)
    }

    // Build the final Opts
    fn build(mut self_: PyRefMut<Self>) -> PyResult<AsyncOpts> {
        let builder = self_
            .builder
            .take()
            .ok_or_else(|| Error::BuilderConsumedError)?;
        Ok(AsyncOpts {
            opts: builder.into(),
        })
    }
}
