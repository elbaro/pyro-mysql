use pyo3::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

#[pyclass]
#[derive(Clone)]
pub struct SyncOpts {
    pub opts: mysql::Opts,
}

#[pyclass]
pub struct SyncOptsBuilder {
    builder: Option<mysql::OptsBuilder>,
}

impl Default for SyncOptsBuilder {
    fn default() -> Self {
        Self {
            builder: Some(mysql::OptsBuilder::new()),
        }
    }
}

#[pymethods]
impl SyncOptsBuilder {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    #[staticmethod]
    fn from_opts(opts: &SyncOpts) -> Self {
        Self {
            builder: Some(mysql::OptsBuilder::from_opts(opts.opts.clone())),
        }
    }

    fn from_hash_map(
        mut self_: PyRefMut<Self>,
        params: HashMap<String, String>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(
            builder
                .from_hash_map(&params)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?,
        );
        Ok(self_)
    }

    // Network/Connection Options
    fn ip_or_hostname(
        mut self_: PyRefMut<Self>,
        hostname: Option<String>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.ip_or_hostname(hostname));
        Ok(self_)
    }

    fn tcp_port(mut self_: PyRefMut<Self>, port: u16) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.tcp_port(port));
        Ok(self_)
    }

    fn socket(mut self_: PyRefMut<Self>, path: Option<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.socket(path));
        Ok(self_)
    }

    fn bind_address(
        mut self_: PyRefMut<Self>,
        address: Option<String>,
    ) -> PyResult<PyRefMut<Self>> {
        let addr = if let Some(addr_str) = address {
            Some(addr_str.parse::<SocketAddr>().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid socket address: {}",
                    e
                ))
            })?)
        } else {
            None
        };
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.bind_address(addr));
        Ok(self_)
    }

    // Authentication Options
    fn user(mut self_: PyRefMut<Self>, username: Option<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.user(username));
        Ok(self_)
    }

    fn pass(mut self_: PyRefMut<Self>, password: Option<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.pass(password));
        Ok(self_)
    }

    fn db_name(mut self_: PyRefMut<Self>, database: Option<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.db_name(database));
        Ok(self_)
    }

    fn secure_auth(mut self_: PyRefMut<Self>, enable: bool) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.secure_auth(enable));
        Ok(self_)
    }

    // Performance/Timeout Options
    fn read_timeout(mut self_: PyRefMut<Self>, seconds: Option<f64>) -> PyResult<PyRefMut<Self>> {
        let duration = seconds.map(|s| Duration::from_secs_f64(s));
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.read_timeout(duration));
        Ok(self_)
    }

    fn write_timeout(mut self_: PyRefMut<Self>, seconds: Option<f64>) -> PyResult<PyRefMut<Self>> {
        let duration = seconds.map(|s| Duration::from_secs_f64(s));
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.write_timeout(duration));
        Ok(self_)
    }

    fn tcp_connect_timeout(
        mut self_: PyRefMut<Self>,
        seconds: Option<f64>,
    ) -> PyResult<PyRefMut<Self>> {
        let duration = seconds.map(|s| Duration::from_secs_f64(s));
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.tcp_connect_timeout(duration));
        Ok(self_)
    }

    fn stmt_cache_size(mut self_: PyRefMut<Self>, size: usize) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.stmt_cache_size(size));
        Ok(self_)
    }

    // Additional Options
    fn tcp_nodelay(mut self_: PyRefMut<Self>, enable: bool) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.tcp_nodelay(enable));
        Ok(self_)
    }

    fn tcp_keepalive_time_ms(
        mut self_: PyRefMut<Self>,
        time_ms: Option<u32>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.tcp_keepalive_time_ms(time_ms));
        Ok(self_)
    }

    fn tcp_keepalive_probe_interval_secs(
        mut self_: PyRefMut<Self>,
        interval_secs: Option<u32>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.tcp_keepalive_probe_interval_secs(interval_secs));
        Ok(self_)
    }

    fn tcp_keepalive_probe_count(
        mut self_: PyRefMut<Self>,
        count: Option<u32>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.tcp_keepalive_probe_count(count));
        Ok(self_)
    }

    fn tcp_user_timeout_ms(
        mut self_: PyRefMut<Self>,
        timeout_ms: Option<u32>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.tcp_user_timeout_ms(timeout_ms));
        Ok(self_)
    }

    fn max_allowed_packet(
        mut self_: PyRefMut<Self>,
        max_allowed_packet: Option<usize>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.max_allowed_packet(max_allowed_packet));
        Ok(self_)
    }

    fn prefer_socket(mut self_: PyRefMut<Self>, prefer_socket: bool) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.prefer_socket(prefer_socket));
        Ok(self_)
    }

    fn init(mut self_: PyRefMut<Self>, commands: Vec<String>) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.init(commands));
        Ok(self_)
    }

    fn connect_attrs(
        mut self_: PyRefMut<Self>,
        attrs: Option<HashMap<String, String>>,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.connect_attrs(attrs));
        Ok(self_)
    }

    fn compress(mut self_: PyRefMut<Self>, level: Option<u32>) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;

        if let Some(level) = level {
            if level > 9 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "the compress level should be 0-9",
                ));
            }
            self_.builder = Some(builder.compress(Some(mysql::Compression::new(level))));
        } else {
            self_.builder = Some(builder.compress(None));
        }
        Ok(self_)
    }

    fn ssl_opts(mut self_: PyRefMut<Self>, opts: Option<Py<PyAny>>) -> PyRefMut<Self> {
        // Note: This would need a separate SslOpts wrapper class
        // For now, leaving as placeholder
        self_
    }

    fn local_infile_handler(
        mut self_: PyRefMut<Self>,
        handler: Option<Py<PyAny>>,
    ) -> PyRefMut<Self> {
        todo!()
    }

    fn pool_opts(mut self_: PyRefMut<Self>, opts: Option<Py<PyAny>>) -> PyRefMut<Self> {
        todo!()
    }

    fn additional_capabilities(
        mut self_: PyRefMut<Self>,
        capabilities: u32,
    ) -> PyResult<PyRefMut<Self>> {
        // Note: This would need CapabilityFlags wrapper, using u32 for now
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.additional_capabilities(
            mysql_common::constants::CapabilityFlags::from_bits_truncate(capabilities),
        ));
        Ok(self_)
    }

    fn enable_cleartext_plugin(
        mut self_: PyRefMut<Self>,
        enable: bool,
    ) -> PyResult<PyRefMut<Self>> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        self_.builder = Some(builder.enable_cleartext_plugin(enable));
        Ok(self_)
    }

    // Build the final Opts
    fn build(mut self_: PyRefMut<Self>) -> PyResult<SyncOpts> {
        let builder = self_.builder.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>("Builder already consumed")
        })?;
        Ok(SyncOpts {
            opts: builder.into(),
        })
    }
}
