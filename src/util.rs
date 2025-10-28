use std::future::Future;

use futures::future::{AbortHandle, Abortable};
use mysql_common::Value as MySqlValue;
use mysql_common::params::Params as MySqlParams;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use tokio_util::task::AbortOnDropHandle;

use crate::error::Error;
use crate::error::PyroResult;
use crate::params::Params;

pub fn mysql_error_to_pyerr(error: mysql_async::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyException, _>(format!("MySQL Error: {}", error))
}

pub fn url_error_to_pyerr(error: mysql_async::UrlError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyException, _>(format!("MySQL Error: {}", error))
}

/// A wrapper around a Python future that cancels the associated Rust future when dropped.
#[pyclass(module = "pyro_mysql")]
pub struct PyroFuture {
    py_future: Py<PyAny>,
    abort_handle: AbortHandle,
}

/// Iterator wrapper that keeps RaiiFuture alive during iteration
#[pyclass]
struct PyroFutureIterator {
    iterator: Py<PyAny>,
    _future: Py<PyroFuture>, // Keep the future alive
}

#[pymethods]
impl PyroFutureIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.iterator.bind(py).call_method0("__next__")
    }

    fn send<'py>(&self, py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        self.iterator.bind(py).call_method1("send", (value,))
    }

    #[pyo3(signature = (*args))]
    fn throw<'py>(
        &self,
        py: Python<'py>,
        args: &'py Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.iterator.bind(py).call_method1("throw", args)
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<()> {
        match self.iterator.bind(py).call_method0("close") {
            Ok(_) => Ok(()),
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl Drop for PyroFuture {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

#[pymethods]
impl PyroFuture {
    fn __await__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Py<PyroFutureIterator>> {
        // Get the iterator from the underlying future
        let awaitable = slf.py_future.bind(py);
        let iterator = awaitable.call_method0("__await__")?;

        // Create our wrapper iterator that keeps self alive
        Py::new(
            py,
            PyroFutureIterator {
                iterator: iterator.unbind(),
                _future: slf.into_pyobject(py)?.unbind(),
            },
        )
    }

    fn cancel<'py>(&mut self, py: Python<'py>) -> PyResult<bool> {
        // mark the Python future as cancelled, making await on __await__ raises
        let py_future = self.py_future.bind(py);
        let result = py_future.call_method0("cancel")?;

        // Also abort the Rust future
        self.abort_handle.abort();

        result.extract()
    }

    fn get_loop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let py_future = self.py_future.bind(py);
        py_future.call_method0("get_loop")
    }
}

pub fn tokio_spawn_as_abort_on_drop<F>(fut: F) -> AbortOnDropHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    AbortOnDropHandle::new(pyo3_async_runtimes::tokio::get_runtime().spawn(fut))
}

/// == Coroutine::new(AbortOnDropHandle::new(pyo3_async_runtimes::tokio::get_runtime().spawn(fut)))
pub fn rust_future_into_py<F, T>(py: Python<'_>, fut: F) -> PyResult<Py<PyroFuture>>
where
    F: Future<Output = PyroResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py>,
{
    // Get the event loop and create a future
    let event_loop = pyo3_async_runtimes::get_running_loop(py)?;
    let py_fut = event_loop.call_method0("create_future")?;
    let future_py = py_fut.clone().unbind();

    // Create an abortable future
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let abortable_fut = Abortable::new(fut, abort_registration);

    // Spawn the task
    let event_loop = event_loop.unbind();
    pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        let result = match abortable_fut.await {
            Ok(result) => result,
            Err(_) => Err(Error::PythonCancelledError),
        };

        // Set the result on the Python future
        Python::attach(|py| {
            let py_fut = future_py.bind(py);

            // Check if user already cancelled the future
            // TODO: move this check inside call_soon_threadsafe
            let cancelled = py_fut
                .call_method0("cancelled")
                .map(|v| v.is_truthy().unwrap_or(false))
                .unwrap_or(false);

            if cancelled {
                return;
            }

            match result {
                Ok(value) => {
                    event_loop
                        .call_method1(
                            py,
                            "call_soon_threadsafe",
                            (
                                py_fut.getattr("set_result").unwrap(),
                                value.into_py_any(py).unwrap(),
                            ),
                        )
                        .unwrap();
                }
                Err(err) => {
                    event_loop
                        .call_method1(
                            py,
                            "call_soon_threadsafe",
                            (
                                py_fut.getattr("set_exception").unwrap(),
                                pyo3::PyErr::from(err).into_bound_py_any(py).unwrap(),
                            ),
                        )
                        .unwrap();
                }
            }
        });
    });

    let raii_future = Py::new(
        py,
        PyroFuture {
            py_future: py_fut.unbind(),
            abort_handle,
        },
    )?;

    Ok(raii_future)
}

/// Converts a MySqlValue to its SQL string representation
fn value_to_sql_string(value: &MySqlValue) -> String {
    match value {
        MySqlValue::NULL => "NULL".to_string(),
        MySqlValue::Int(i) => i.to_string(),
        MySqlValue::UInt(u) => u.to_string(),
        MySqlValue::Float(f) => f.to_string(),
        MySqlValue::Double(d) => d.to_string(),
        MySqlValue::Bytes(b) => {
            // Escape and quote the bytes as a string
            let mut escaped = String::with_capacity(b.len() + 2);
            escaped.push('\'');
            for &byte in b.iter() {
                match byte {
                    b'\0' => escaped.push_str("\\0"),
                    b'\'' => escaped.push_str("\\'"),
                    b'"' => escaped.push_str("\\\""),
                    b'\x08' => escaped.push_str("\\b"),
                    b'\n' => escaped.push_str("\\n"),
                    b'\r' => escaped.push_str("\\r"),
                    b'\t' => escaped.push_str("\\t"),
                    b'\\' => escaped.push_str("\\\\"),
                    _ => escaped.push(byte as char),
                }
            }
            escaped.push('\'');
            escaped
        }
        MySqlValue::Date(year, month, day, hour, minute, second, microsecond) => {
            if *hour == 0 && *minute == 0 && *second == 0 && *microsecond == 0 {
                // Date only
                format!("'{:04}-{:02}-{:02}'", year, month, day)
            } else if *microsecond == 0 {
                // DateTime without microseconds
                format!(
                    "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'",
                    year, month, day, hour, minute, second
                )
            } else {
                // DateTime with microseconds
                format!(
                    "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
                    year, month, day, hour, minute, second, microsecond
                )
            }
        }
        MySqlValue::Time(is_negative, days, hours, minutes, seconds, microseconds) => {
            let total_hours = days * 24 + *hours as u32;
            let sign = if *is_negative { "-" } else { "" };
            if *microseconds == 0 {
                format!("'{}{:02}:{:02}:{:02}'", sign, total_hours, minutes, seconds)
            } else {
                format!(
                    "'{}{:02}:{:02}:{:02}.{:06}'",
                    sign, total_hours, minutes, seconds, microseconds
                )
            }
        }
    }
}

/// Returns the SQL query string with parameters interpolated.
/// This is useful for debugging to see what the final SQL would look like.
///
/// # Arguments
/// * `sql` - The SQL query with placeholders (? for positional, :name for named)
/// * `params` - The parameters to interpolate
///
/// # Example
/// ```
/// use pyro_mysql::util::mogrify;
/// use pyro_mysql::params::Params;
///
/// let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
/// let params = ...; // Some Params instance
/// let result = mogrify(sql, &params);
/// ```
#[pyfunction]
#[pyo3(signature = (sql, params))]
pub fn mogrify(sql: &str, params: Params) -> String {
    mogrify_internal(sql, &params)
}

/// Internal implementation of mogrify
fn mogrify_internal(sql: &str, params: &Params) -> String {
    match &params.inner {
        MySqlParams::Empty => sql.to_string(),

        MySqlParams::Positional(values) => {
            let mut result = String::with_capacity(sql.len() * 2);
            let mut chars = sql.chars().peekable();
            let mut param_index = 0;

            while let Some(ch) = chars.next() {
                if ch == '?' && param_index < values.len() {
                    // Replace ? with the parameter value
                    result.push_str(&value_to_sql_string(&values[param_index]));
                    param_index += 1;
                } else if ch == '\'' || ch == '"' {
                    // Handle quoted strings - don't replace ? inside quotes
                    result.push(ch);
                    let quote = ch;
                    let mut escaped = false;
                    while let Some(ch) = chars.next() {
                        result.push(ch);
                        if escaped {
                            escaped = false;
                        } else if ch == '\\' {
                            escaped = true;
                        } else if ch == quote {
                            break;
                        }
                    }
                } else {
                    result.push(ch);
                }
            }

            result
        }

        MySqlParams::Named(map) => {
            let mut result = String::with_capacity(sql.len() * 2);
            let mut chars = sql.chars().peekable();

            while let Some(ch) = chars.next() {
                if ch == ':' {
                    // Check if this is a named parameter
                    let mut param_name = String::new();
                    while let Some(&next_ch) = chars.peek() {
                        if next_ch.is_alphanumeric() || next_ch == '_' {
                            param_name.push(next_ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }

                    if !param_name.is_empty() {
                        // Look up the parameter value
                        if let Some(value) = map.get(param_name.as_bytes()) {
                            result.push_str(&value_to_sql_string(value));
                        } else {
                            // Parameter not found, keep the original placeholder
                            result.push(':');
                            result.push_str(&param_name);
                        }
                    } else {
                        result.push(':');
                    }
                } else if ch == '\'' || ch == '"' {
                    // Handle quoted strings - don't replace :name inside quotes
                    result.push(ch);
                    let quote = ch;
                    let mut escaped = false;
                    while let Some(ch) = chars.next() {
                        result.push(ch);
                        if escaped {
                            escaped = false;
                        } else if ch == '\\' {
                            escaped = true;
                        } else if ch == quote {
                            break;
                        }
                    }
                } else {
                    result.push(ch);
                }
            }

            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_common::Value as MySqlValue;
    use mysql_common::params::Params as MySqlParams;

    #[test]
    fn test_mogrify_positional_params() {
        let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
        let params = Params {
            inner: MySqlParams::Positional(vec![
                MySqlValue::Int(42),
                MySqlValue::Bytes("John".as_bytes().to_vec()),
            ]),
        };

        let result = mogrify_internal(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE id = 42 AND name = 'John'"
        );
    }

    #[test]
    fn test_mogrify_named_params() {
        let sql = "SELECT * FROM users WHERE id = :id AND name = :name";
        let mut map = std::collections::HashMap::new();
        map.insert("id".as_bytes().to_vec(), MySqlValue::Int(42));
        map.insert(
            "name".as_bytes().to_vec(),
            MySqlValue::Bytes("John".as_bytes().to_vec()),
        );

        let params = Params {
            inner: MySqlParams::Named(map),
        };

        let result = mogrify_internal(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE id = 42 AND name = 'John'"
        );
    }

    #[test]
    fn test_mogrify_empty_params() {
        let sql = "SELECT * FROM users";
        let params = Params {
            inner: MySqlParams::Empty,
        };

        let result = mogrify_internal(sql, &params);
        assert_eq!(result, "SELECT * FROM users");
    }

    #[test]
    fn test_mogrify_null_value() {
        let sql = "INSERT INTO users (name) VALUES (?)";
        let params = Params {
            inner: MySqlParams::Positional(vec![MySqlValue::NULL]),
        };

        let result = mogrify_internal(sql, &params);
        assert_eq!(result, "INSERT INTO users (name) VALUES (NULL)");
    }

    #[test]
    fn test_mogrify_string_escaping() {
        let sql = "SELECT * FROM users WHERE name = ?";
        let params = Params {
            inner: MySqlParams::Positional(vec![MySqlValue::Bytes("O'Brien".as_bytes().to_vec())]),
        };

        let result = mogrify_internal(sql, &params);
        assert_eq!(result, "SELECT * FROM users WHERE name = 'O\\'Brien'");
    }

    #[test]
    fn test_mogrify_date_values() {
        let sql = "SELECT * FROM events WHERE created_at = ?";
        let params = Params {
            inner: MySqlParams::Positional(vec![MySqlValue::Date(2024, 1, 15, 10, 30, 45, 0)]),
        };

        let result = mogrify_internal(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM events WHERE created_at = '2024-01-15 10:30:45'"
        );
    }

    #[test]
    fn test_mogrify_quoted_string_with_placeholder() {
        // Ensure ? inside quotes is not replaced
        let sql = "SELECT * FROM users WHERE name = ? AND comment = 'This is a ? mark'";
        let params = Params {
            inner: MySqlParams::Positional(vec![MySqlValue::Bytes("John".as_bytes().to_vec())]),
        };

        let result = mogrify_internal(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE name = 'John' AND comment = 'This is a ? mark'"
        );
    }
}
