//! Cached Python standard library imports
//!
//! This module provides cached access to commonly used Python standard library
//! classes and modules. Using PyOnceLock ensures that imports are only performed
//! once and then reused, improving performance.

use pyo3::{prelude::*, sync::PyOnceLock, types::PyModule};

static DATETIME_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DATE_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static TIMEDELTA_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DECIMAL_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static JSON_MODULE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

pub fn get_datetime_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(DATETIME_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "datetime")
                .unwrap()
                .getattr("datetime")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

pub fn get_date_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(DATE_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "datetime")
                .unwrap()
                .getattr("date")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

pub fn get_timedelta_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(TIMEDELTA_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "datetime")
                .unwrap()
                .getattr("timedelta")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

pub fn get_decimal_class<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyAny>> {
    Ok(DECIMAL_CLASS
        .get_or_init(py, || {
            PyModule::import(py, "decimal")
                .unwrap()
                .getattr("Decimal")
                .unwrap()
                .unbind()
        })
        .bind(py))
}

pub fn get_json_module<'py>(py: Python<'py>) -> PyResult<&'py Bound<'py, PyModule>> {
    Ok(JSON_MODULE
        .get_or_init(py, || PyModule::import(py, "json").unwrap().unbind())
        .bind(py))
}
