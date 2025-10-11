use chrono::{DateTime, Timelike, Utc};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDate, PyDateTime, PyTime};

#[pyfunction]
#[pyo3(name = "Date")]
pub fn date(py: Python, year: i32, month: u8, day: u8) -> PyResult<Bound<PyDate>> {
    PyDate::new(py, year, month, day)
}

#[pyfunction]
#[pyo3(name = "Time")]
pub fn time(py: Python, hour: u8, minute: u8, second: u8) -> PyResult<Bound<PyTime>> {
    PyTime::new(py, hour, minute, second, 0, None)
}

#[pyfunction]
#[pyo3(name = "Timestamp")]
pub fn timestamp(
    py: Python,
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
) -> PyResult<Bound<PyDateTime>> {
    PyDateTime::new(py, year, month, day, hour, minute, second, 0, None)
}

#[pyfunction]
#[pyo3(name = "DateFromTicks")]
pub fn date_from_ticks(py: Python, ticks: i64) -> PyResult<Bound<PyDate>> {
    PyDate::from_timestamp(py, ticks)
}

#[pyfunction]
#[pyo3(name = "TimeFromTicks")]
pub fn time_from_ticks(py: Python, ticks: i64) -> PyResult<Bound<PyTime>> {
    let dt = DateTime::from_timestamp(ticks, 0)
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid timestamp: {}", ticks))
        })?
        .with_timezone(&Utc);
    PyTime::new(py, dt.hour() as u8, dt.minute() as u8, dt.second() as u8, 0, None)
}

#[pyfunction]
#[pyo3(name = "TimestampFromTicks")]
pub fn timestamp_from_ticks(py: Python, ticks: f64) -> PyResult<Bound<PyDateTime>> {
    PyDateTime::from_timestamp(py, ticks, None)
}

#[pyfunction]
#[pyo3(name = "Binary")]
pub fn binary(py: Python, x: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    // Mimics Python's bytes(x) behavior
    let bytes_type = py.get_type::<PyBytes>();
    let result = bytes_type.call1((x,))?;
    Ok(result.extract::<Py<PyBytes>>()?)
}
