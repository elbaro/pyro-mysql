use color_eyre::Result;
use mysql::{QueryResult, ResultSet};
use mysql_common::proto::Binary;
use pyo3::{exceptions::PyStopIteration, prelude::*};

use crate::row::Row;

#[pyclass]
pub struct ResultSetIterator {
    pub owner: Py<PyAny>,  // To keep the owner alive for the lifetime of the iterator
    pub inner: QueryResult<'static, 'static, 'static, Binary>,
}

#[pymethods]
impl ResultSetIterator {
    // Iterator is also Iterable
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __next__(slf: Py<Self>) -> PyResult<RowIterator> {
        Python::attach(|py| {
            let slf_clone = slf.clone_ref(py);
            slf.borrow_mut(py)
                .inner
                .iter()
                .map(|x| RowIterator {
                    owner: slf_clone,
                    inner: unsafe { std::mem::transmute::<_, _>(x) },
                })
                .ok_or_else(|| PyStopIteration::new_err("ResultSet exhausted"))
        })
    }
}

#[pyclass]
pub struct RowIterator {
    pub owner: Py<ResultSetIterator>, // To erase the lifetime, the owner should be alive
    pub inner: ResultSet<'static, 'static, 'static, 'static, Binary>,
}

#[pymethods]
impl RowIterator {
    // Iterator is also Iterable
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __next__(&mut self) -> Result<Row> {
        Ok(Row {
            inner: self
                .inner
                .next()
                .ok_or_else(|| PyStopIteration::new_err("Row exhausted"))??,
        })
    }
}
