use either::Either;
use mysql::{Binary, QueryResult, ResultSet, Text};
use pyo3::{exceptions::PyStopIteration, prelude::*};

use crate::{error::Error, row::Row};

// TODO: cover both Text and Binary
#[pyclass(module = "pyro_mysql.sync")]
pub struct ResultSetIterator {
    pub owner: Py<PyAny>, // To keep the owner alive for the lifetime of the iterator
    pub inner: Either<
        QueryResult<'static, 'static, 'static, Text>,
        QueryResult<'static, 'static, 'static, Binary>,
    >,
}

#[pymethods]
impl ResultSetIterator {
    // Iterator is also Iterable
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __next__(slf: Py<Self>, py: Python) -> PyResult<RowIterator> {
        match &mut slf.borrow_mut(py).inner {
            Either::Left(text) => text
                .iter()
                .map(|x| RowIterator {
                    inner: Either::Left(unsafe {
                        std::mem::transmute::<
                            mysql::ResultSet<'_, '_, '_, '_, mysql::Text>,
                            mysql::ResultSet<'_, '_, '_, '_, mysql::Text>,
                        >(x)
                    }),
                })
                .ok_or_else(|| PyStopIteration::new_err("ResultSet exhausted")),
            Either::Right(binary) => binary
                .iter()
                .map(|x| RowIterator {
                    inner: Either::Right(unsafe {
                        std::mem::transmute::<
                            mysql::ResultSet<'_, '_, '_, '_, mysql::Binary>,
                            mysql::ResultSet<'_, '_, '_, '_, mysql::Binary>,
                        >(x)
                    }),
                })
                .ok_or_else(|| PyStopIteration::new_err("ResultSet exhausted")),
        }
    }
}

#[pyclass]
pub struct RowIterator {
    // ResultSet holds a reference to QueryResult
    pub inner: Either<
        ResultSet<'static, 'static, 'static, 'static, Text>,
        ResultSet<'static, 'static, 'static, 'static, Binary>,
    >,
}

#[pymethods]
impl RowIterator {
    // Iterator is also Iterable
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __next__(&mut self) -> PyResult<Row> {
        Ok(Row {
            inner: crate::row::RowInner::MysqlCommon(
                self
                    .inner
                    .next()
                    .ok_or_else(|| PyStopIteration::new_err("Row exhausted"))?
                    .map_err(Error::from)?
            ),
        })
    }
}
