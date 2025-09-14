pub mod conn;
pub mod isolation_level;
pub mod params;
pub mod pool;
pub mod queryable;
pub mod row;
pub mod sync;
pub mod transaction;
pub mod util;
pub mod value;

use conn::Conn;
use pool::Pool;
use pyo3::prelude::*;
use tokio::runtime::Builder;

use crate::{isolation_level::IsolationLevel, row::Row, transaction::Transaction};

#[pyfunction]
/// This function can be called multiple times until any async operation is called.
#[pyo3(signature = (worker_threads=Some(1), thread_name=None))]
fn init(worker_threads: Option<usize>, thread_name: Option<&str>) {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    if let Some(n) = worker_threads {
        builder.worker_threads(n);
    }
    if let Some(name) = thread_name {
        builder.thread_name(name);
    }
    pyo3_async_runtimes::tokio::init(builder);
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyro_mysql(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    if cfg!(debug_assertions) {
        println!("Running in Debug mode.");
    } else {
        println!("Running in Release mode.");
    }

    init(Some(1), None);
    m.add_function(wrap_pyfunction!(init, m)?)?;
    m.add_class::<Row>()?;
    m.add_class::<Pool>()?;
    m.add_class::<Conn>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<IsolationLevel>()?;
    m.add_class::<sync::conn::SyncConn>()?;
    Ok(())
}
