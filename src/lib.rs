#![allow(async_fn_in_trait)]

pub mod r#async;
pub mod capability_flags;
pub mod error;
pub mod isolation_level;
pub mod params;
pub mod row;
pub mod sync;
pub mod util;
pub mod value;

use pyo3::prelude::*;
use tokio::runtime::Builder;

use crate::{
    r#async::{
        AsyncOpts, AsyncOptsBuilder, AsyncPoolOpts, conn::AsyncConn, pool::AsyncPool,
        transaction::AsyncTransaction,
    },
    capability_flags::CapabilityFlags,
    isolation_level::IsolationLevel,
    row::Row,
    sync::{
        SyncConn, SyncPool, SyncPoolOpts, SyncPooledConn, SyncTransaction,
        opts::{SyncOpts, SyncOptsBuilder},
    },
    util::PyroFuture,
};

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
mod pyro_mysql {
    use super::*;

    #[pymodule_export]
    use super::init;

    #[pymodule_export]
    use super::Row;

    #[pymodule_export]
    use super::IsolationLevel;

    #[pymodule_export]
    use super::CapabilityFlags;

    #[pymodule_export]
    use super::PyroFuture;

    #[pymodule]
    mod error {
        use crate::error as error_types;

        #[pymodule_export]
        use error_types::IncorrectApiUsageError;

        #[pymodule_export]
        use error_types::UrlError;

        #[pymodule_export]
        use error_types::MysqlError;

        #[pymodule_export]
        use error_types::ConnectionClosedError;

        #[pymodule_export]
        use error_types::TransactionClosedError;

        #[pymodule_export]
        use error_types::BuilderConsumedError;

        #[pymodule_export]
        use error_types::DecodeError;
    }

    #[pymodule]
    mod async_ {
        #[pymodule_export]
        use crate::r#async::pool::AsyncPool;

        #[pymodule_export]
        use crate::r#async::conn::AsyncConn;

        #[pymodule_export]
        use crate::r#async::transaction::AsyncTransaction;

        #[pymodule_export]
        use crate::r#async::AsyncOpts;

        #[pymodule_export]
        use crate::r#async::AsyncOptsBuilder;

        #[pymodule_export]
        use crate::r#async::AsyncPoolOpts;
    }

    #[pymodule]
    mod sync {
        #[pymodule_export]
        use crate::sync::SyncConn;

        #[pymodule_export]
        use crate::sync::SyncPool;

        #[pymodule_export]
        use crate::sync::SyncPooledConn;

        #[pymodule_export]
        use crate::sync::SyncTransaction;

        #[pymodule_export]
        use crate::sync::opts::SyncOpts;

        #[pymodule_export]
        use crate::sync::opts::SyncOptsBuilder;

        #[pymodule_export]
        use crate::sync::SyncPoolOpts;

        #[pymodule_export]
        use crate::sync::iterator::ResultSetIterator;
    }

    #[pymodule_init]
    fn module_init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        pyo3_log::init();

        if cfg!(debug_assertions) {
            log::debug!("Running in Debug mode.");
        } else {
            log::debug!("Running in Release mode.");
        }

        super::init(Some(1), None);

        // ─── Alias ───────────────────────────────────────────────────
        Python::attach(|py| {
            m.add("AsyncPool", py.get_type::<super::AsyncPool>())?;
            m.add("AsyncConn", py.get_type::<super::AsyncConn>())?;
            m.add("AsyncOpts", py.get_type::<super::AsyncOpts>())?;
            m.add("AsyncOptsBuilder", py.get_type::<super::AsyncOptsBuilder>())?;
            m.add("AsyncPoolOpts", py.get_type::<super::AsyncPoolOpts>())?;
            m.add("AsyncTransaction", py.get_type::<super::AsyncTransaction>())?;
            m.add("SyncConn", py.get_type::<super::SyncConn>())?;
            m.add("SyncOpts", py.get_type::<super::SyncOpts>())?;
            m.add("SyncOptsBuilder", py.get_type::<super::SyncOptsBuilder>())?;
            m.add("SyncPool", py.get_type::<super::SyncPool>())?;
            m.add("SyncPoolOpts", py.get_type::<super::SyncPoolOpts>())?;
            m.add("SyncPooledConn", py.get_type::<super::SyncPooledConn>())?;
            m.add("SyncTransaction", py.get_type::<super::SyncTransaction>())?;
            PyResult::Ok(())
        })?;

        let py = m.py();
        let sys_modules = py.import("sys")?.getattr("modules")?;
        for module in ["error", "sync", "async_"] {
            sys_modules.set_item(format!("pyro_mysql.{module}"), m.getattr(module)?)?;
        }

        Ok(())
    }
}
