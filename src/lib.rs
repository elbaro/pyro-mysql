#![allow(async_fn_in_trait)]

pub mod r#async;
pub mod capability_flags;
pub mod dbapi;
pub mod error;
pub mod isolation_level;
pub mod opts;
pub mod params;
pub mod py_imports;
pub mod row;
pub mod sync;
pub mod tokio_thread;
pub mod util;
pub mod value;
pub mod zero_mysql_util;

use pyo3::prelude::*;

use crate::{
    opts::Opts,
    r#async::{conn::AsyncConn, transaction::AsyncTransaction},
    capability_flags::CapabilityFlags,
    isolation_level::IsolationLevel,
    sync::conn::SyncConn,
    util::PyroFuture,
};

#[pyfunction]
/// Initialize the Tokio runtime thread.
/// This is called automatically when the module is loaded.
/// Note: worker_threads and thread_name parameters are ignored since we use a single-threaded runtime.
#[pyo3(signature = (worker_threads=None, thread_name=None))]
fn init(worker_threads: Option<usize>, thread_name: Option<&str>) {
    // Initialize the global TokioThread
    let _ = tokio_thread::get_tokio_thread();

    // Suppress unused variable warnings
    let _ = worker_threads;
    let _ = thread_name;
}

/// A Python module implemented in Rust.
#[pymodule(gil_used = false)]
mod pyro_mysql {
    use crate::sync::transaction::SyncTransaction;

    use super::*;

    #[pymodule_export]
    use super::init;

    #[pymodule_export]
    use super::IsolationLevel;

    #[pymodule_export]
    use super::CapabilityFlags;

    #[pymodule_export]
    use super::Opts;

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

        #[pymodule_export]
        use error_types::PoisonError;

        #[pymodule_export]
        use error_types::PythonObjectCreationError;
    }

    #[pymodule]
    mod async_ {
        #[pymodule_export]
        use crate::r#async::conn::AsyncConn;

        #[pymodule_export]
        use crate::r#async::transaction::AsyncTransaction;
    }

    #[pymodule]
    mod sync {
        #[pymodule_export]
        use crate::sync::conn::SyncConn;

        #[pymodule_export]
        use crate::sync::transaction::SyncTransaction;

        #[pymodule_export]
        use crate::sync::iterator::ResultSetIterator;
    }

    #[pymodule]
    mod dbapi {
        #[pymodule_export]
        use crate::dbapi::connect;

        // ─── Global Constant ─────────────────────────────────────────

        #[pymodule_export]
        #[allow(non_upper_case_globals)]
        const apilevel: &str = "2.0";

        #[pymodule_export]
        #[allow(non_upper_case_globals)]
        const threadsafety: u8 = 1;

        #[pymodule_export]
        #[allow(non_upper_case_globals)]
        const paramstyle: &str = "qmark";

        // ─── Error ───────────────────────────────────────────────────
        use crate::dbapi::error;

        #[pymodule_export]
        use error::Warning;

        #[pymodule_export]
        use error::Error;

        #[pymodule_export]
        use error::InterfaceError;

        #[pymodule_export]
        use error::DatabaseError;

        #[pymodule_export]
        use error::DataError;

        #[pymodule_export]
        use error::OperationalError;

        #[pymodule_export]
        use error::IntegrityError;

        #[pymodule_export]
        use error::InternalError;

        #[pymodule_export]
        use error::ProgrammingError;

        #[pymodule_export]
        use error::NotSupportedError;

        // ─── Main Class ──────────────────────────────────────────────
        #[pymodule_export]
        use crate::dbapi::conn::DbApiConn;

        #[pymodule_export]
        use crate::dbapi::cursor::Cursor;

        // ─── Type Constructor ────────────────────────────────────────
        #[pymodule_export]
        use crate::dbapi::type_constructor::date;

        #[pymodule_export]
        use crate::dbapi::type_constructor::time;

        #[pymodule_export]
        use crate::dbapi::type_constructor::timestamp;

        #[pymodule_export]
        use crate::dbapi::type_constructor::date_from_ticks;

        #[pymodule_export]
        use crate::dbapi::type_constructor::time_from_ticks;

        #[pymodule_export]
        use crate::dbapi::type_constructor::timestamp_from_ticks;

        #[pymodule_export]
        use crate::dbapi::type_constructor::binary;

        // ─── Type Object ─────────────────────────────────────────────
        #[pymodule_export]
        use crate::dbapi::type_object::TypeObject;

        #[pymodule_export]
        const BINARY: TypeObject = crate::dbapi::type_object::BINARY;

        #[pymodule_export]
        const DATETIME: TypeObject = crate::dbapi::type_object::DATETIME;

        #[pymodule_export]
        const NUMBER: TypeObject = crate::dbapi::type_object::NUMBER;

        #[pymodule_export]
        const ROWID: TypeObject = crate::dbapi::type_object::ROWID;

        #[pymodule_export]
        const STRING: TypeObject = crate::dbapi::type_object::STRING;
    }

    #[pymodule]
    mod dbapi_async {
        use pyo3::prelude::*;

        #[pymodule_export]
        use crate::dbapi::connect_async;

        // ─── Global Constant ─────────────────────────────────────────

        #[pymodule_export]
        #[allow(non_upper_case_globals)]
        const apilevel: &str = "2.0";

        #[pymodule_export]
        #[allow(non_upper_case_globals)]
        const threadsafety: u8 = 1;

        #[pymodule_export]
        #[allow(non_upper_case_globals)]
        const paramstyle: &str = "qmark";

        // ─── Error ───────────────────────────────────────────────────
        use crate::dbapi::error;

        #[pymodule_export]
        use error::Warning;

        #[pymodule_export]
        use error::Error;

        #[pymodule_export]
        use error::InterfaceError;

        #[pymodule_export]
        use error::DatabaseError;

        #[pymodule_export]
        use error::DataError;

        #[pymodule_export]
        use error::OperationalError;

        #[pymodule_export]
        use error::IntegrityError;

        #[pymodule_export]
        use error::InternalError;

        #[pymodule_export]
        use error::ProgrammingError;

        #[pymodule_export]
        use error::NotSupportedError;

        // ─── Main Class ──────────────────────────────────────────────
        #[pymodule_export]
        use crate::dbapi::async_conn::AsyncDbApiConn;

        #[pymodule_export]
        use crate::dbapi::async_cursor::AsyncCursor;

        // ─── Type Constructor ────────────────────────────────────────
        #[pymodule_export]
        use crate::dbapi::type_constructor::date;

        #[pymodule_export]
        use crate::dbapi::type_constructor::time;

        #[pymodule_export]
        use crate::dbapi::type_constructor::timestamp;

        #[pymodule_export]
        use crate::dbapi::type_constructor::date_from_ticks;

        #[pymodule_export]
        use crate::dbapi::type_constructor::time_from_ticks;

        #[pymodule_export]
        use crate::dbapi::type_constructor::timestamp_from_ticks;

        #[pymodule_export]
        use crate::dbapi::type_constructor::binary;

        // ─── Type Object ─────────────────────────────────────────────
        #[pymodule_export]
        use crate::dbapi::type_object::TypeObject;

        #[pymodule_export]
        const BINARY: TypeObject = crate::dbapi::type_object::BINARY;

        #[pymodule_export]
        const DATETIME: TypeObject = crate::dbapi::type_object::DATETIME;

        #[pymodule_export]
        const NUMBER: TypeObject = crate::dbapi::type_object::NUMBER;

        #[pymodule_export]
        const ROWID: TypeObject = crate::dbapi::type_object::ROWID;

        #[pymodule_export]
        const STRING: TypeObject = crate::dbapi::type_object::STRING;

        #[pymodule_init]
        fn module_init(m: &Bound<'_, PyModule>) -> PyResult<()> {
            let cls = m.getattr("Cursor")?; // AsyncCursor
            cls.setattr("__anext__", cls.getattr("fetchone")?)?;
            Ok(())
        }
    }

    #[pymodule_init]
    fn module_init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        pyo3_log::init();

        if cfg!(debug_assertions) {
            log::debug!("Running in Debug mode.");
        } else {
            log::debug!("Running in Release mode.");
        }

        super::init(None, None);

        // ─── Alias ───────────────────────────────────────────────────
        Python::attach(|py| {
            m.add("Opts", py.get_type::<Opts>())?;
            m.add("AsyncConn", py.get_type::<AsyncConn>())?;
            m.add("AsyncTransaction", py.get_type::<AsyncTransaction>())?;
            m.add("SyncConn", py.get_type::<SyncConn>())?;
            m.add("SyncTransaction", py.get_type::<SyncTransaction>())?;
            PyResult::Ok(())
        })?;

        let py = m.py();
        let sys_modules = py.import("sys")?.getattr("modules")?;
        for name in ["error", "sync", "async_", "dbapi", "dbapi_async"] {
            let module = m.getattr(name)?;
            module.setattr("__name__", format!("pyro_mysql.{name}"))?;
            sys_modules.set_item(format!("pyro_mysql.{module}"), module)?;
        }

        Ok(())
    }
}
