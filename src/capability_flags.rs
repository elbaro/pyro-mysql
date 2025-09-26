use pyo3::prelude::*;

#[pyclass]
pub struct CapabilityFlags;

#[pymethods]
impl CapabilityFlags {
    #[classattr]
    const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
    #[classattr]
    const CLIENT_FOUND_ROWS: u32 = 0x0000_0002;
    #[classattr]
    const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
    #[classattr]
    const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
    #[classattr]
    const CLIENT_NO_SCHEMA: u32 = 0x0000_0010;
    #[classattr]
    const CLIENT_COMPRESS: u32 = 0x0000_0020;
    #[classattr]
    const CLIENT_ODBC: u32 = 0x0000_0040;
    #[classattr]
    const CLIENT_LOCAL_FILES: u32 = 0x0000_0080;
    #[classattr]
    const CLIENT_IGNORE_SPACE: u32 = 0x0000_0100;
    #[classattr]
    const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
    #[classattr]
    const CLIENT_INTERACTIVE: u32 = 0x0000_0400;
    #[classattr]
    const CLIENT_SSL: u32 = 0x0000_0800;
    #[classattr]
    const CLIENT_IGNORE_SIGPIPE: u32 = 0x0000_1000;
    #[classattr]
    const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
    #[classattr]
    const CLIENT_RESERVED: u32 = 0x0000_4000;
    #[classattr]
    const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    #[classattr]
    const CLIENT_MULTI_STATEMENTS: u32 = 0x0001_0000;
    #[classattr]
    const CLIENT_MULTI_RESULTS: u32 = 0x0002_0000;
    #[classattr]
    const CLIENT_PS_MULTI_RESULTS: u32 = 0x0004_0000;
    #[classattr]
    const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
    #[classattr]
    const CLIENT_CONNECT_ATTRS: u32 = 0x0010_0000;
    #[classattr]
    const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
    #[classattr]
    const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 0x0040_0000;
    #[classattr]
    const CLIENT_SESSION_TRACK: u32 = 0x0080_0000;
    #[classattr]
    const CLIENT_DEPRECATE_EOF: u32 = 0x0100_0000;
    #[classattr]
    const CLIENT_OPTIONAL_RESULTSET_METADATA: u32 = 0x0200_0000;
    #[classattr]
    const CLIENT_ZSTD_COMPRESSION_ALGORITHM: u32 = 0x0400_0000;
    #[classattr]
    const CLIENT_QUERY_ATTRIBUTES: u32 = 0x0800_0000;
    #[classattr]
    const MULTI_FACTOR_AUTHENTICATION: u32 = 0x1000_0000;
    #[classattr]
    const CLIENT_PROGRESS_OBSOLETE: u32 = 0x2000_0000;
    #[classattr]
    const CLIENT_SSL_VERIFY_SERVER_CERT: u32 = 0x4000_0000;
    #[classattr]
    const CLIENT_REMEMBER_OPTIONS: u32 = 0x8000_0000;
}
