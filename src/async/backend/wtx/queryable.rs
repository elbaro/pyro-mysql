use std::collections::HashMap;

use crate::error::Error;
use super::conn::WtxMysqlExecutor;

/// Helper function to get or prepare a statement with client-side caching
pub async fn get_or_prepare_stmt(
    executor: &mut WtxMysqlExecutor,
    stmt_cache: &mut HashMap<String, u64>,
    query: &str,
) -> Result<u64, Error> {
    use wtx::database::Executor;

    // Check cache first
    if let Some(&stmt_id) = stmt_cache.get(query) {
        return Ok(stmt_id);
    }

    // Not in cache, prepare and cache it
    let stmt_id = executor
        .prepare(query)
        .await
        .map_err(|e| Error::WtxError(e.to_string()))?;

    stmt_cache.insert(query.to_string(), stmt_id);
    Ok(stmt_id)
}
