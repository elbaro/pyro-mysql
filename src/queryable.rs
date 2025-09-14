use color_eyre::{Result, eyre::ContextCompat};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{params::Params, row::Row};

/// This trait implements the common methods between Conn, Connection, Transaction.
///
/// pyo3_async_runtimes::tokio::future_into_py_with_locals
/// pyo3_async_runtimes::tokio::get_runtime().spawn

pub trait Queryable {
    async fn ping(&self) -> Result<()>;
    // async fn prep(&self, query: String) -> Result<()>; // TODO
    async fn close_prepared_statement<'py>(&self, stmt: mysql_async::Statement) -> Result<()>;

    // ─── Text Protocol ───────────────────────────────────────────────────

    // ─── Binary Protocol ─────────────────────────────────────────────────
    async fn exec(&self, query: String, params: Params) -> Result<Vec<Row>>;
    async fn exec_first(&self, query: String, params: Params) -> Result<Option<Row>>;
    async fn exec_drop(&self, query: String, params: Params) -> Result<()>;
    async fn exec_batch(&self, query: String, params: Vec<Params>) -> Result<()>;
    // TODO: convert tokio Stream to Python async iterable.
    // async fn exec_iter(&self, query: String, params: Params) -> Result<Option<Row>>;
}

impl<T: mysql_async::prelude::Queryable + Send + Sync + 'static> Queryable
    for Arc<RwLock<Option<T>>>
{
    async fn ping(&self) -> Result<()> {
        let inner = self.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let mut inner = inner.write().await;
                Ok(inner
                    .as_mut()
                    .context("connection is already closed")?
                    .ping()
                    .await?)
            })
            .await?
    }

    async fn close_prepared_statement<'py>(&self, stmt: mysql_async::Statement) -> Result<()> {
        let inner = self.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let mut inner = inner.write().await;
                Ok(inner
                    .as_mut()
                    .context("connection is already closed")?
                    .close(stmt)
                    .await?)
            })
            .await?
    }

    // StatementLike: str, Statement
    // AsQuery

    // -- text protocol - good if there is no parametesr - all values are encoded as a string
    // query_iter
    // query
    // query_first
    // query_drop -> None
    // query_stream -> Stream<Row>

    // ─── Binary Protocol ─────────────────────────────────────────────────
    async fn exec(&self, query: String, params: Params) -> Result<Vec<Row>> {
        let inner = self.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let mut inner = inner.write().await;
                Ok(inner
                    .as_mut()
                    .context("connection is already closed")?
                    .exec(query, params)
                    .await?)
            })
            .await?
    }
    async fn exec_first(&self, query: String, params: Params) -> Result<Option<Row>> {
        let inner = self.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let mut inner = inner.write().await;
                Ok(inner
                    .as_mut()
                    .context("connection is already closed")?
                    .exec_first(query, params)
                    .await?)
            })
            .await?
    }
    async fn exec_drop(&self, query: String, params: Params) -> Result<()> {
        let inner = self.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let mut inner = inner.write().await;
                Ok(inner
                    .as_mut()
                    .context("connection is already closed")?
                    .exec_drop(query, params)
                    .await?)
            })
            .await?
    }
    async fn exec_batch(&self, query: String, params: Vec<Params>) -> Result<()> {
        let inner = self.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let mut inner = inner.write().await;
                Ok(inner
                    .as_mut()
                    .context("connection is already closed")?
                    .exec_batch(query, params)
                    .await?)
            })
            .await?
    }
}
