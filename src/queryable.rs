use color_eyre::{Result, eyre::ContextCompat};
use mysql_async::{BinaryProtocol, QueryResult};
use std::sync::Arc;
use tokio::sync::RwLock;
use pyo3::prelude::*;

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
    async fn query(&self, query: String) -> Result<Vec<Row>>;
    async fn query_first(&self, query: String) -> Result<Option<Row>>;
    async fn query_drop(&self, query: String) -> Result<()>;

    // ─── Binary Protocol ─────────────────────────────────────────────────
    async fn exec(&self, query: String, params: Params) -> Result<Vec<Row>>;
    async fn exec_first(&self, query: String, params: Params) -> Result<Option<Row>>;
    async fn exec_drop(&self, query: String, params: Params) -> Result<()>;
    async fn exec_batch(&self, query: String, params: Vec<Params>) -> Result<()>;
    // async fn exec_iter<T>(&self, query: String, params: Params) -> Result<RowStream<T>>;
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

    // ─── Text Protocol ───────────────────────────────────────────────────
    async fn query(&self, query: String) -> Result<Vec<Row>> {
        let inner = self.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let mut inner = inner.write().await;
                Ok(inner
                    .as_mut()
                    .context("connection is already closed")?
                    .query(query)
                    .await?)
            })
            .await?
    }

    async fn query_first(&self, query: String) -> Result<Option<Row>> {
        let inner = self.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let mut inner = inner.write().await;
                Ok(inner
                    .as_mut()
                    .context("connection is already closed")?
                    .query_first(query)
                    .await?)
            })
            .await?
    }

    async fn query_drop(&self, query: String) -> Result<()> {
        let inner = self.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move {
                let mut inner = inner.write().await;
                Ok(inner
                    .as_mut()
                    .context("connection is already closed")?
                    .query_drop(query)
                    .await?)
            })
            .await?
    }

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
    // async fn exec_iter(&self, query: String, params: Params) -> Result<RowStream> {
    //     let inner = self.clone();
    //     pyo3_async_runtimes::tokio::get_runtime()
    //         .spawn(async move {
    //             let mut inner = inner.write().await;
    //             Ok(RowStream::new(inner
    //                 .as_mut()
    //                 .context("connection is already closed")?
    //                 .exec_iter(query, params)
    //                 .await?))
    //         })
    //         .await?
        
        
    // }
}
