use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle as TokioJoinHandle;

/// Global TokioThread instance
static GLOBAL_TOKIO_THREAD: OnceLock<TokioThread> = OnceLock::new();

/// Get or initialize the global TokioThread instance
pub fn get_tokio_thread() -> &'static TokioThread {
    GLOBAL_TOKIO_THREAD.get_or_init(TokioThread::new)
}

/// A dedicated OS thread running a Tokio runtime with 'current_thread' flavor.
///
/// This struct spawns an OS thread that creates a Tokio runtime and blocks on
/// `std::future::pending()` indefinitely. Futures can be spawned onto this runtime
/// from the main thread using the `spawn()` method.
pub struct TokioThread {
    handle: Arc<Handle>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl TokioThread {
    /// Creates a new TokioThread with a dedicated OS thread running a Tokio runtime.
    pub fn new() -> Self {
        let (handle_tx, handle_rx) = std::sync::mpsc::channel::<Arc<Handle>>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let thread = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create Tokio runtime");

            let handle = Arc::new(rt.handle().clone());

            // Send the handle to the main thread
            handle_tx
                .send(handle)
                .expect("Failed to send runtime handle");

            // Block on shutdown signal
            rt.block_on(async {
                let _ = shutdown_rx.await;
            });
        });

        let handle = handle_rx.recv().expect("Failed to receive runtime handle");

        TokioThread {
            handle,
            shutdown_tx: Some(shutdown_tx),
            thread: Some(thread),
        }
    }

    /// Spawns a future onto the Tokio runtime running on the dedicated thread.
    pub fn spawn<F>(&self, future: F) -> TokioJoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }
}

impl Drop for TokioThread {
    fn drop(&mut self) {
        // Send shutdown signal to the runtime thread
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // Wait for the thread to finish
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Default for TokioThread {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_tokio_thread_spawn() {
        let tokio_thread = TokioThread::new();

        let handle = tokio_thread.spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            42
        });

        // Block on the spawned task
        let result = futures::executor::block_on(handle).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_multiple_spawns() {
        let tokio_thread = TokioThread::new();

        let handle1 = tokio_thread.spawn(async { 1 });
        let handle2 = tokio_thread.spawn(async { 2 });
        let handle3 = tokio_thread.spawn(async { 3 });

        let result1 = futures::executor::block_on(handle1).unwrap();
        let result2 = futures::executor::block_on(handle2).unwrap();
        let result3 = futures::executor::block_on(handle3).unwrap();

        assert_eq!(result1, 1);
        assert_eq!(result2, 2);
        assert_eq!(result3, 3);
    }
}
