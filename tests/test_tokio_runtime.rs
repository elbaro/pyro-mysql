use pyro_mysql::tokio_thread::TokioThread;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_basic_spawn() {
    let tokio_thread = TokioThread::new();

    let handle = tokio_thread.spawn(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        42
    });

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

#[test]
fn test_concurrent_tasks() {
    let tokio_thread = TokioThread::new();
    let counter = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];
    for _ in 0..10 {
        let counter_clone = Arc::clone(&counter);
        let handle = tokio_thread.spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });
        handles.push(handle);
    }

    for handle in handles {
        futures::executor::block_on(handle).unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 10);
}

#[test]
fn test_async_computation() {
    let tokio_thread = TokioThread::new();

    let handle = tokio_thread.spawn(async {
        let mut sum = 0;
        for i in 1..=100 {
            sum += i;
        }
        sum
    });

    let result = futures::executor::block_on(handle).unwrap();
    assert_eq!(result, 5050);
}

#[test]
fn test_spawn_with_tokio_features() {
    let tokio_thread = TokioThread::new();

    let handle = tokio_thread.spawn(async {
        // Test tokio::time::sleep
        let start = std::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let elapsed = start.elapsed();

        // Should be approximately 50ms (allow some variance)
        elapsed.as_millis() >= 45 && elapsed.as_millis() <= 100
    });

    let result = futures::executor::block_on(handle).unwrap();
    assert!(result);
}

#[test]
fn test_spawn_with_shared_state() {
    let tokio_thread = TokioThread::new();
    let data = Arc::new(AtomicU32::new(0));

    let data1 = Arc::clone(&data);
    let handle1 = tokio_thread.spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        data1.store(100, Ordering::SeqCst);
    });

    let data2 = Arc::clone(&data);
    let handle2 = tokio_thread.spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        data2.load(Ordering::SeqCst)
    });

    futures::executor::block_on(handle1).unwrap();
    let result = futures::executor::block_on(handle2).unwrap();

    assert_eq!(result, 100);
}

#[test]
fn test_multiple_tokio_threads() {
    let thread1 = TokioThread::new();
    let thread2 = TokioThread::new();

    let handle1 = thread1.spawn(async { "thread1" });
    let handle2 = thread2.spawn(async { "thread2" });

    let result1 = futures::executor::block_on(handle1).unwrap();
    let result2 = futures::executor::block_on(handle2).unwrap();

    assert_eq!(result1, "thread1");
    assert_eq!(result2, "thread2");
}

#[test]
fn test_drop_cleanup() {
    // Create and immediately drop
    {
        let tokio_thread = TokioThread::new();
        let _handle = tokio_thread.spawn(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
        });
        // TokioThread drops here
    }
    // If we get here without hanging, drop worked correctly
}

#[test]
fn test_spawn_returns_result() {
    let tokio_thread = TokioThread::new();

    let handle = tokio_thread.spawn(async {
        if 2 + 2 == 4 {
            Ok::<_, String>(42)
        } else {
            Err("math is broken".to_string())
        }
    });

    let result = futures::executor::block_on(handle).unwrap();
    assert_eq!(result, Ok(42));
}

#[test]
fn test_nested_async_operations() {
    let tokio_thread = TokioThread::new();

    let handle = tokio_thread.spawn(async {
        let inner = async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            10
        };

        let result = inner.await;
        result * 2
    });

    let result = futures::executor::block_on(handle).unwrap();
    assert_eq!(result, 20);
}
