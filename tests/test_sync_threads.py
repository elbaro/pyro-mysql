"""Tests for SyncConn and SyncTransaction with multi-threading."""

# TODO: remove
import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from pyro_mysql import (
    SyncConn,
    SyncOptsBuilder,
    SyncPool,
    SyncPoolOpts,
)

from .conftest import get_test_db_url


@pytest.fixture
def sync_conn():
    """Create a sync connection for testing."""
    conn = SyncConn(get_test_db_url())
    # Create test table
    conn.exec_drop(
        """
        CREATE TABLE IF NOT EXISTS test_threads (
            id INT AUTO_INCREMENT PRIMARY KEY,
            thread_id VARCHAR(50),
            value INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    conn.exec_drop("TRUNCATE TABLE test_threads")
    yield conn
    # Cleanup
    conn.exec_drop("DROP TABLE IF EXISTS test_threads")
    conn.close()


@pytest.fixture
def sync_pool():
    """Create a sync connection pool for testing."""
    # Create pool opts with constraints
    pool_opts = SyncPoolOpts().with_constraints((2, 10))  # (min, max)

    # Create connection opts with pool options
    opts = SyncOptsBuilder.from_url(get_test_db_url()).pool_opts(pool_opts).build()

    pool = SyncPool(opts)
    # Create test table using a connection from pool
    conn = pool.get_conn()
    try:
        conn.exec_drop(
            """
            CREATE TABLE IF NOT EXISTS test_threads (
                id INT AUTO_INCREMENT PRIMARY KEY,
                thread_id VARCHAR(50),
                value INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        conn.exec_drop("TRUNCATE TABLE test_threads")
    finally:
        conn.close()

    yield pool

    # Cleanup
    conn = pool.get_conn()
    try:
        conn.exec_drop("DROP TABLE IF EXISTS test_threads")
    finally:
        conn.close()
    # pool.close()  # Pool doesn't have close method, connections cleaned up when pool drops


class TestSyncConnThreadSafety:
    """Test thread safety of SyncConn."""

    def test_concurrent_queries(self, sync_conn):
        """Test multiple threads executing queries on same connection."""
        results = queue.Queue()
        errors = queue.Queue()

        def worker(thread_id: int):
            try:
                # Each thread executes multiple queries
                for i in range(5):
                    sync_conn.exec_drop(
                        "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                        (f"thread-{thread_id}", i),
                    )
                    last_id = sync_conn.last_insert_id()
                    results.put((thread_id, i, last_id))

                    # Also do a select
                    rows = sync_conn.exec(
                        "SELECT COUNT(*) as cnt FROM test_threads WHERE thread_id = ?",
                        (f"thread-{thread_id}",),
                    )
                    assert len(rows) > 0
            except Exception as e:
                errors.put((thread_id, str(e)))

        # Run workers in parallel
        threads = []
        num_threads = 10
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        # Check for errors
        error_list = []
        while not errors.empty():
            error_list.append(errors.get())
        assert len(error_list) == 0, f"Errors occurred: {error_list}"

        # Verify all inserts succeeded
        total_row = sync_conn.exec_first("SELECT COUNT(*) as cnt FROM test_threads")
        if total_row:
            total_row_dict = total_row.to_dict()
            assert total_row_dict["cnt"] == num_threads * 5

    def test_read_write_consistency(self, sync_conn):
        """Test that reads and writes are consistent across threads."""
        counter_lock = threading.Lock()
        counter = {"value": 0}

        def writer(thread_id: int):
            for i in range(10):
                with counter_lock:
                    counter["value"] += 1
                    current = counter["value"]

                sync_conn.exec_drop(
                    "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                    (f"writer-{thread_id}", current),
                )
                time.sleep(0.001)  # Small delay to interleave operations

        def reader(thread_id: int):
            for _ in range(20):
                rows = sync_conn.exec("SELECT MAX(value) as max_val FROM test_threads")

                if rows and len(rows) > 0:
                    row_dict = rows[0].to_dict()
                    if row_dict["max_val"] is not None:
                        max_val = row_dict["max_val"]
                        # The max value should never exceed the counter
                        with counter_lock:
                            assert max_val <= counter["value"]

                time.sleep(0.001)

        # Start writers and readers
        threads = []
        for i in range(3):
            t = threading.Thread(target=writer, args=(i,))
            threads.append(t)
            t.start()

        for i in range(2):
            t = threading.Thread(target=reader, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Final verification
        final_count = sync_conn.exec_first("SELECT COUNT(*) as cnt FROM test_threads")
        if final_count:
            final_count_dict = final_count.to_dict()
            assert final_count_dict["cnt"] == 30  # 3 writers * 10 inserts each


class TestSyncPoolThreadSafety:
    """Test thread safety with connection pools."""

    def test_pool_concurrent_connections(self, sync_pool):
        """Test multiple threads getting connections from pool."""
        results = []
        errors = []

        def worker(thread_id: int):
            try:
                # Get connection from pool
                conn = sync_pool.get_conn()
                try:
                    # Use the connection
                    for i in range(3):
                        conn.exec_drop(
                            "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                            (f"pool-{thread_id}", i),
                        )

                    rows = conn.exec(
                        "SELECT COUNT(*) as cnt FROM test_threads WHERE thread_id = ?",
                        (f"pool-{thread_id}",),
                    )

                    if rows:
                        row_dict = rows[0].to_dict()
                        results.append((thread_id, row_dict["cnt"]))
                finally:
                    # Return connection to pool
                    conn.close()

            except Exception as e:
                errors.append((thread_id, str(e)))

        # Run many threads (more than pool size)
        num_threads = 20
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()

        # Check results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == num_threads

        # Each thread should have inserted 3 rows
        for thread_id, count in results:
            assert count == 3

        # Verify total
        conn = sync_pool.get_conn()
        try:
            total = conn.exec_first("SELECT COUNT(*) as cnt FROM test_threads")
            if total:
                total_dict = total.to_dict()
                assert total_dict["cnt"] == num_threads * 3
        finally:
            conn.close()

    def test_pool_connection_reuse(self, sync_pool):
        """Test that connections are properly reused across threads."""
        connection_ids = []
        lock = threading.Lock()

        def worker(thread_id: int):
            conn = sync_pool.get_conn()
            try:
                # Get connection ID
                row = conn.exec_first("SELECT CONNECTION_ID() as id")
                if row:
                    row_dict = row.to_dict()
                    conn_id = row_dict["id"]

                with lock:
                    connection_ids.append(conn_id)

                # Do some work
                conn.exec_drop(
                    "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                    (f"reuse-{thread_id}", conn_id),
                )

                # Small delay to simulate work
                time.sleep(0.01)

            finally:
                conn.close()

        # Run threads in batches to force connection reuse
        for batch in range(3):
            threads = []
            for i in range(5):
                thread_id = batch * 5 + i
                t = threading.Thread(target=worker, args=(thread_id,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

        # Check that connections were reused (should have fewer unique IDs than threads)
        unique_conn_ids = set(connection_ids)
        assert len(unique_conn_ids) <= 10  # Pool max size
        assert len(connection_ids) == 15  # Total thread executions

    def test_pool_stress_test(self, sync_pool):
        """Stress test with many threads and operations."""
        num_threads = 50
        operations_per_thread = 10
        success_count = threading.Semaphore(0)
        error_count = threading.Semaphore(0)

        def stress_worker(thread_id: int):
            try:
                for op in range(operations_per_thread):
                    conn = sync_pool.get_conn()
                    try:
                        # Mix of operations
                        if op % 3 == 0:
                            # Insert
                            conn.exec_drop(
                                "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                                (f"stress-{thread_id}", op),
                            )
                        elif op % 3 == 1:
                            # Select
                            rows = conn.exec("SELECT * FROM test_threads LIMIT 10")
                        else:
                            # Update
                            conn.exec_drop(
                                "UPDATE test_threads SET value = value + 1 WHERE thread_id = ?",
                                (f"stress-{thread_id}",),
                            )
                    finally:
                        conn.close()

                    # Very small delay to increase concurrency
                    time.sleep(0.001)

                success_count.release()
            except Exception:
                error_count.release()

        # Launch all threads
        threads = []
        start_time = time.time()

        for i in range(num_threads):
            t = threading.Thread(target=stress_worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all to complete
        for t in threads:
            t.join()

        elapsed_time = time.time() - start_time

        # Count successes and errors
        successes = 0
        errors = 0

        for _ in range(num_threads):
            if success_count.acquire(blocking=False):
                successes += 1
            elif error_count.acquire(blocking=False):
                errors += 1

        # All threads should succeed
        assert (
            successes == num_threads
        ), f"Only {successes}/{num_threads} threads succeeded"
        assert errors == 0, f"Had {errors} errors"

        print(f"Stress test completed in {elapsed_time:.2f}s")
        print(
            f"Operations per second: {(num_threads * operations_per_thread) / elapsed_time:.0f}"
        )
