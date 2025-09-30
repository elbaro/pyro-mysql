"""Tests for SyncConn and SyncTransaction with multi-threading."""

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
    # pool.disconnect()  # Pool doesn't have disconnect method, connections cleaned up when pool drops


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

    def test_concurrent_transactions(self, sync_conn):
        """Test multiple threads using transactions on same connection."""
        results = []
        lock = threading.Lock()

        def worker(thread_id: int):
            try:
                # Start transaction
                tx = sync_conn.start_transaction()

                # Insert data within transaction
                tx.exec_drop(
                    "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                    (f"tx-{thread_id}", thread_id * 100),
                )

                # Read within transaction
                rows = tx.exec(
                    "SELECT * FROM test_threads WHERE thread_id = ?",
                    (f"tx-{thread_id}",),
                )

                # Commit
                tx.commit()

                with lock:
                    results.append((thread_id, len(rows)))

            except Exception as e:
                with lock:
                    results.append((thread_id, f"error: {e}"))

        # Run multiple transaction threads
        threads = []
        num_threads = 5
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Check results
        errors = [r for r in results if isinstance(r[1], str)]
        assert len(errors) == 0, f"Transaction errors: {errors}"

        # Verify all transactions completed
        total_row = sync_conn.exec_first(
            "SELECT COUNT(*) as cnt FROM test_threads WHERE thread_id LIKE 'tx-%'"
        )
        if total_row:
            total_row_dict = total_row.to_dict()
            assert total_row_dict["cnt"] == num_threads

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


class TestSyncTransactionThreadSafety:
    """Test thread safety of SyncTransaction."""

    @pytest.mark.skip(
        reason="Transaction isolation behavior depends on MySQL configuration"
    )
    def test_transaction_isolation(self, sync_conn):
        """Test that transactions are properly isolated between threads."""
        barrier = threading.Barrier(2)
        results = {"thread1": None, "thread2": None}

        def thread1_work():
            tx = sync_conn.start_transaction()
            try:
                # Insert in transaction
                tx.exec_drop(
                    "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                    ("thread1", 100),
                )

                # Signal thread2 and wait
                barrier.wait()

                # Read own insert (should see it)
                rows = tx.exec("SELECT * FROM test_threads WHERE thread_id = 'thread1'")
                results["thread1"] = len(rows)

                # Wait for thread2 to try reading
                barrier.wait()

                # Commit
                tx.commit()
            except Exception:
                tx.rollback()
                raise

        def thread2_work():
            # Wait for thread1 to insert
            barrier.wait()

            # Try to read thread1's uncommitted data (shouldn't see it)
            rows = sync_conn.exec(
                "SELECT * FROM test_threads WHERE thread_id = 'thread1'"
            )
            results["thread2"] = len(rows)

            # Signal thread1 to continue
            barrier.wait()

        t1 = threading.Thread(target=thread1_work)
        t2 = threading.Thread(target=thread2_work)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # Thread1 should see its own insert
        assert results["thread1"] == 1
        # Note: MySQL's default isolation level is REPEATABLE READ
        # Thread2's ability to see thread1's uncommitted data depends on isolation level
        # In REPEATABLE READ, thread2 should NOT see uncommitted changes
        assert results["thread2"] == 0, f"Thread2 saw uncommitted data: {results}"

    def test_concurrent_transaction_commits(self, sync_conn):
        """Test multiple transactions committing concurrently."""
        num_threads = 5
        results = []

        def transaction_worker(thread_id: int):
            tx = sync_conn.start_transaction()
            try:
                # Each transaction inserts multiple rows
                for i in range(3):
                    tx.exec_drop(
                        "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                        (f"tx-{thread_id}", i),
                    )

                # Random sleep to vary commit timing
                time.sleep(random.uniform(0.001, 0.01))

                # Commit
                tx.commit()
                results.append((thread_id, "committed"))

            except Exception as e:
                tx.rollback()
                results.append((thread_id, f"rolled back: {e}"))

        # Run transactions in parallel
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(transaction_worker, i) for i in range(num_threads)
            ]
            for future in as_completed(futures):
                future.result()

        # Check all committed successfully
        committed = [r for r in results if r[1] == "committed"]
        assert len(committed) == num_threads

        # Verify data integrity
        total_row = sync_conn.exec_first("SELECT COUNT(*) as cnt FROM test_threads")
        if total_row:
            total_row_dict = total_row.to_dict()
            assert total_row_dict["cnt"] == num_threads * 3

    def test_transaction_rollback_isolation(self, sync_conn):
        """Test that rolled back transactions don't affect other threads."""
        barrier = threading.Barrier(2)

        def rollback_thread():
            tx = sync_conn.start_transaction()
            try:
                # Insert data
                tx.exec_drop(
                    "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                    ("rollback", 999),
                )

                # Let other thread proceed
                barrier.wait()

                # Wait a bit
                time.sleep(0.01)

                # Rollback
                tx.rollback()
            except Exception:
                tx.rollback()
                raise

        def commit_thread():
            # Wait for rollback thread to insert
            barrier.wait()

            tx = sync_conn.start_transaction()
            try:
                # Insert different data
                tx.exec_drop(
                    "INSERT INTO test_threads (thread_id, value) VALUES (?, ?)",
                    ("commit", 111),
                )

                # Commit
                tx.commit()
            except Exception:
                tx.rollback()
                raise

        t1 = threading.Thread(target=rollback_thread)
        t2 = threading.Thread(target=commit_thread)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # Only the committed data should exist
        rows = sync_conn.exec("SELECT * FROM test_threads")
        print("rows ", rows[0].to_dict(), rows[1].to_dict())

        assert len(rows) == 1
        row_dict = rows[0].to_dict()
        assert row_dict["thread_id"] == "commit"
        assert row_dict["value"] == 111


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


class TestDeadlockPrevention:
    """Test that the implementation prevents common deadlock scenarios."""

    def test_no_deadlock_on_concurrent_transactions(self, sync_conn):
        """Test that concurrent transactions don't deadlock."""
        # Create two tables for cross-updates
        sync_conn.exec_drop(
            """
            CREATE TABLE IF NOT EXISTS test_table1 (
                id INT PRIMARY KEY,
                value INT
            )
        """
        )
        sync_conn.exec_drop(
            """
            CREATE TABLE IF NOT EXISTS test_table2 (
                id INT PRIMARY KEY,
                value INT
            )
        """
        )

        # Insert initial data
        sync_conn.exec_drop("INSERT INTO test_table1 (id, value) VALUES (1, 0)")
        sync_conn.exec_drop("INSERT INTO test_table2 (id, value) VALUES (1, 0)")

        completed = []
        lock = threading.Lock()

        def transaction1():
            tx = sync_conn.start_transaction()
            try:
                # Update table1 first
                tx.exec_drop("UPDATE test_table1 SET value = value + 1 WHERE id = 1")
                time.sleep(0.01)  # Small delay to encourage interleaving
                # Then table2
                tx.exec_drop("UPDATE test_table2 SET value = value + 1 WHERE id = 1")
                tx.commit()
                with lock:
                    completed.append("tx1")
            except Exception as e:
                tx.rollback()
                with lock:
                    completed.append(f"tx1_error: {e}")

        def transaction2():
            tx = sync_conn.start_transaction()
            try:
                # Update table2 first (opposite order)
                tx.exec_drop("UPDATE test_table2 SET value = value + 10 WHERE id = 1")
                time.sleep(0.01)  # Small delay to encourage interleaving
                # Then table1
                tx.exec_drop("UPDATE test_table1 SET value = value + 10 WHERE id = 1")
                tx.commit()
                with lock:
                    completed.append("tx2")
            except Exception as e:
                tx.rollback()
                with lock:
                    completed.append(f"tx2_error: {e}")

        # Run both transactions concurrently
        t1 = threading.Thread(target=transaction1)
        t2 = threading.Thread(target=transaction2)

        t1.start()
        t2.start()

        # Wait with timeout to detect deadlock
        t1.join(timeout=5.0)
        t2.join(timeout=5.0)

        # Check that threads completed (no hanging/deadlock)
        assert not t1.is_alive(), "Thread 1 is still running (possible deadlock)"
        assert not t2.is_alive(), "Thread 2 is still running (possible deadlock)"

        # At least one transaction should complete (deadlock would prevent both)
        successful = [
            c
            for c in completed
            if not c.startswith("tx1_error") and not c.startswith("tx2_error")
        ]
        assert (
            len(successful) >= 1
        ), f"No transactions completed successfully: {completed}"

        # Cleanup
        sync_conn.exec_drop("DROP TABLE IF EXISTS test_table1")
        sync_conn.exec_drop("DROP TABLE IF EXISTS test_table2")
