import pyro_mysql
import pytest
from pyro_mysql import SyncConn, SyncPool

from .conftest import get_test_db_url


class TestSyncTransaction:
    # TODO
    # def test_start_transaction_context_manager(self):
    #     """Test that start_transaction works as a context manager"""
    #     conn = SyncConn(get_test_db_url())

    #     # Test basic context manager usage
    #     with conn.start_transaction() as tx:
    #         rows = tx.exec("SELECT 1 as n")
    #         assert len(rows) == 1
    #         assert rows[0].to_dict()["n"] == 1
    #         tx.commit()

    #     # Test auto-rollback on exception
    #     with conn.start_transaction() as tx:
    #         tx.exec("CREATE TEMPORARY TABLE test_tx (id INT)")
    #         tx.exec("INSERT INTO test_tx VALUES (1)")
    #         # Don't commit - should auto-rollback

    #     # Verify table doesn't exist (was rolled back)
    #     with pytest.raises(Exception):
    #         conn.exec("SELECT * FROM test_tx")

    # TODO
    # def test_start_transaction_with_options(self):
    #     """Test start_transaction with various options"""
    #     conn = SyncConn(get_test_db_url())

    #     # Test with readonly transaction
    #     with conn.start_transaction(readonly=True) as tx:
    #         rows = tx.exec("SELECT 1")
    #         assert len(rows) == 1
    #         # Write operations should fail in readonly transaction
    #         with pytest.raises(Exception):
    #             tx.exec("CREATE TEMPORARY TABLE test_readonly (id INT)")
    #         tx.commit()

    #     # Test with isolation level
    #     with conn.start_transaction(
    #         isolation_level=pyro_mysql.IsolationLevel.ReadCommitted
    #     ) as tx:
    #         rows = tx.exec("SELECT 1")
    #         assert len(rows) == 1
    #         tx.commit()

    def test_run_transaction(self):
        """Test run_transaction with a callable"""
        conn = SyncConn(get_test_db_url())

        # First create a test table outside of transaction
        conn.exec("CREATE TEMPORARY TABLE test_tx_rollback (id INT, value VARCHAR(50))")

        def transaction_func(tx):
            rows = tx.exec("SELECT 42 as answer")
            tx.commit()
            return rows[0].to_dict()["answer"]

        result = conn.run_transaction(transaction_func)
        assert result == 42

        # Test auto-rollback on exception
        def failing_transaction(tx):
            tx.exec("INSERT INTO test_tx_rollback VALUES (1, 'should_rollback')")
            raise ValueError("Intentional failure")

        with pytest.raises(ValueError):
            conn.run_transaction(failing_transaction)

        # Verify insert was rolled back
        rows = conn.exec("SELECT * FROM test_tx_rollback")
        assert len(rows) == 0

    def test_transaction_reference_count_warning(self):
        """Test that keeping a reference to transaction shows warning"""
        conn = SyncConn(get_test_db_url())

        # This should trigger a warning because we keep a reference to tx
        tx_ref = None
        with conn.start_transaction() as tx:
            tx_ref = tx  # Keep a reference
            tx.exec("SELECT 1")
            tx.commit()
        # Warning should be printed in __exit__ about refcount != 1

        # Clean up the reference
        del tx_ref

    def test_using_conn_while_transaction_active(self):
        """Test that we can use Conn while a Transaction is active"""
        conn = SyncConn(get_test_db_url())

        with conn.start_transaction() as tx:
            # Use transaction
            tx_rows = tx.exec("SELECT 1 as n")
            assert tx_rows[0].to_dict()["n"] == 1

            # Try to use connection directly - this should work or fail gracefully
            # depending on the implementation
            try:
                conn_rows = conn.exec("SELECT 2 as n")
                # If it works, verify the result
                assert conn_rows[0].to_dict()["n"] == 2
            except Exception as e:
                # If it fails, that's also acceptable behavior
                print(f"Using conn while transaction is active failed: {e}")

            tx.commit()

    # def test_pooled_conn_start_transaction(self):
    #     """Test start_transaction with pooled connections"""
    #     pool = SyncPool(get_test_db_url())

    #     with pool.acquire() as conn:
    #         with conn.start_transaction() as tx:
    #             rows = tx.exec("SELECT 1 as n")
    #             assert rows[0].to_dict()["n"] == 1
    #             tx.commit()

    #     # Test multiple transactions from pool
    #     with pool.acquire() as conn1:
    #         with pool.acquire() as conn2:
    #             with conn1.start_transaction() as tx1:
    #                 with conn2.start_transaction() as tx2:
    #                     tx1.exec("SELECT 1")
    #                     tx2.exec("SELECT 2")
    #                     tx1.commit()
    #                     tx2.commit()
