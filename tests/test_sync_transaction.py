import pyro_mysql
import pytest
from pyro_mysql import SyncConn, SyncPool

from .conftest import get_test_db_url


class TestSyncTransaction:
    def test_start_transaction(self):
        """Test run_transaction with a callable"""
        conn = SyncConn(get_test_db_url())

        # First create a test table outside of transaction
        conn.exec("CREATE TEMPORARY TABLE test_tx_rollback (id INT, value VARCHAR(50))")

        with conn.start_transaction() as tx:
            rows = tx.exec("SELECT 42 as answer")
            tx.commit()
            result = rows[0].to_dict()["answer"]
        assert result == 42

        # Test auto-rollback on exception
        with pytest.raises(ValueError):
            with conn.start_transaction() as tx:
                tx.exec("INSERT INTO test_tx_rollback VALUES (1, 'should_rollback')")
                raise ValueError("Intentional failure")

        # Verify insert was rolled back
        rows = conn.exec("SELECT * FROM test_tx_rollback")
        assert len(rows) == 0

    def test_transaction_reference_count_warning(self):
        """Test that keeping a reference to transaction shows warning"""
        conn = SyncConn(get_test_db_url())

        with pytest.raises(pyro_mysql.error.IncorrectApiUsageError):
            tx_ref = None
            with conn.start_transaction() as tx:
                tx_ref = tx  # Keep a reference
                tx.exec("SELECT 1")
                tx.commit()

    def test_using_conn_while_transaction_active(self):
        """Test that we can use Conn while a Transaction is active"""
        conn = SyncConn(get_test_db_url())

        with conn.start_transaction() as tx:
            tx_rows = tx.exec("SELECT 1 as n")
            assert tx_rows[0].to_dict()["n"] == 1
            with pytest.raises(pyro_mysql.error.ConnectionClosedError):
                conn.exec("SELECT 2 as n")

            tx.commit()

    def test_pooled_conn_start_transaction(self):
        """Test start_transaction with pooled connections"""
        pool = SyncPool(get_test_db_url())

        with pool.acquire() as conn:
            with conn.start_transaction() as tx:
                rows = tx.exec("SELECT 1 as n")
                assert rows[0].to_dict()["n"] == 1
                tx.commit()

        # Test multiple transactions from pool
        with pool.acquire() as conn1:
            with pool.acquire() as conn2:
                with conn1.start_transaction() as tx1:
                    with conn2.start_transaction() as tx2:
                        tx1.exec("SELECT 1")
                        tx2.exec("SELECT 2")
                        tx1.commit()
                        tx2.commit()
