import pyro_mysql
import pytest
from pyro_mysql import SyncConn

from .conftest import get_test_db_url


class TestSyncTransaction:
    def test_start_transaction(self):
        """Test run_transaction with a callable"""
        conn = SyncConn(get_test_db_url())

        # First create a test table outside of transaction
        conn.exec_drop(
            "CREATE TEMPORARY TABLE test_tx_rollback (id INT, value VARCHAR(50))"
        )

        with conn.start_transaction() as tx:
            rows = conn.exec("SELECT 42 as answer")
            tx.commit()
            result = rows[0][0]
        assert result == 42

        # Test auto-rollback on exception
        with pytest.raises(ValueError):
            with conn.start_transaction() as tx:
                conn.exec_drop(
                    "INSERT INTO test_tx_rollback VALUES (1, 'should_rollback')"
                )
                raise ValueError("Intentional failure")

        # Verify insert was rolled back
        rows = conn.exec("SELECT * FROM test_tx_rollback")
        assert len(rows) == 0

    def test_nested_transaction_not_allowed(self):
        """Test that nested transactions are not allowed"""
        conn = SyncConn(get_test_db_url())

        with conn.start_transaction() as tx:
            # Trying to start a second transaction should fail
            with pytest.raises(pyro_mysql.error.IncorrectApiUsageError):
                with conn.start_transaction() as tx2:
                    pass
            tx.commit()
