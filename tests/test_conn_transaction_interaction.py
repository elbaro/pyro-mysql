"""
Test that using Conn while Transaction is active raises an error.
"""

import pytest
from pyro_mysql import AsyncConn

from .conftest import get_test_db_url

# TODO: raise 'Conn is already in use' instead of deadlock
# class TestSyncConnTransactionInteraction:
#     def test_conn_usage_during_transaction_raises(self):
#         """Test that using Conn while a Transaction is active raises an error"""
#         conn = SyncConn(get_test_db_url())

#         with conn.start_transaction() as tx:
#             # First, use the transaction - should work
#             tx_result = tx.exec("SELECT 1 as value")
#             assert tx_result[0].to_dict()["value"] == 1

#             # Now try to use the connection directly - should raise
#             with pytest.raises(Exception) as exc_info:
#                 conn.exec("SELECT 2 as value")

#             print(f"Expected error when using conn during transaction: {exc_info.value}")

#             # Transaction should still work after the failed conn attempt
#             tx_result2 = tx.exec("SELECT 3 as value")
#             assert tx_result2[0].to_dict()["value"] == 3

#             tx.commit()

#         # After transaction, conn should work normally
#         after_result = conn.exec("SELECT 4 as value")
#         assert after_result[0].to_dict()["value"] == 4


class TestAsyncConnTransactionInteraction:
    @pytest.mark.asyncio
    async def test_async_conn_usage_during_transaction_raises(self):
        """Test that using async Conn while AsyncTransaction is active raises an error"""
        conn = await AsyncConn.new(get_test_db_url())

        async with conn.start_transaction() as tx:
            # Use the transaction - should work
            tx_result = await tx.exec("SELECT 1 as value")
            assert tx_result[0].to_dict()["value"] == 1

            # Try to use the connection directly - should raise
            with pytest.raises(Exception) as exc_info:
                await conn.exec("SELECT 2 as value")

            print(
                f"Expected error when using conn during async transaction: {exc_info.value}"
            )

            # Transaction should still work
            tx_result2 = await tx.exec("SELECT 3 as value")
            assert tx_result2[0].to_dict()["value"] == 3

            await tx.commit()

        # After transaction, conn should work normally
        after_result = await conn.exec("SELECT 4 as value")
        assert after_result[0].to_dict()["value"] == 4
