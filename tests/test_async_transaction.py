import asyncio

import pytest
from pyro_mysql import IsolationLevel

from .conftest import (
    cleanup_test_table_async,
    get_async_conn_with_backend,
    get_test_db_url,
    setup_test_table_async,
)


@pytest.mark.asyncio
async def test_basic_transaction(async_backend):
    """Test basic transaction commit."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    print("---")

    async with conn.start_transaction() as tx:
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
        )

        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 1

        await tx.commit()

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 1

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_transaction_rollback(async_backend):
    """Test transaction rollback."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    async with conn.start_transaction() as tx:
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
        )

        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 1

        await tx.rollback()

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 0

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_transaction_isolation_levels(async_backend):
    """Test different transaction isolation levels."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    isolation_levels = [
        IsolationLevel.ReadUncommitted,
        IsolationLevel.ReadCommitted,
        IsolationLevel.RepeatableRead,
        IsolationLevel.Serializable,
    ]

    for isolation_level in isolation_levels:
        async with conn.start_transaction(isolation_level=isolation_level) as tx:
            await conn.exec_drop(
                "INSERT INTO test_table (name, age) VALUES (?, ?)",
                (f"Test_{isolation_level.name}", 25),
            )

            await tx.commit()

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 4

    await cleanup_test_table_async(conn)
    await conn.close()


# TODO
# Server error: `ERROR HY000 (1295): This command is not supported in the prepared statement protocol yet
@pytest.mark.asyncio
async def test_nested_transactions(async_backend):
    """Test nested transactions with savepoints."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    async with conn.start_transaction() as tx:
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
        )

        await conn.exec_drop("SAVEPOINT sp1")

        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Bob", 25)
        )

        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 2

        await conn.exec_drop("ROLLBACK TO SAVEPOINT sp1")

        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 1

        await tx.commit()

    result = await conn.query("SELECT name, age FROM test_table")
    assert len(result) == 1
    assert (result[0][0], result[0][1]) == ("Alice", 30)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_transaction_with_error(async_backend):
    """Test transaction behavior with errors."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    async with conn.start_transaction() as tx:
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Bob", 25)
        )

        # Try to insert with duplicate primary key
        with pytest.raises(Exception):
            await conn.exec_drop(
                "INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)",
                (1, "Charlie", 35),
            )

        await tx.rollback()

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 1

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_transaction_concurrent_read(async_backend):
    """Test concurrent reads with transactions."""
    conn1 = await get_async_conn_with_backend(get_test_db_url(), async_backend)
    conn2 = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn1)

    await conn1.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Initial", 20)
    )

    async with conn1.start_transaction(
        isolation_level=IsolationLevel.ReadCommitted
    ) as tx:
        await conn1.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
        )

        # conn2 should not see the uncommitted change
        count = await conn2.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 1

        await tx.commit()

    # Now conn2 should see the committed change
    count = await conn2.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 2

    await cleanup_test_table_async(conn1)
    await conn1.close()
    await conn2.close()


@pytest.mark.asyncio
async def test_transaction_read_only(async_backend):
    """Test read-only transactions."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    async with conn.start_transaction(readonly=True) as tx:
        result = await conn.query("SELECT name, age FROM test_table")
        assert len(result) == 1

        # Write operations should fail in read-only transaction
        with pytest.raises(Exception):
            await conn.exec_drop(
                "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Bob", 25)
            )

        await tx.rollback()

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_transaction_consistent_snapshot(async_backend):
    """Test consistent snapshot transactions."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    async with conn.start_transaction(
        consistent_snapshot=True, isolation_level=IsolationLevel.RepeatableRead
    ) as tx:
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 1

        await tx.commit()

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_transaction_auto_rollback_on_drop(async_backend):
    """Test automatic rollback when transaction is dropped."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    # Create and drop transaction without explicit commit/rollback
    async with conn.start_transaction() as tx:
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
        )
        # Transaction will auto-rollback when exiting context without commit
        pass

    # Give some time for cleanup
    await asyncio.sleep(0.1)

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 0

    await cleanup_test_table_async(conn)
    await conn.close()
