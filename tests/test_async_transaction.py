import asyncio

import pytest
from pyro_mysql import IsolationLevel
from pyro_mysql.async_ import Conn

from .conftest import cleanup_test_table_async, get_async_opts, setup_test_table_async


@pytest.mark.asyncio
async def test_basic_transaction():
    """Test basic transaction commit."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    tx = await conn.start_transaction()

    await tx.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    count = await tx.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (1,)

    await tx.commit()

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (1,)

    await cleanup_test_table_async(conn)
    await conn.disconnect()


@pytest.mark.asyncio
async def test_transaction_rollback():
    """Test transaction rollback."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    tx = await conn.start_transaction()

    await tx.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    count = await tx.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (1,)

    await tx.rollback()

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (0,)

    await cleanup_test_table_async(conn)
    await conn.disconnect()


@pytest.mark.asyncio
async def test_transaction_isolation_levels():
    """Test different transaction isolation levels."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    isolation_levels = [
        IsolationLevel.ReadUncommitted,
        IsolationLevel.ReadCommitted,
        IsolationLevel.RepeatableRead,
        IsolationLevel.Serializable,
    ]

    for isolation_level in isolation_levels:
        tx = await conn.start_transaction(isolation_level=isolation_level)

        await tx.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)",
            (f"Test_{isolation_level.as_str()}", 25),
        )

        await tx.commit()

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (4,)

    await cleanup_test_table_async(conn)
    await conn.disconnect()


@pytest.mark.asyncio
async def test_nested_transactions():
    """Test nested transactions with savepoints."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    tx = await conn.start_transaction()

    await tx.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    await tx.exec_drop("SAVEPOINT sp1")

    await tx.exec_drop("INSERT INTO test_table (name, age) VALUES (?, ?)", ("Bob", 25))

    count = await tx.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (2,)

    await tx.exec_drop("ROLLBACK TO SAVEPOINT sp1")

    count = await tx.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (1,)

    await tx.commit()

    result = await conn.query("SELECT name, age FROM test_table")
    assert len(result) == 1
    assert result[0].to_tuple() == ("Alice", 30)

    await cleanup_test_table_async(conn)
    await conn.disconnect()


@pytest.mark.asyncio
async def test_transaction_with_error():
    """Test transaction behavior with errors."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    tx = await conn.start_transaction()

    await tx.exec_drop("INSERT INTO test_table (name, age) VALUES (?, ?)", ("Bob", 25))

    # Try to insert with duplicate primary key
    with pytest.raises(Exception):
        await tx.exec_drop(
            "INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)",
            (1, "Charlie", 35),
        )

    await tx.rollback()

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (1,)

    await cleanup_test_table_async(conn)
    await conn.disconnect()


@pytest.mark.asyncio
async def test_transaction_concurrent_read():
    """Test concurrent reads with transactions."""
    opts = get_async_opts()
    conn1 = await Conn.new(opts)
    conn2 = await Conn.new(opts)

    await setup_test_table_async(conn1)

    await conn1.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Initial", 20)
    )

    tx = await conn1.start_transaction(isolation_level=IsolationLevel.ReadCommitted)
    await tx.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    # conn2 should not see the uncommitted change
    count = await conn2.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (1,)

    await tx.commit()

    # Now conn2 should see the committed change
    count = await conn2.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (2,)

    await cleanup_test_table_async(conn1)
    await conn1.disconnect()
    await conn2.disconnect()


@pytest.mark.asyncio
async def test_transaction_read_only():
    """Test read-only transactions."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    tx = await conn.start_transaction(readonly=True)

    result = await tx.query("SELECT name, age FROM test_table")
    assert len(result) == 1

    # Write operations should fail in read-only transaction
    with pytest.raises(Exception):
        await tx.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Bob", 25)
        )

    await tx.rollback()

    await cleanup_test_table_async(conn)
    await conn.disconnect()


@pytest.mark.asyncio
async def test_transaction_consistent_snapshot():
    """Test consistent snapshot transactions."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    tx_opts = (
        TxOpts.default()
        .with_consistent_snapshot(True)
        .with_isolation_level(IsolationLevel.RepeatableRead)
    )

    tx = await conn.start_transaction(tx_opts)

    count = await tx.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (1,)

    await tx.commit()

    await cleanup_test_table_async(conn)
    await conn.disconnect()


@pytest.mark.asyncio
async def test_transaction_auto_rollback_on_drop():
    """Test automatic rollback when transaction is dropped."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    # Create and drop transaction without explicit commit/rollback
    tx = await conn.start_transaction()

    await tx.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    # Explicitly drop the transaction (simulating going out of scope)
    del tx

    # Give some time for cleanup
    await asyncio.sleep(0.1)

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (0,)

    await cleanup_test_table_async(conn)
    await conn.disconnect()
