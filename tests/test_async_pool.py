import asyncio
from datetime import timedelta

import pytest
from pyro_mysql.async_ import Pool, PoolOpts

from .conftest import cleanup_test_table_async, get_async_opts, setup_test_table_async


@pytest.mark.asyncio
async def test_basic_pool():
    """Test basic pool functionality."""
    opts = get_async_opts()
    pool = Pool(opts)

    conn = await pool.get_conn()

    result = await conn.query_first("SELECT 1")
    assert result
    assert result.to_tuple() == (1,)

    await conn.close()
    await pool.close()


@pytest.mark.asyncio
async def test_pool_constraints():
    """Test pool with constraints."""
    opts = get_async_opts()
    pool_opts = (
        PoolOpts()
        .with_constraints((2, 10))
        .with_inactive_connection_ttl(timedelta(seconds=60))
        .with_ttl_check_interval(timedelta(seconds=30))
    )

    pool = Pool(opts.pool_opts(pool_opts))

    conn1 = await pool.get_conn()
    conn2 = await pool.get_conn()

    result1 = await conn1.query_first("SELECT 1")
    result2 = await conn2.query_first("SELECT 2")

    assert result1
    assert result1.to_tuple() == (1,)
    assert result2
    assert result2.to_tuple() == (2,)

    await conn1.close()
    await conn2.close()
    await pool.close()


@pytest.mark.asyncio
async def test_concurrent_connections():
    """Test multiple concurrent connections from pool."""
    opts = get_async_opts()
    pool_opts = PoolOpts().with_constraints((2, 5))
    pool = Pool(opts.pool_opts(pool_opts))

    async def worker(worker_id):
        conn = await pool.get_conn()
        result = await conn.query_first(f"SELECT {worker_id}")
        await conn.close()
        assert result
        return result.to_tuple()

    # Create multiple concurrent workers
    tasks = [worker(i) for i in range(1, 6)]
    results = await asyncio.gather(*tasks)

    expected = [(i,) for i in range(1, 6)]
    assert sorted(results) == sorted(expected)

    await pool.close()


@pytest.mark.asyncio
async def test_pool_with_transactions():
    """Test pool connections with transactions."""
    opts = get_async_opts()
    pool = Pool(opts)

    conn = await pool.get_conn()
    await setup_test_table_async(conn)

    async with conn.start_transaction() as tx:
        await tx.exec_drop(
            "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
        )

        count = await tx.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count.to_tuple() == (1,)

        await tx.commit()

    await cleanup_test_table_async(conn)
    await conn.close()
    await pool.close()


# TODO: how to test the reuse?
# @pytest.mark.asyncio
# async def test_pool_connection_reuse():
#     """Test that pool connections are properly reused."""
#     opts = get_async_opts()
#     pool_opts = PoolOpts().with_constraints((1, 1))
#     pool = Pool(opts.pool_opts(pool_opts))

#     # Get and release a connection
#     conn1 = await pool.get_conn()
#     connection_id1 = await conn1.id()
#     await conn1.close()

#     # Get another connection - should be the same one reused
#     conn2 = await pool.get_conn()
#     connection_id2 = await conn2.id()
#     await conn2.close()

#     # Note: Connection IDs might be different
#     # due to MySQL server behavior
#     # but the test verifies pool functionality
#     await pool.close()


@pytest.mark.asyncio
async def test_pool_max_connections():
    """Test pool respects maximum connection limits."""
    opts = get_async_opts()
    pool_opts = PoolOpts().with_constraints((1, 2))
    pool = Pool(opts.pool_opts(pool_opts))

    conn1 = await pool.get_conn()
    conn2 = await pool.get_conn()

    # Both connections should work
    result1 = await conn1.query_first("SELECT 1")
    result2 = await conn2.query_first("SELECT 2")

    assert result1
    assert result1.to_tuple() == (1,)
    assert result2
    assert result2.to_tuple() == (2,)

    await conn1.close()
    await conn2.close()
    await pool.close()


@pytest.mark.asyncio
async def test_pool_connection_timeout():
    """Test pool connection timeout behavior."""
    opts = get_async_opts()
    pool_opts = (
        PoolOpts()
        .with_constraints((1, 1))
        .with_inactive_connection_ttl(timedelta(milliseconds=100))
    )

    pool = Pool(opts.pool_opts(pool_opts))

    conn = await pool.get_conn()
    await conn.query_first("SELECT 1")
    await conn.close()

    # Wait for connection to potentially expire
    await asyncio.sleep(0.2)

    # Get another connection
    conn2 = await pool.get_conn()
    result = await conn2.query_first("SELECT 2")
    assert result
    assert result.to_tuple() == (2,)

    await conn2.close()
    await pool.close()
