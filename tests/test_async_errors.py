from datetime import timedelta

import pytest
from pyro_mysql import AsyncOptsBuilder
from pyro_mysql.async_ import Conn, Pool

from .conftest import get_async_opts, get_test_db_url


@pytest.mark.asyncio
async def test_connection_timeout_error():
    """Test connection timeout errors."""
    opts = (
        AsyncOptsBuilder()
        .ip_or_hostname("192.0.2.0")  # Non-routable IP
        .tcp_port(3306)
        .connect_timeout(timedelta(milliseconds=100))
        .build()
    )

    with pytest.raises(Exception):
        await Conn.new(opts)


@pytest.mark.asyncio
async def test_invalid_credentials_error():
    """Test invalid credentials error."""
    opts = (
        AsyncOptsBuilder()
        .ip_or_hostname("localhost")
        .tcp_port(3306)
        .user("nonexistent_user")
        .password("wrong_password")
        .build()
    )

    with pytest.raises(Exception) as exc_info:
        await Conn.new(opts)

    # Should be an authentication error
    assert exc_info.value is not None


@pytest.mark.asyncio
async def test_invalid_database_error():
    """Test connecting to non-existent database."""
    url = get_test_db_url()
    opts = AsyncOptsBuilder.from_url(url).db_name("nonexistent_database").build()

    with pytest.raises(Exception):
        await Conn.new(opts)


@pytest.mark.asyncio
async def test_syntax_error_in_query():
    """Test SQL syntax errors."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    with pytest.raises(Exception):
        await conn.query("INVALID SQL SYNTAX")

    await conn.disconnect()


@pytest.mark.asyncio
async def test_table_not_found_error():
    """Test table not found errors."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    with pytest.raises(Exception):
        await conn.query("SELECT * FROM nonexistent_table")

    await conn.disconnect()


@pytest.mark.asyncio
async def test_duplicate_key_error():
    """Test duplicate key constraint errors."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await conn.query_drop("DROP TABLE IF EXISTS test_unique")
    await conn.query_drop(
        """
        CREATE TABLE test_unique (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        )
    """
    )

    # First insert should succeed
    await conn.exec_drop(
        "INSERT INTO test_unique (id, name) VALUES (?, ?)", (1, "test")
    )

    # Second insert with same primary key should fail
    with pytest.raises(Exception):
        await conn.exec_drop(
            "INSERT INTO test_unique (id, name) VALUES (?, ?)", (1, "test2")
        )

    await conn.query_drop("DROP TABLE test_unique")
    await conn.disconnect()


@pytest.mark.asyncio
async def test_data_too_long_error():
    """Test data too long errors."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await conn.query_drop("DROP TABLE IF EXISTS test_varchar")
    await conn.query_drop(
        """
        CREATE TABLE test_varchar (
            short_text VARCHAR(10)
        )
    """
    )

    # Should fail because string is too long
    with pytest.raises(Exception):
        await conn.exec_drop(
            "INSERT INTO test_varchar (short_text) VALUES (?)",
            ("This string is definitely longer than 10 characters",),
        )

    await conn.query_drop("DROP TABLE test_varchar")
    await conn.disconnect()


@pytest.mark.asyncio
async def test_foreign_key_constraint_error():
    """Test foreign key constraint errors."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await conn.query_drop("DROP TABLE IF EXISTS test_child")
    await conn.query_drop("DROP TABLE IF EXISTS test_parent")

    await conn.query_drop(
        """
        CREATE TABLE test_parent (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        )
    """
    )

    await conn.query_drop(
        """
        CREATE TABLE test_child (
            id INT PRIMARY KEY,
            parent_id INT,
            FOREIGN KEY (parent_id) REFERENCES test_parent(id)
        )
    """
    )

    # Should fail because parent doesn't exist
    with pytest.raises(Exception):
        await conn.exec_drop(
            "INSERT INTO test_child (id, parent_id) VALUES (?, ?)", (1, 999)
        )

    await conn.query_drop("DROP TABLE test_child")
    await conn.query_drop("DROP TABLE test_parent")
    await conn.disconnect()


@pytest.mark.asyncio
async def test_connection_lost_error():
    """Test handling of lost connections."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    # Force disconnect and try to use connection
    await conn.disconnect()

    with pytest.raises(Exception):
        await conn.query("SELECT 1")


@pytest.mark.asyncio
async def test_pool_exhaustion():
    """Test pool connection exhaustion."""
    opts = get_async_opts()
    from pyro_mysql import PoolOpts

    pool_opts = PoolOpts.new().with_constraints((1, 1))
    pool = Pool.new(opts.pool_opts(pool_opts))

    # Get the only available connection
    conn1 = await pool.get_conn()

    # Trying to get another connection should timeout or fail
    # (behavior depends on implementation)
    try:
        conn2 = await pool.get_conn()
        await conn2.disconnect()
    except Exception:
        pass  # Expected to potentially fail

    await conn1.disconnect()
    await pool.disconnect()
