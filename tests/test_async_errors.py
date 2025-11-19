import pytest
from pyro_mysql import Opts

from .conftest import get_async_conn_with_backend, get_async_opts, get_test_db_url

# TODO: hangs
# @pytest.mark.asyncio
# async def test_wait_timeout_error():
#     """Test wait timeout errors."""
#     opts = (
#         OptsBuilder()
#         .ip_or_hostname("192.0.2.0")  # Non-routable IP
#         .tcp_port(3306)
#         .wait_timeout(1)
#         .build()
#     )

#     with pytest.raises(Exception):
#         await Conn.new(opts)


@pytest.mark.asyncio
@pytest.mark.parametrize("async_backend", ["mysql_async"], indirect=True)
async def test_invalid_credentials_error(async_backend):
    """Test invalid credentials error (mysql_async only)."""
    opts = (
        Opts()
        .host("127.0.0.1")
        .port(3306)
        .user("nonexistent_user")
        .password("wrong_password")
    )

    with pytest.raises(Exception) as exc_info:
        await get_async_conn_with_backend(opts, async_backend)

    # Should be an authentication error
    assert exc_info.value is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("async_backend", ["mysql_async"], indirect=True)
async def test_invalid_database_error(async_backend):
    """Test connecting to non-existent database (mysql_async only)."""
    url = get_test_db_url()
    opts = Opts(url).db("nonexistent_database")

    with pytest.raises(Exception):
        await get_async_conn_with_backend(opts, async_backend)


@pytest.mark.asyncio
async def test_syntax_error_in_query(async_backend):
    """Test SQL syntax errors."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    with pytest.raises(Exception):
        await conn.query("INVALID SQL SYNTAX")

    await conn.close()


@pytest.mark.asyncio
async def test_table_not_found_error(async_backend):
    """Test table not found errors."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    with pytest.raises(Exception):
        await conn.query("SELECT * FROM nonexistent_table")

    await conn.close()


@pytest.mark.asyncio
async def test_duplicate_key_error(async_backend):
    """Test duplicate key constraint errors."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

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
    await conn.close()


@pytest.mark.asyncio
async def test_data_too_long_error(async_backend):
    """Test data too long errors."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

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
    await conn.close()


@pytest.mark.asyncio
async def test_foreign_key_constraint_error(async_backend):
    """Test foreign key constraint errors."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

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
    await conn.close()


@pytest.mark.asyncio
async def test_connection_lost_error(async_backend):
    """Test handling of lost connections."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    # Force close and try to use connection
    await conn.close()

    with pytest.raises(Exception):
        await conn.query("SELECT 1")


# TODO: raise timeout
# @pytest.mark.asyncio
# async def test_pool_exhaustion():
#     """Test pool connection exhaustion."""
#     opts = get_async_opts()

#     pool_opts = PoolOpts().with_constraints((1, 1))
#     pool = Pool(opts.pool_opts(pool_opts))

#     # Get the only available connection
#     conn1 = await pool.get()

#     # Trying to get another connection should timeout or fail
#     # (behavior depends on implementation)
#     try:
#         conn2 = await pool.get()
#         await conn2.close()
#     except Exception:
#         pass  # Expected to potentially fail

#     await conn1.close()
#     await pool.close()
