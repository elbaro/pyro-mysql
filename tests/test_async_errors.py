import pytest
from pyro_mysql import Opts

from .conftest import get_async_conn, get_async_opts, get_test_db_url


@pytest.mark.asyncio
async def test_invalid_credentials_error():
    """Test invalid credentials error."""
    with pytest.raises(Exception) as exc_info:
        await get_async_conn(
            "mysql://nonexistent_user:wrong_password@localhost:3306/test"
        )

    # Should be an authentication error
    assert exc_info.value is not None


@pytest.mark.asyncio
async def test_syntax_error_in_query():
    """Test SQL syntax errors."""
    conn = await get_async_conn(get_test_db_url())

    with pytest.raises(Exception):
        await conn.query("INVALID SQL SYNTAX")

    await conn.close()


@pytest.mark.asyncio
async def test_table_not_found_error():
    """Test table not found errors."""
    conn = await get_async_conn(get_test_db_url())

    with pytest.raises(Exception):
        await conn.query("SELECT * FROM nonexistent_table")

    await conn.close()


@pytest.mark.asyncio
async def test_duplicate_key_error():
    """Test duplicate key constraint errors."""
    conn = await get_async_conn(get_test_db_url())

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
async def test_data_too_long_error():
    """Test data too long errors."""
    conn = await get_async_conn(get_test_db_url())

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
async def test_foreign_key_constraint_error():
    """Test foreign key constraint errors."""
    conn = await get_async_conn(get_test_db_url())

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
async def test_connection_lost_error():
    """Test handling of lost connections."""
    conn = await get_async_conn(get_test_db_url())

    # Force close and try to use connection
    await conn.close()

    with pytest.raises(Exception):
        await conn.query("SELECT 1")
