import pytest

from .conftest import (
    cleanup_test_table_async,
    get_async_conn_with_backend,
    get_test_db_url,
    setup_test_table_async,
)


@pytest.mark.asyncio
async def test_basic_query(async_backend):
    """Test basic query execution."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    result = await conn.query("SELECT 1 UNION SELECT 2 UNION SELECT 3")

    assert len(result) == 3
    assert result[0][0] == 1
    assert result[1][0] == 2
    assert result[2][0] == 3

    await conn.close()


@pytest.mark.asyncio
async def test_query_with_params(async_backend):
    """Test query execution with parameters."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    results = await conn.exec("SELECT name, age FROM test_table WHERE age > ?", (20,))

    assert len(results) == 2

    results = await conn.exec("SELECT name, age FROM test_table WHERE age = ?", (25,))

    assert len(results) == 1
    assert (results[0][0], results[0][1]) == ("Bob", 25)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_query_first(async_backend):
    """Test query_first method."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    result = await conn.exec_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", ()
    )
    assert result
    assert (result[0], result[1]) == ("Alice", 30)

    result = await conn.exec_first(
        "SELECT name, age FROM test_table WHERE age > ?", (100,)
    )

    assert result is None

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_batch_exec(async_backend):
    """Test batch execution."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    params = [
        ("Alice", 30),
        ("Bob", 25),
        ("Charlie", 35),
        ("David", 40),
        ("Eve", 28),
    ]

    await conn.exec_batch("INSERT INTO test_table (name, age) VALUES (?, ?)", params)

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 5

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_query_with_nulls(async_backend):
    """Test handling of NULL values in queries."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, NULL)",
        ("Alice", 30, "Bob"),
    )

    results = await conn.query("SELECT name, age FROM test_table ORDER BY name")

    assert len(results) == 2
    assert (results[0][0], results[0][1]) == ("Alice", 30)
    assert (results[1][0], results[1][1]) == ("Bob", None)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_multi_statement_query(async_backend):
    """Test multi-statement query execution."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    await conn.query_drop(
        "INSERT INTO test_table (name, age) VALUES ('Alice', 30); "
        "INSERT INTO test_table (name, age) VALUES ('Bob', 25);"
    )

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 2

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_last_insert_id(async_backend):
    """Test last_insert_id functionality."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30)
    )

    last_id = await conn.last_insert_id()
    assert last_id is not None
    assert last_id > 0

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?)", ("Bob", 25)
    )

    new_last_id = await conn.last_insert_id()
    assert new_last_id is not None
    assert new_last_id > last_id

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_affected_rows(async_backend):
    """Test affected_rows functionality."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25, "Charlie", 35),
    )

    affected_rows = await conn.affected_rows()
    assert affected_rows == 3

    await conn.exec_drop("UPDATE test_table SET age = age + 1 WHERE age > ?", (25,))

    affected_rows = await conn.affected_rows()
    assert affected_rows == 2

    await conn.exec_drop("DELETE FROM test_table WHERE age < ?", (30,))

    affected_rows = await conn.affected_rows()
    assert affected_rows == 1

    await cleanup_test_table_async(conn)
    await conn.close()
