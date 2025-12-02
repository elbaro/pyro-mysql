import pytest

from .conftest import (
    cleanup_test_table_async,
    get_async_conn,
    get_test_db_url,
    setup_test_table_async,
)


@pytest.mark.asyncio
async def test_basic_query():
    """Test basic query execution."""
    conn = await get_async_conn(get_test_db_url())

    result = await conn.query("SELECT 1 UNION SELECT 2 UNION SELECT 3")

    assert len(result) == 3
    assert result[0][0] == 1
    assert result[1][0] == 2
    assert result[2][0] == 3

    await conn.close()


@pytest.mark.asyncio
async def test_query_with_params():
    """Test query execution with parameters."""
    conn = await get_async_conn(get_test_db_url())

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
async def test_query_first():
    """Test query_first method."""
    conn = await get_async_conn(get_test_db_url())

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
async def test_batch_exec():
    """Test batch execution."""
    conn = await get_async_conn(get_test_db_url())

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
async def test_query_with_nulls():
    """Test handling of NULL values in queries."""
    conn = await get_async_conn(get_test_db_url())

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
async def test_multi_statement_query():
    """Test multi-statement query execution."""
    conn = await get_async_conn(get_test_db_url())

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
async def test_last_insert_id():
    """Test last_insert_id functionality."""
    conn = await get_async_conn(get_test_db_url())

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
async def test_affected_rows():
    """Test affected_rows functionality."""
    conn = await get_async_conn(get_test_db_url())

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


# ─── as_dict=True Tests ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_query_as_dict():
    """Test async query with as_dict=True returns dictionaries."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    results = await conn.query(
        "SELECT name, age FROM test_table ORDER BY age", as_dict=True
    )

    assert len(results) == 2
    assert isinstance(results[0], dict)
    assert isinstance(results[1], dict)
    assert results[0]["name"] == "Bob"
    assert results[0]["age"] == 25
    assert results[1]["name"] == "Alice"
    assert results[1]["age"] == 30

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_query_first_as_dict():
    """Test async query_first with as_dict=True returns dictionary."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    result = await conn.query_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", as_dict=True
    )

    assert result is not None
    assert isinstance(result, dict)
    assert result["name"] == "Alice"
    assert result["age"] == 30

    # Test with no results
    result = await conn.query_first(
        "SELECT name, age FROM test_table WHERE age > 100", as_dict=True
    )
    assert result is None

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_exec_as_dict():
    """Test async exec with as_dict=True returns dictionaries."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    results = await conn.exec(
        "SELECT name, age FROM test_table WHERE age > ?", (20,), as_dict=True
    )

    assert len(results) == 2
    assert all(isinstance(r, dict) for r in results)

    # Check that we can access by column name
    names = {r["name"] for r in results}
    assert names == {"Alice", "Bob"}

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_exec_first_as_dict():
    """Test async exec_first with as_dict=True returns dictionary."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    result = await conn.exec_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", (), as_dict=True
    )

    assert result is not None
    assert isinstance(result, dict)
    assert result["name"] == "Alice"
    assert result["age"] == 30

    # Test with no results
    result = await conn.exec_first(
        "SELECT name, age FROM test_table WHERE age > ?", (100,), as_dict=True
    )
    assert result is None

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_query_as_dict_with_nulls():
    """Test async query with as_dict=True handles NULL values correctly."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, NULL)",
        ("Alice", 30, "Bob"),
    )

    results = await conn.query(
        "SELECT name, age FROM test_table ORDER BY name", as_dict=True
    )

    assert len(results) == 2
    assert results[0]["name"] == "Alice"
    assert results[0]["age"] == 30
    assert results[1]["name"] == "Bob"
    assert results[1]["age"] is None

    await cleanup_test_table_async(conn)
    await conn.close()
