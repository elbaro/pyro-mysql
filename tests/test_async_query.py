import pytest
from pyro_mysql.async_ import Conn

from .conftest import cleanup_test_table_async, get_async_opts, setup_test_table_async


@pytest.mark.asyncio
async def test_basic_query():
    """Test basic query execution."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    result = await conn.query("SELECT 1 UNION SELECT 2 UNION SELECT 3")

    assert len(result) == 3
    assert result[0].to_tuple() == (1,)
    assert result[1].to_tuple() == (2,)
    assert result[2].to_tuple() == (3,)

    await conn.close()


@pytest.mark.asyncio
async def test_query_with_params():
    """Test query execution with parameters."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    results = await conn.exec("SELECT name, age FROM test_table WHERE age > ?", (20,))

    assert len(results) == 2

    results = await conn.exec("SELECT name, age FROM test_table WHERE age = ?", (25,))

    assert len(results) == 1
    assert results[0].to_tuple() == ("Bob", 25)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_query_first():
    """Test query_first method."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    result = await conn.exec_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", ()
    )
    assert result
    assert result.to_tuple() == ("Alice", 30)

    result = await conn.exec_first(
        "SELECT name, age FROM test_table WHERE age > ?", (100,)
    )

    assert result is None

    await cleanup_test_table_async(conn)
    await conn.close()


# TODO
# @pytest.mark.asyncio
# async def test_query_iter():
#     """Test query_iter functionality."""
#     opts = get_async_opts()
#     conn = await Conn.new(opts)

#     await setup_test_table_async(conn)

#     await conn.exec_drop(
#         "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?), (?, ?)",
#         ("Alice", 30, "Bob", 25, "Charlie", 35),
#     )

#     result_iter = await conn.query_iter("SELECT name, age FROM test_table ORDER BY age")

#     count = 0
#     expected_results = [("Bob", 25), ("Alice", 30), ("Charlie", 35)]

#     async for row in result_iter:
#         name, age = row
#         assert (name, age) == expected_results[count]
#         count += 1

#     assert count == 3

#     await cleanup_test_table_async(conn)
#     await conn.close()


@pytest.mark.asyncio
async def test_named_params():
    """Test named parameter queries."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    params = {
        "name": "Alice",
        "age": 30,
    }

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (:name, :age)", params
    )

    result = await conn.exec_first(
        "SELECT name, age FROM test_table WHERE name = :name", {"name": "Alice"}
    )
    assert result
    assert result.to_tuple() == ("Alice", 30)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_batch_exec():
    """Test batch execution."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

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
    assert count.to_tuple() == (5,)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_query_with_nulls():
    """Test handling of NULL values in queries."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, NULL)",
        ("Alice", 30, "Bob"),
    )

    results = await conn.query("SELECT name, age FROM test_table ORDER BY name")

    assert len(results) == 2
    assert results[0].to_tuple() == ("Alice", 30)
    assert results[1].to_tuple() == ("Bob", None)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_multi_statement_query():
    """Test multi-statement query execution."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

    await setup_test_table_async(conn)

    await conn.query_drop(
        "INSERT INTO test_table (name, age) VALUES ('Alice', 30); "
        "INSERT INTO test_table (name, age) VALUES ('Bob', 25);"
    )

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count.to_tuple() == (2,)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_last_insert_id():
    """Test last_insert_id functionality."""
    opts = get_async_opts()
    conn = await Conn.new(opts)

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
    opts = get_async_opts()
    conn = await Conn.new(opts)

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
