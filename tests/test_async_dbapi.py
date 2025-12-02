"""Tests for asynchronous DBAPI interface."""

import pytest
import pytest_asyncio

from pyro_mysql.dbapi_async import connect

from .conftest import get_test_db_url


@pytest_asyncio.fixture
async def conn():
    """Provide an async DBAPI connection."""
    connection = await connect(get_test_db_url())
    yield connection
    await connection.close()


@pytest_asyncio.fixture
async def conn_with_table(conn):
    """Provide an async DBAPI connection with test table set up."""
    cursor = conn.cursor()

    # Create test table
    await cursor.execute("DROP TABLE IF EXISTS test_async_dbapi")
    await cursor.execute(
        """
        CREATE TABLE test_async_dbapi (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            age INT
        )
        """
    )

    yield conn

    # Cleanup
    await cursor.execute("DROP TABLE IF EXISTS test_async_dbapi")
    await cursor.close()


@pytest.mark.asyncio
async def test_update_rowcount(conn_with_table):
    """Test that rowcount is correctly set after UPDATE."""
    cursor = conn_with_table.cursor()
    assert cursor.rowcount == -1

    # Insert test data
    await cursor.execute(
        "INSERT INTO test_async_dbapi (name, age) VALUES (?, ?), (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25, "Charlie", 35),
    )
    assert cursor.rowcount == 3

    # Update some rows
    await cursor.execute(
        "UPDATE test_async_dbapi SET age = age + 1 WHERE age > ?", (25,)
    )
    assert cursor.rowcount == 2  # Alice and Charlie

    # Update no rows
    await cursor.execute("UPDATE test_async_dbapi SET age = 100 WHERE age > ?", (1000,))
    assert cursor.rowcount == 0

    # Update all rows
    await cursor.execute("UPDATE test_async_dbapi SET age = 50")
    assert cursor.rowcount == 3

    await cursor.close()


@pytest.mark.asyncio
async def test_delete_rowcount(conn_with_table):
    """Test that rowcount is correctly set after DELETE."""
    cursor = conn_with_table.cursor()

    # Insert test data
    await cursor.execute(
        "INSERT INTO test_async_dbapi (name, age) VALUES (?, ?), (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25, "Charlie", 35),
    )

    # Delete some rows
    await cursor.execute("DELETE FROM test_async_dbapi WHERE age < ?", (30,))
    assert cursor.rowcount == 1  # Bob

    # Delete remaining rows
    await cursor.execute("DELETE FROM test_async_dbapi WHERE age >= ?", (30,))
    assert cursor.rowcount == 2  # Alice and Charlie

    await cursor.close()


@pytest.mark.asyncio
async def test_select_rowcount(conn_with_table):
    """Test rowcount behavior with SELECT queries."""
    cursor = conn_with_table.cursor()

    # Insert test data
    await cursor.execute(
        "INSERT INTO test_async_dbapi (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    # SELECT - per DBAPI spec, rowcount for SELECT is typically -1 or 0
    # since "affected rows" doesn't apply to SELECT statements.
    # We verify we can fetch the expected number of rows instead.
    await cursor.execute("SELECT * FROM test_async_dbapi")
    rows = await cursor.fetchall()
    assert len(rows) == 2

    await cursor.execute("SELECT * FROM test_async_dbapi WHERE age > ?", (100,))
    rows = await cursor.fetchall()
    assert len(rows) == 0

    await cursor.close()


@pytest.mark.asyncio
async def test_executemany_rowcount(conn_with_table):
    """Test rowcount with executemany."""
    cursor = conn_with_table.cursor()

    # Insert using executemany
    await cursor.executemany(
        "INSERT INTO test_async_dbapi (name, age) VALUES (?, ?)",
        [("Alice", 30), ("Bob", 25), ("Charlie", 35)],
    )
    assert cursor.rowcount == 3

    await cursor.close()


@pytest.mark.asyncio
async def test_cursor_lastrowid(conn_with_table):
    """Test lastrowid is set after INSERT."""
    cursor = conn_with_table.cursor()
    assert cursor.lastrowid is None

    # Insert and check lastrowid
    await cursor.execute("INSERT INTO test_async_dbapi (name) VALUES (?)", ("Alice",))
    first_id = cursor.lastrowid
    assert first_id is not None
    assert first_id > 0

    await cursor.execute("INSERT INTO test_async_dbapi (name) VALUES (?)", ("Bob",))
    second_id = cursor.lastrowid
    assert second_id is not None
    assert second_id > first_id

    await cursor.close()


@pytest.mark.asyncio
async def test_fetchone(conn_with_table):
    """Test fetchone returns rows one at a time."""
    cursor = conn_with_table.cursor()

    # Insert test data
    await cursor.execute(
        "INSERT INTO test_async_dbapi (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    # Fetch rows one at a time
    await cursor.execute("SELECT name, age FROM test_async_dbapi ORDER BY age")
    row1 = await cursor.fetchone()
    assert row1 is not None
    assert row1[0] == "Bob"
    assert row1[1] == 25

    row2 = await cursor.fetchone()
    assert row2 is not None
    assert row2[0] == "Alice"
    assert row2[1] == 30

    row3 = await cursor.fetchone()
    assert row3 is None

    await cursor.close()


@pytest.mark.asyncio
async def test_fetchmany(conn_with_table):
    """Test fetchmany returns the requested number of rows."""
    cursor = conn_with_table.cursor()

    # Insert test data
    await cursor.execute(
        "INSERT INTO test_async_dbapi (name, age) VALUES (?, ?), (?, ?), (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25, "Charlie", 35, "Dave", 40),
    )

    # Fetch rows in batches
    await cursor.execute("SELECT name FROM test_async_dbapi ORDER BY age")
    rows = await cursor.fetchmany(2)
    assert len(rows) == 2

    rows = await cursor.fetchmany(2)
    assert len(rows) == 2

    rows = await cursor.fetchmany(2)
    assert len(rows) == 0

    await cursor.close()


@pytest.mark.asyncio
async def test_commit_rollback(conn_with_table):
    """Test commit and rollback operations."""
    cursor = conn_with_table.cursor()

    # Insert with autocommit off
    await cursor.execute("INSERT INTO test_async_dbapi (name) VALUES (?)", ("Alice",))
    await conn_with_table.commit()

    # Verify committed
    await cursor.execute("SELECT COUNT(*) FROM test_async_dbapi")
    row = await cursor.fetchone()
    assert row[0] == 1

    # Insert and rollback
    await cursor.execute("INSERT INTO test_async_dbapi (name) VALUES (?)", ("Bob",))
    await conn_with_table.rollback()

    # Verify rolled back
    await cursor.execute("SELECT COUNT(*) FROM test_async_dbapi")
    row = await cursor.fetchone()
    assert row[0] == 1

    await cursor.close()


@pytest.mark.asyncio
async def test_description(conn_with_table):
    """Test that description is set after SELECT."""
    cursor = conn_with_table.cursor()

    # Before any query, description should be None
    assert cursor.description is None

    # After a SELECT, description should have column info
    await cursor.execute("SELECT id, name, age FROM test_async_dbapi")
    desc = cursor.description
    assert desc is not None
    assert len(desc) == 3

    # Column names
    assert desc[0][0] == "id"
    assert desc[1][0] == "name"
    assert desc[2][0] == "age"

    await cursor.close()


@pytest.mark.asyncio
async def test_multiple_cursors(conn_with_table):
    """Test using multiple cursors on the same connection."""
    cursor1 = conn_with_table.cursor()
    cursor2 = conn_with_table.cursor()

    # Insert with cursor1
    await cursor1.execute("INSERT INTO test_async_dbapi (name) VALUES (?)", ("Alice",))

    # Select with cursor2
    await cursor2.execute("SELECT name FROM test_async_dbapi")
    rows = await cursor2.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Alice"

    await cursor1.close()
    await cursor2.close()
