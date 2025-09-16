import pytest
from pyro_mysql.sync import Conn

from .conftest import (
    cleanup_test_table_sync,
    get_sync_opts,
    get_test_db_url,
    setup_test_table_sync,
)


def test_basic_sync_query():
    """Test basic synchronous query execution."""
    conn = Conn(get_test_db_url())

    result = conn.query("SELECT 1 UNION SELECT 2 UNION SELECT 3")

    assert len(result) == 3
    assert result[0].to_tuple() == (1,)
    assert result[1].to_tuple() == (2,)
    assert result[2].to_tuple() == (3,)



def test_sync_query_with_params():
    """Test sync query execution with parameters."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    results = conn.exec("SELECT name, age FROM test_table WHERE age > ?", (20,))

    assert len(results) == 2

    results = conn.exec("SELECT name, age FROM test_table WHERE age = ?", (25,))

    assert len(results) == 1
    assert results[0].to_tuple() == ("Bob", 25)

    cleanup_test_table_sync(conn)


def test_sync_query_first():
    """Test sync query_first method."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    result = conn.exec_first("SELECT name, age FROM test_table ORDER BY age DESC", ())

    assert result.to_tuple() == ("Alice", 30)

    result = conn.exec_first("SELECT name, age FROM test_table WHERE age > ?", (100,))

    assert result is None

    cleanup_test_table_sync(conn)


def test_sync_query_iter():
    """Test sync query_iter functionality."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25, "Charlie", 35),
    )

    result_iter = conn.query_iter("SELECT name, age FROM test_table ORDER BY age")

    count = 0
    expected_results = [("Bob", 25), ("Alice", 30), ("Charlie", 35)]

    for row in result_iter:
        name, age = row
        assert (name, age) == expected_results[count]
        count += 1

    assert count == 3

    cleanup_test_table_sync(conn)


def test_sync_named_params():
    """Test sync named parameter queries."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    params = {
        "name": "Alice",
        "age": 30,
    }

    conn.exec_drop("INSERT INTO test_table (name, age) VALUES (:name, :age)", params)

    result = conn.exec_first(
        "SELECT name, age FROM test_table WHERE name = :name", {"name": "Alice"}
    )

    assert result.to_tuple() == ("Alice", 30)

    cleanup_test_table_sync(conn)


def test_sync_batch_exec():
    """Test sync batch execution."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    params = [
        ("Alice", 30),
        ("Bob", 25),
        ("Charlie", 35),
        ("David", 40),
        ("Eve", 28),
    ]

    conn.exec_batch("INSERT INTO test_table (name, age) VALUES (?, ?)", params)

    count = conn.query_first("SELECT COUNT(*) FROM test_table")

    assert count.to_tuple() == (5,)

    cleanup_test_table_sync(conn)


def test_sync_query_with_nulls():
    """Test sync handling of NULL values in queries."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, NULL)",
        ("Alice", 30, "Bob"),
    )

    results = conn.query("SELECT name, age FROM test_table ORDER BY name")

    assert len(results) == 2
    assert results[0].to_tuple() == ("Alice", 30)
    assert results[1].to_tuple() == ("Bob", None)

    cleanup_test_table_sync(conn)


def test_sync_multi_statement_query():
    """Test sync multi-statement query execution."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.query_drop(
        "INSERT INTO test_table (name, age) VALUES ('Alice', 30); "
        "INSERT INTO test_table (name, age) VALUES ('Bob', 25);"
    )

    count = conn.query_first("SELECT COUNT(*) FROM test_table")

    assert count.to_tuple() == (2,)

    cleanup_test_table_sync(conn)


def test_sync_last_insert_id():
    """Test sync last_insert_id functionality."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.exec_drop("INSERT INTO test_table (name, age) VALUES (?, ?)", ("Alice", 30))

    last_id = conn.last_insert_id()
    assert last_id is not None
    assert last_id > 0

    conn.exec_drop("INSERT INTO test_table (name, age) VALUES (?, ?)", ("Bob", 25))

    new_last_id = conn.last_insert_id()
    assert new_last_id is not None
    assert new_last_id > last_id

    cleanup_test_table_sync(conn)


def test_sync_affected_rows():
    """Test sync affected_rows functionality."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25, "Charlie", 35),
    )

    affected_rows = conn.affected_rows()
    assert affected_rows == 3

    conn.exec_drop("UPDATE test_table SET age = age + 1 WHERE age > ?", (25,))

    affected_rows = conn.affected_rows()
    assert affected_rows == 2

    conn.exec_drop("DELETE FROM test_table WHERE age < ?", (30,))

    affected_rows = conn.affected_rows()
    assert affected_rows == 1

    cleanup_test_table_sync(conn)
