from pyro_mysql.sync import Conn

from .conftest import (
    cleanup_test_table_sync,
    get_test_db_url,
    setup_test_table_sync,
)


def test_basic_sync_query(backend):
    """Test basic synchronous query execution."""
    conn = Conn(get_test_db_url(), backend=backend)

    result = conn.query("SELECT 1 UNION SELECT 2 UNION SELECT 3")

    assert len(result) == 3
    assert result[0][0] == 1
    assert result[1][0] == 2
    assert result[2][0] == 3

    conn.close()


def test_sync_query_with_params(backend):
    """Test sync query execution with parameters."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    results = conn.exec("SELECT name, age FROM test_table WHERE age > ?", (20,))

    assert len(results) == 2

    results = conn.exec("SELECT name, age FROM test_table WHERE age = ?", (25,))

    assert len(results) == 1
    assert (results[0][0], results[0][1]) == ("Bob", 25)

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_query_first(backend):
    """Test sync query_first method."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    result = conn.exec_first("SELECT name, age FROM test_table ORDER BY age DESC", ())
    assert result
    assert (result[0], result[1]) == ("Alice", 30)

    result = conn.exec_first("SELECT name, age FROM test_table WHERE age > ?", (100,))

    assert result is None

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_batch_exec(backend):
    """Test sync batch execution."""
    conn = Conn(get_test_db_url(), backend=backend)

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
    assert count
    assert count[0] == 5

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_query_with_nulls(backend):
    """Test sync handling of NULL values in queries."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, NULL)",
        ("Alice", 30, "Bob"),
    )

    results = conn.query("SELECT name, age FROM test_table ORDER BY name")

    assert len(results) == 2
    assert (results[0][0], results[0][1]) == ("Alice", 30)
    assert (results[1][0], results[1][1]) == ("Bob", None)

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_multi_statement_query(backend):
    """Test sync multi-statement query execution."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.query_drop(
        "INSERT INTO test_table (name, age) VALUES ('Alice', 30); "
        "INSERT INTO test_table (name, age) VALUES ('Bob', 25);"
    )

    count = conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 2

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_last_insert_id(backend):
    """Test sync last_insert_id functionality."""
    conn = Conn(get_test_db_url(), backend=backend)

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
    conn.close()


def test_sync_affected_rows(backend):
    """Test sync affected_rows functionality."""
    conn = Conn(get_test_db_url(), backend=backend)

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
    conn.close()


# ─── as_dict=True Tests ────────────────────────────────────────────────────


def test_sync_query_as_dict(backend):
    """Test sync query with as_dict=True returns dictionaries."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    results = conn.query("SELECT name, age FROM test_table ORDER BY age", as_dict=True)

    assert len(results) == 2
    assert isinstance(results[0], dict)
    assert isinstance(results[1], dict)
    assert results[0]["name"] == "Bob"
    assert results[0]["age"] == 25
    assert results[1]["name"] == "Alice"
    assert results[1]["age"] == 30

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_query_first_as_dict(backend):
    """Test sync query_first with as_dict=True returns dictionary."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    result = conn.query_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", as_dict=True
    )

    assert result is not None
    assert isinstance(result, dict)
    assert result["name"] == "Alice"
    assert result["age"] == 30

    # Test with no results
    result = conn.query_first(
        "SELECT name, age FROM test_table WHERE age > 100", as_dict=True
    )
    assert result is None

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_as_dict(backend):
    """Test sync exec with as_dict=True returns dictionaries."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    results = conn.exec(
        "SELECT name, age FROM test_table WHERE age > ?", (20,), as_dict=True
    )

    assert len(results) == 2
    assert all(isinstance(r, dict) for r in results)

    # Check that we can access by column name
    names = {r["name"] for r in results}
    assert names == {"Alice", "Bob"}

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_first_as_dict(backend):
    """Test sync exec_first with as_dict=True returns dictionary."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    result = conn.exec_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", (), as_dict=True
    )

    assert result is not None
    assert isinstance(result, dict)
    assert result["name"] == "Alice"
    assert result["age"] == 30

    # Test with no results
    result = conn.exec_first(
        "SELECT name, age FROM test_table WHERE age > ?", (100,), as_dict=True
    )
    assert result is None

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_query_as_dict_with_nulls(backend):
    """Test sync query with as_dict=True handles NULL values correctly."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES (?, ?), (?, NULL)",
        ("Alice", 30, "Bob"),
    )

    results = conn.query("SELECT name, age FROM test_table ORDER BY name", as_dict=True)

    assert len(results) == 2
    assert results[0]["name"] == "Alice"
    assert results[0]["age"] == 30
    assert results[1]["name"] == "Bob"
    assert results[1]["age"] is None

    cleanup_test_table_sync(conn)
    conn.close()
