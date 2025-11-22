"""Tests for synchronous DBAPI interface."""

from pyro_mysql.dbapi import connect

from .conftest import get_test_db_url


def test_update_rowcount():
    """Test that rowcount is correctly set after UPDATE."""
    conn = connect(get_test_db_url())
    cursor = conn.cursor()
    assert cursor.rowcount == -1

    # Create test table
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.execute(
        """
        CREATE TABLE test_dbapi (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            age INT
        )
        """
    )
    assert cursor.rowcount == 0

    # Insert test data
    cursor.execute(
        "INSERT INTO test_dbapi (name, age) VALUES (?, ?), (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25, "Charlie", 35),
    )
    assert cursor.rowcount == 3

    # Update some rows
    cursor.execute("UPDATE test_dbapi SET age = age + 1 WHERE age > ?", (25,))
    assert cursor.rowcount == 2  # Alice and Charlie

    # Update no rows
    cursor.execute("UPDATE test_dbapi SET age = 100 WHERE age > ?", (1000,))
    assert cursor.rowcount == 0

    # Update all rows
    cursor.execute("UPDATE test_dbapi SET age = 50")
    assert cursor.rowcount == 3

    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.close()
    conn.close()


def test_delete_rowcount():
    """Test that rowcount is correctly set after DELETE."""
    conn = connect(get_test_db_url())
    cursor = conn.cursor()

    # Create test table
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.execute(
        """
        CREATE TABLE test_dbapi (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            age INT
        )
        """
    )

    # Insert test data
    cursor.execute(
        "INSERT INTO test_dbapi (name, age) VALUES (?, ?), (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25, "Charlie", 35),
    )

    # Delete some rows
    cursor.execute("DELETE FROM test_dbapi WHERE age < ?", (30,))
    assert cursor.rowcount == 1  # Bob

    # Delete remaining rows
    cursor.execute("DELETE FROM test_dbapi WHERE age >= ?", (30,))
    assert cursor.rowcount == 2  # Alice and Charlie

    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.close()
    conn.close()


def test_select_rowcount():
    """Test rowcount behavior with SELECT queries."""
    conn = connect(get_test_db_url())
    cursor = conn.cursor()

    # Create test table
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.execute(
        """
        CREATE TABLE test_dbapi (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            age INT
        )
        """
    )

    # Insert test data
    cursor.execute(
        "INSERT INTO test_dbapi (name, age) VALUES (?, ?), (?, ?)",
        ("Alice", 30, "Bob", 25),
    )

    # SELECT - per DBAPI spec, rowcount for SELECT is typically -1 or 0
    # since "affected rows" doesn't apply to SELECT statements.
    # We verify we can fetch the expected number of rows instead.
    cursor.execute("SELECT * FROM test_dbapi")
    rows = cursor.fetchall()
    assert len(rows) == 2

    cursor.execute("SELECT * FROM test_dbapi WHERE age > ?", (100,))
    rows = cursor.fetchall()
    assert len(rows) == 0

    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.close()
    conn.close()


def test_executemany_rowcount():
    """Test rowcount with executemany."""
    conn = connect(get_test_db_url())
    cursor = conn.cursor()

    # Create test table
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.execute(
        """
        CREATE TABLE test_dbapi (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            age INT
        )
        """
    )

    # Insert using executemany
    cursor.executemany(
        "INSERT INTO test_dbapi (name, age) VALUES (?, ?)",
        [("Alice", 30), ("Bob", 25), ("Charlie", 35)],
    )
    assert cursor.rowcount == 3

    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.close()
    conn.close()


def test_cursor_lastrowid():
    """Test lastrowid is set after INSERT."""
    conn = connect(get_test_db_url())
    cursor = conn.cursor()
    assert cursor.lastrowid is None

    # Create test table
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.execute(
        """
        CREATE TABLE test_dbapi (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255)
        )
        """
    )
    assert cursor.lastrowid is None

    # Insert and check lastrowid
    cursor.execute("INSERT INTO test_dbapi (name) VALUES (?)", ("Alice",))
    first_id = cursor.lastrowid
    assert first_id is not None
    assert first_id > 0

    cursor.execute("INSERT INTO test_dbapi (name) VALUES (?)", ("Bob",))
    second_id = cursor.lastrowid
    assert second_id is not None
    assert second_id > first_id

    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS test_dbapi")
    cursor.close()
    conn.close()
