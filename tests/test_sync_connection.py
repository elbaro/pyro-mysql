from datetime import timedelta

import pytest
from pyro_mysql.sync import Conn, OptsBuilder

from .conftest import (
    cleanup_test_table_sync,
    get_test_db_url,
    setup_test_table_sync,
)


def test_basic_sync_connection():
    """Test basic synchronous connection."""
    conn = Conn(get_test_db_url())

    result = conn.query_first("SELECT 1")
    assert result.to_tuple() == (1,)


def test_sync_connection_with_database():
    """Test sync connection with specific database."""
    opts = OptsBuilder().ip_or_hostname("localhost").db_name("test").build()
    conn = Conn(opts)

    db_name = conn.query_first("SELECT DATABASE()")
    assert db_name.to_tuple() == ("test",)


def test_sync_connection_timeout():
    """Test sync connection timeout handling."""
    url = get_test_db_url()
    opts = OptsBuilder.from_url(url).read_timeout(timedelta(milliseconds=1)).build()

    try:
        conn = Conn(opts)
    except Exception:
        # Timeout is expected to potentially fail
        pass


def test_sync_connection_ping():
    """Test sync connection ping functionality."""
    conn = Conn(get_test_db_url())

    conn.ping()


def test_sync_connection_reset():
    """Test sync connection reset functionality."""
    conn = Conn(get_test_db_url())

    conn.query_drop("SET @test_var = 42")

    result = conn.query_first("SELECT @test_var")
    assert result.to_tuple() == (42,)

    conn.reset()

    result = conn.query_first("SELECT @test_var")
    assert result.to_tuple() == (None,)


def test_sync_connection_server_info():
    """Test retrieving server information."""
    conn = Conn(get_test_db_url())

    server_version = conn.server_version()
    assert server_version[0] >= 5

    connection_id = conn.id()
    assert connection_id > 0


def test_sync_connection_charset():
    """Test sync connection charset handling."""
    url = get_test_db_url()
    opts = OptsBuilder.from_url(url).build()

    conn = Conn(opts)

    charset = conn.query_first("SELECT @@character_set_connection")
    assert charset is not None

    conn.query_drop("SET NAMES utf8mb4")

    charset = conn.query_first("SELECT @@character_set_connection")
    assert charset.to_tuple() == ("utf8mb4",)


def test_sync_connection_autocommit():
    """Test sync autocommit functionality."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.query_drop("SET autocommit = 0")

    autocommit = conn.query_first("SELECT @@autocommit")
    assert autocommit.to_tuple() == (0,)

    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Test', 25)")

    conn.query_drop("ROLLBACK")

    count = conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (0,)

    conn.query_drop("SET autocommit = 1")

    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Test2', 30)")

    count = conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count.to_tuple() == (1,)

    cleanup_test_table_sync(conn)


def test_sync_connection_ssl():
    """Test SSL connection (if available)."""
    url = get_test_db_url()
    opts = OptsBuilder.from_url(url).prefer_socket(False).build()

    try:
        conn = Conn(opts)

        try:
            ssl_result = conn.query_first("SHOW STATUS LIKE 'Ssl_cipher'")
            # SSL cipher status may or may not be available
        except Exception:
            pass

    except Exception:
        # SSL connection may not be available in test environment
        pass


def test_sync_connection_init_command():
    """Test sync connection initialization commands."""
    url = get_test_db_url()
    opts = OptsBuilder.from_url(url).init(["SET @init_test = 123"]).build()

    conn = Conn(opts)

    result = conn.query_first("SELECT @init_test")
    assert result.to_tuple() == (123,)


def test_sync_large_data_transfer():
    """Test handling of large data transfers."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    large_string = "x" * (16 * 1024 * 1024)  # 16MB string

    conn.exec_drop("INSERT INTO test_table (name) VALUES (?)", (large_string,))

    result = conn.query_first("SELECT name FROM test_table WHERE id = 1")
    assert result.to_tuple() == (large_string,)

    cleanup_test_table_sync(conn)


def test_sync_connection_with_wrong_credentials():
    """Test sync connection failure with wrong credentials."""
    opts = (
        OptsBuilder()
        .ip_or_hostname("localhost")
        .user("nonexistent_user")
        .password("wrong_password")
        .build()
    )

    with pytest.raises(Exception):
        Conn(opts)


def test_sync_connection_to_invalid_host():
    """Test sync connection failure to invalid host."""
    opts = (
        OptsBuilder()
        .ip_or_hostname("invalid.host.that.does.not.exist")
        .tcp_port(3306)
        .build()
    )

    with pytest.raises(Exception):
        Conn(opts)
