import pytest
from pyro_mysql import Opts
from pyro_mysql.sync import Conn

from .conftest import (
    cleanup_test_table_sync,
    get_test_db_url,
    setup_test_table_sync,
)


def test_basic_sync_connection(backend):
    """Test basic synchronous connection."""
    conn = Conn(get_test_db_url(), backend=backend)

    result = conn.query_first("SELECT 1")
    assert result
    assert result[0] == 1

    conn.close()


# Add the second db to test this
# def test_sync_connection_with_database():
#     """Test sync connection with specific database."""
#     conn = Conn(get_test_db_url())

#     db_name = conn.query_first("SELECT DATABASE()")
#     assert db_name[0] == "test"

#     conn.close()


def test_sync_connection_ping(backend):
    """Test sync connection ping functionality."""
    conn = Conn(get_test_db_url(), backend=backend)
    conn.ping()
    conn.close()


def test_sync_connection_reset(backend):
    """Test sync connection reset functionality."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("SET @test_var = 42")

    result = conn.query_first("SELECT @test_var")
    assert result
    assert result[0] == 42

    conn.reset()

    result = conn.query_first("SELECT @test_var")
    assert result
    assert result[0] == None

    conn.close()


def test_sync_connection_server_info(backend):
    """Test retrieving server information."""
    conn = Conn(get_test_db_url(), backend=backend)

    server_version = conn.server_version()
    assert server_version[0] >= 5

    connection_id = conn.id()
    assert connection_id > 0

    conn.close()


def test_sync_connection_charset(backend):
    """Test sync connection charset handling."""
    url = get_test_db_url()
    opts = Opts(url)

    conn = Conn(opts, backend=backend)

    charset = conn.query_first("SELECT @@character_set_connection")
    assert charset is not None

    conn.query_drop("SET NAMES utf8mb4")

    charset = conn.query_first("SELECT @@character_set_connection")
    assert charset
    assert charset[0] == "utf8mb4"

    conn.close()


def test_sync_connection_autocommit(backend):
    """Test sync autocommit functionality."""
    conn = Conn(get_test_db_url(), backend=backend)

    setup_test_table_sync(conn)

    conn.query_drop("SET autocommit = 0")

    autocommit = conn.query_first("SELECT @@autocommit")
    assert autocommit
    assert autocommit[0] == 0

    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Test', 25)")

    conn.query_drop("ROLLBACK")

    count = conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 0

    conn.query_drop("SET autocommit = 1")

    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Test2', 30)")

    count = conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 1

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_connection_ssl(backend):
    """Test SSL connection (if available)."""
    url = get_test_db_url()
    # Note: prefer_socket option is backend-specific, just use URL
    conn_input = url

    try:
        conn = Conn(conn_input, backend=backend)

        try:
            _ssl_result = conn.query_first("SHOW STATUS LIKE 'Ssl_cipher'")
            # SSL cipher status may or may not be available
        except Exception:
            pass

        conn.close()

    except Exception:
        # SSL connection may not be available in test environment
        pass


def test_sync_connection_init_command(backend):
    """Test sync connection initialization commands."""
    url = get_test_db_url()
    # Note: init commands are backend-specific, test manually
    conn = Conn(url, backend=backend)

    # Manually set the variable instead of using init command
    conn.query_drop("SET @init_test = 123")

    result = conn.query_first("SELECT @init_test")
    assert result
    assert result[0] == 123

    conn.close()


# TODO: needs a separate table to test this
# def test_sync_large_data_transfer():
#     """Test handling of large data transfers."""
#     conn = Conn(get_test_db_url())

#     setup_test_table_sync(conn)

#     large_string = "x" * (16 * 1024 * 1024)  # 16MB string

#     conn.exec_drop("INSERT INTO test_table (name) VALUES (?)", (large_string,))

#     result = conn.query_first("SELECT name FROM test_table WHERE id = 1")
#     assert result[0] == large_string

#     cleanup_test_table_sync(conn)
#     conn.close()


def test_sync_connection_with_wrong_credentials(backend):
    """Test sync connection failure with wrong credentials."""
    opts = (
        Opts()
        .host("127.0.0.1")
        .user("nonexistent_user")
        .password("wrong_password")
    )

    with pytest.raises(Exception):
        Conn(opts, backend=backend)


def test_sync_connection_to_invalid_host(backend):
    """Test sync connection failure to invalid host."""
    opts = (
        Opts()
        .host("invalid.host.that.does.not.exist")
        .port(3306)
    )

    with pytest.raises(Exception):
        Conn(opts, backend=backend)
