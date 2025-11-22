import logging
import os

import pytest
from pyro_mysql import Opts
from sqlalchemy.dialects import registry


def pytest_configure(config):
    logging.getLogger("pyro_mysql").setLevel(logging.DEBUG)

    # Register pyro_mysql dialects explicitly since we're using a local directory
    # instead of pip install (entry points from pyproject.toml aren't available)
    registry.register("mysql.pyro_mysql", "pyro_mysql.sqlalchemy_sync", "MySQLDialect_sync")
    registry.register("mariadb.pyro_mysql", "pyro_mysql.sqlalchemy_sync", "MariaDBDialect_sync")
    registry.register("mysql.pyro_mysql_async", "pyro_mysql.sqlalchemy_async", "MySQLDialect_async")
    registry.register("mariadb.pyro_mysql_async", "pyro_mysql.sqlalchemy_async", "MariaDBDialect_async")
    # Also register with pyro_mysql:// URL scheme
    registry.register("pyro_mysql", "pyro_mysql.sqlalchemy_sync", "MySQLDialect_sync")


def get_test_db_url() -> str:
    """Get the test database URL from environment or default."""
    return os.environ.get("TEST_DATABASE_URL", "mysql://test:1234@127.0.0.1:3306/test")


def get_async_opts() -> Opts:
    """Get async connection options for testing."""
    url = get_test_db_url()
    return Opts(url)


def get_sync_opts() -> Opts:
    """Get sync connection options for testing."""
    url = get_test_db_url()
    return Opts(url)


@pytest.fixture
async def async_conn():
    """Provide an async database connection for tests."""
    from pyro_mysql.async_ import Conn

    conn = await Conn.new(get_test_db_url())

    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
def sync_conn():
    """Provide a sync database connection for tests."""
    from pyro_mysql import SyncConn

    conn = SyncConn(get_test_db_url())

    yield conn


async def setup_test_table_async(conn):
    """Set up a test table for async tests."""
    await conn.query_drop("DROP TABLE IF EXISTS test_table")
    await conn.query_drop(
        """
        CREATE TABLE test_table (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            age INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


def setup_test_table_sync(conn):
    """Set up a test table for sync tests."""
    conn.query_drop("DROP TABLE IF EXISTS test_table")
    conn.query_drop(
        """
        CREATE TABLE test_table (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255),
            age INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


async def cleanup_test_table_async(conn):
    """Clean up test table for async tests."""
    await conn.query_drop("DROP TABLE IF EXISTS test_table")


def cleanup_test_table_sync(conn):
    """Clean up test table for sync tests."""
    conn.query_drop("DROP TABLE IF EXISTS test_table")


@pytest.fixture
async def async_conn_with_table():
    """Provide an async connection with test table set up."""
    from pyro_mysql.async_ import Conn

    conn = await Conn.new(get_test_db_url())

    try:
        await setup_test_table_async(conn)
        yield conn
        await cleanup_test_table_async(conn)
    finally:
        await conn.close()


@pytest.fixture
def sync_conn_with_table():
    """Provide a sync connection with test table set up."""
    from pyro_mysql.sync import Conn

    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)
    yield conn
    cleanup_test_table_sync(conn)


@pytest.fixture(params=["mysql", "diesel", "zero"])
def backend(request):
    """Fixture that provides mysql, diesel, and zero backends for parameterized tests."""
    return request.param


@pytest.fixture(params=["mysql_async", "wtx", "zero"])
def async_backend(request):
    """Fixture that provides mysql_async, wtx, and zero backends for parameterized async tests."""
    return request.param


async def get_async_conn_with_backend(url_or_opts, backend: str):
    """Helper function to create async connection with specified backend."""
    from pyro_mysql.async_ import Conn

    return await Conn.new(url_or_opts, backend=backend)
