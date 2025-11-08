import logging
import os

import pytest
from pyro_mysql import AsyncOpts, AsyncOptsBuilder, SyncOpts, SyncOptsBuilder


def pytest_configure(config):
    logging.getLogger("pyro_mysql").setLevel(logging.DEBUG)


def get_test_db_url() -> str:
    """Get the test database URL from environment or default."""
    return os.environ.get("TEST_DATABASE_URL", "mysql://test:1234@127.0.0.1:3306/test")


def get_async_opts() -> AsyncOpts:
    """Get async connection options for testing."""
    url = get_test_db_url()
    return AsyncOptsBuilder.from_url(url).build()


def get_sync_opts() -> SyncOpts:
    """Get sync connection options for testing."""
    url = get_test_db_url()
    return SyncOptsBuilder.from_url(url).build()


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


@pytest.fixture(params=["mysql", "diesel"])
def backend(request):
    """Fixture that provides both mysql and diesel backends for parameterized tests."""
    return request.param
