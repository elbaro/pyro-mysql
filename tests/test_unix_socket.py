"""Tests for Unix socket connections (both sync and async)."""

import os

import pytest
from pyro_mysql import Opts
from pyro_mysql.async_ import Conn as AsyncConn
from pyro_mysql.sync import Conn as SyncConn


def get_socket_path() -> str | None:
    """Get Unix socket path from environment or common default locations."""
    if path := os.environ.get("TEST_MYSQL_SOCKET"):
        return path

    common_paths = [
        "/var/run/mysqld/mysqld.sock",
        "/tmp/mysql.sock",
        "/var/lib/mysql/mysql.sock",
    ]
    for path in common_paths:
        if os.path.exists(path):
            return path
    return None


def get_socket_opts() -> Opts:
    """Create Opts configured for Unix socket connection."""
    socket_path = get_socket_path()
    if not socket_path:
        pytest.skip("No Unix socket found")

    return Opts().socket(socket_path).user("test").password("1234").db("test")


@pytest.fixture
def socket_path():
    """Fixture that provides socket path or skips test."""
    path = get_socket_path()
    if not path:
        pytest.skip("No Unix socket found")
    return path


class TestSyncUnixSocket:
    """Tests for synchronous Unix socket connections."""

    def test_basic_connection(self, socket_path, backend):
        """Test basic sync connection via Unix socket."""
        opts = Opts().socket(socket_path).user("test").password("1234").db("test")
        conn = SyncConn(opts, backend=backend)

        result = conn.query_first("SELECT 1")
        assert result
        assert result[0] == 1

        conn.close()

    def test_query_execution(self, socket_path, backend):
        """Test query execution via Unix socket."""
        opts = Opts().socket(socket_path).user("test").password("1234").db("test")
        conn = SyncConn(opts, backend=backend)

        result = conn.query_first("SELECT DATABASE()")
        assert result
        assert result[0] == "test"

        conn.close()

    def test_ping(self, socket_path, backend):
        """Test ping via Unix socket."""
        opts = Opts().socket(socket_path).user("test").password("1234").db("test")
        conn = SyncConn(opts, backend=backend)

        conn.ping()

        conn.close()

    def test_prepared_statement(self, socket_path, backend):
        """Test prepared statement via Unix socket."""
        opts = Opts().socket(socket_path).user("test").password("1234").db("test")
        conn = SyncConn(opts, backend=backend)

        result = conn.exec_first("SELECT ? + ?", (1, 2))
        assert result
        assert result[0] == 3

        conn.close()


class TestAsyncUnixSocket:
    """Tests for asynchronous Unix socket connections."""

    @pytest.mark.asyncio
    async def test_basic_connection(self, socket_path, async_backend):
        """Test basic async connection via Unix socket."""
        opts = Opts().socket(socket_path).user("test").password("1234").db("test")
        conn = await AsyncConn.new(opts, backend=async_backend)

        result = await conn.query_first("SELECT 1")
        assert result
        assert result[0] == 1

        await conn.close()

    @pytest.mark.asyncio
    async def test_query_execution(self, socket_path, async_backend):
        """Test query execution via Unix socket."""
        opts = Opts().socket(socket_path).user("test").password("1234").db("test")
        conn = await AsyncConn.new(opts, backend=async_backend)

        result = await conn.query_first("SELECT DATABASE()")
        assert result
        assert result[0] == "test"

        await conn.close()

    @pytest.mark.asyncio
    async def test_ping(self, socket_path, async_backend):
        """Test ping via Unix socket."""
        opts = Opts().socket(socket_path).user("test").password("1234").db("test")
        conn = await AsyncConn.new(opts, backend=async_backend)

        await conn.ping()

        await conn.close()

    @pytest.mark.asyncio
    async def test_prepared_statement(self, socket_path, async_backend):
        """Test prepared statement via Unix socket."""
        opts = Opts().socket(socket_path).user("test").password("1234").db("test")
        conn = await AsyncConn.new(opts, backend=async_backend)

        result = await conn.exec_first("SELECT ? + ?", (1, 2))
        assert result
        assert result[0] == 3

        await conn.close()
