"""pyro_mysql - High-performance MySQL driver for Python.

```py
import asyncio
import pyro_mysql as mysql

mysql.init(worker_threads=1)

async def example_select():
    conn = await mysql.Conn.new("mysql://localhost@127.0.0.1:3306/test")
    rows = await conn.exec("SELECT * from mydb.mytable")
    print(row[-1].to_dict())


async def example_transaction():
    conn = await mysql.Conn.new("mysql://localhost@127.0.0.1:3306/test")

    async with conn.start_transaction() as tx:
        await tx.exec_drop(
            "INSERT INTO test.asyncmy(`decimal`, `date`, `datetime`, `float`, `string`, `tinyint`) VALUES (?,?,?,?,?,?)",
            (
                1,
                "2021-01-01",
                "2020-07-16 22:49:54",
                1,
                "asyncmy",
                1,
            ),
        )
        await tx.commit()

    await len(conn.exec('SELECT * FROM mydb.mytable')) == 100

# The connection pool is not tied to a single event loop.
# You can reuse the pool between event loops.
asyncio.run(example_pool())
asyncio.run(example_select())
asyncio.run(example_transaction())
...
```

"""

import datetime
import decimal
import time
from types import TracebackType
from typing import Any, Self

__all__ = [
    "init",
    "Row",
    "TxOpts",
    "IsolationLevel",
    "CapabilityFlags",
    "async_",
    "sync",
    "AsyncConn",
    "AsyncPool",
    "AsyncTransaction",
    "AsyncOpts",
    "AsyncOptsBuilder",
    "AsyncPoolOpts",
    "SyncConn",
    "SyncPool",
    "SyncPooledConn",
    "SyncTransaction",
    "SyncOpts",
    "SyncOptsBuilder",
    "SyncPoolOpts",
]

JsonEncodable = (
    dict[str, "JsonEncodable"] | list["JsonEncodable"] | str | int | float | bool | None
)
type Value = None | bool | int | float | str | bytes | bytearray | tuple[
    JsonEncodable, ...
] | list[JsonEncodable] | set[JsonEncodable] | frozenset[JsonEncodable] | dict[
    str, JsonEncodable
] | datetime.datetime | datetime.date | datetime.time | datetime.timedelta | time.struct_time | decimal.Decimal
type Params = None | tuple[Value, ...] | list[Value] | dict[str, Value]

def init(worker_threads: int | None = 1, thread_name: str | None = None) -> None:
    """
    Initialize the Tokio runtime for async operations.
    This function can be called multiple times until Any async operation is called.

    Args:
        worker_threads: Number of worker threads for the Tokio runtime. If None, set to the number of CPUs.
        thread_name: Name prefix for worker threads.
    """
    ...

class IsolationLevel:
    """Transaction isolation level enum."""

    ReadUncommitted: "IsolationLevel"
    ReadCommitted: "IsolationLevel"
    RepeatableRead: "IsolationLevel"
    Serializable: "IsolationLevel"

    def as_str(self) -> str:
        """Return the isolation level as a string."""
        ...

class TxOpts:
    """Transaction options."""

    def __init__(
        self,
        consistent_snapshot: bool = False,
        isolation_level: IsolationLevel | None = None,
        readonly: bool = False,
    ) -> None:
        """
        Create transaction options.

        Args:
            consistent_snapshot: Whether to use consistent snapshot.
            isolation_level: Transaction isolation level.
            readonly: Whether the transaction is read-only.
        """
        ...

class Row:
    """
    A row returned from a MySQL query.
    to_tuple() / to_dict() copies the data, and should not be called many times.
    """

    def to_tuple(self) -> tuple[Value, ...]:
        """Convert the row to a Python list."""
        ...

    def to_dict(self) -> dict[str, Value]:
        f"""
        Convert the row to a Python dictionary with column names as keys.
        If there are multiple columns with the same name, a later column wins.

            row = await conn.exec_first("SELECT 1, 2, 2 FROM some_table")
            assert row.as_dict() == {"1": 1, "2": 2}
        """
        ...

# ============================================================================
# Async API
# ============================================================================

class AsyncOpts:
    """MySQL connection options for async operations."""

    def pool_opts(self, pool_opts: "AsyncPoolOpts") -> "AsyncOpts":
        """Set pool options for the connection."""
        ...

class AsyncOptsBuilder:
    """Builder for AsyncOpts with method chaining."""

    def __init__(self) -> None:
        """Create a new AsyncOptsBuilder."""
        ...

    @staticmethod
    def from_opts(opts: AsyncOpts) -> "AsyncOptsBuilder":
        """Create builder from existing AsyncOpts."""
        ...

    @staticmethod
    def from_url(url: str) -> "AsyncOptsBuilder":
        """Create builder from a MySQL connection URL.

        URL format: mysql://[user[:password]@]host[:port][/database][?param1=value1&...]

        Args:
            url: MySQL connection URL string

        Returns:
            AsyncOptsBuilder configured from the URL

        Raises:
            ValueError: If the URL is invalid or cannot be parsed
        """
        ...
    # Network/Connection Options
    def ip_or_hostname(self, hostname: str) -> "AsyncOptsBuilder":
        """Set the hostname or IP address."""
        ...

    def tcp_port(self, port: int) -> "AsyncOptsBuilder":
        """Set the TCP port."""
        ...

    def socket(self, path: str | None) -> "AsyncOptsBuilder":
        """Set the Unix socket path."""
        ...
    # Authentication Options
    def user(self, username: str | None) -> "AsyncOptsBuilder":
        """Set the username."""
        ...

    def password(self, password: str | None) -> "AsyncOptsBuilder":
        """Set the password."""
        ...

    def db_name(self, database: str | None) -> "AsyncOptsBuilder":
        """Set the database name."""
        ...

    def secure_auth(self, enable: bool) -> "AsyncOptsBuilder":
        """Enable or disable secure authentication."""
        ...
    # Performance/Timeout Options
    def wait_timeout(self, seconds: int | None) -> "AsyncOptsBuilder":
        """Set the wait timeout in seconds."""
        ...

    def stmt_cache_size(self, size: int) -> "AsyncOptsBuilder":
        """Set the statement cache size."""
        ...
    # Additional Options
    def tcp_nodelay(self, enable: bool) -> "AsyncOptsBuilder":
        """Enable or disable TCP_NODELAY."""
        ...

    def tcp_keepalive(self, keepalive_ms: int | None) -> "AsyncOptsBuilder":
        """Set TCP keepalive in milliseconds."""
        ...

    def max_allowed_packet(self, max_allowed_packet: int | None) -> "AsyncOptsBuilder":
        """Set the maximum allowed packet size."""
        ...

    def prefer_socket(self, prefer_socket: bool) -> "AsyncOptsBuilder":
        """Prefer Unix socket over TCP."""
        ...

    def init(self, commands: list[str]) -> "AsyncOptsBuilder":
        """Set initialization commands."""
        ...

    def compression(self, level: int | None) -> "AsyncOptsBuilder":
        """Set compression level (0-9)."""
        ...

    def ssl_opts(self, opts: Any | None) -> "AsyncOptsBuilder":
        """Set SSL options."""
        ...

    def local_infile_handler(self, handler: Any | None) -> "AsyncOptsBuilder":
        """Set local infile handler."""
        ...

    def pool_opts(self, opts: "AsyncPoolOpts") -> "AsyncOptsBuilder":
        """Set pool options."""
        ...

    def enable_cleartext_plugin(self, enable: bool) -> "AsyncOptsBuilder":
        """Enable or disable cleartext plugin."""
        ...

    def client_found_rows(self, enable: bool) -> "AsyncOptsBuilder":
        """Enable or disable CLIENT_FOUND_ROWS."""
        ...

    def conn_ttl(self, ttl_seconds: float | None) -> "AsyncOptsBuilder":
        """Set connection TTL in seconds."""
        ...

    def setup(self, commands: list[str]) -> "AsyncOptsBuilder":
        """Set setup commands."""
        ...

    def build(self) -> AsyncOpts:
        """Build the AsyncOpts object."""
        ...

class AsyncPoolOpts:
    """Pool options for async connections."""

    def __init__(self) -> None:
        """Create new AsyncPoolOpts with default values."""
        ...

    def with_constraints(self, constraints: tuple[int, int]) -> "AsyncPoolOpts":
        """
        Set pool constraints as (min_connections, max_connections).

        Args:
            constraints: Tuple of (min, max) connections where min <= max.

        Returns:
            New AsyncPoolOpts with updated constraints.
        """
        ...

    def with_inactive_connection_ttl(self, ttl: datetime.timedelta) -> "AsyncPoolOpts":
        """
        Set the TTL for inactive connections.

        Args:
            ttl: Time to live for inactive connections.

        Returns:
            New AsyncPoolOpts with updated TTL.
        """
        ...

    def with_ttl_check_interval(self, interval: datetime.timedelta) -> "AsyncPoolOpts":
        """
        Set the interval for TTL checks.

        Args:
            interval: How often to check for expired connections.

        Returns:
            New AsyncPoolOpts with updated interval.
        """
        ...

class async_:
    """Async MySQL driver components."""

    class Transaction:
        """
        Represents a MySQL transaction with async context manager support.
        """

        async def __aenter__(self) -> Self:
            """Enter the async context manager."""
            ...

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            """Exit the async context manager. Automatically rolls back if not committed."""
            ...

        async def commit(self) -> None:
            """Commit the transaction."""
            ...

        async def rollback(self) -> None:
            """Rollback the transaction."""
            ...

        async def close_prepared_statement(self, stmt: str) -> None:
            """Close a prepared statement (not yet implemented)."""
            ...

        async def ping(self) -> None:
            """Ping the server to check connection."""
            ...

        async def exec(self, query: str, params: Params = None) -> list[Row]:
            """
            Execute a query and return all rows.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.

            Returns:
                List of Row objects.
            """
            ...

        async def exec_first(self, query: str, params: Params = None) -> Row | None:
            """
            Execute a query and return the first row.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.

            Returns:
                First Row or None if no results.
            """
            ...

        async def exec_drop(self, query: str, params: Params = None) -> None:
            """
            Execute a query and discard the results.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.
            """
            ...

        async def exec_batch(self, query: str, params: list[Params] = []) -> None:
            """
            Execute a query multiple times with different parameters.

            Args:
                query: SQL query string with '?' placeholders.
                params: List of parameter sets.
            """
            ...

        async def query(self, query: str) -> list[Row]:
            """
            Execute a query using text protocol and return all rows.

            Args:
                query: SQL query string.

            Returns:
                List of Row objects.
            """
            ...

        async def query_first(self, query: str) -> Row | None:
            """
            Execute a query using text protocol and return the first row.

            Args:
                query: SQL query string.

            Returns:
                First Row or None if no results.
            """
            ...

        async def query_drop(self, query: str) -> None:
            """
            Execute a query using text protocol and discard the results.

            Args:
                query: SQL query string.
            """
            ...

    class Conn:
        """
        MySQL connection.

        The API is thread-safe. The underlying implementation is protected by RwLock.
        """

        def __init__(self) -> None:
            """
            Direct instantiation is not allowed.
            Use Conn.new() instead.
            """
            ...

        @staticmethod
        async def new(url_or_opts: str | AsyncOpts) -> "async_.Conn":
            """
            Create a new connection.

            Args:
                url_or_opts: MySQL connection URL (e.g., 'mysql://user:password@host:port/database')
                    or AsyncOpts object with connection configuration.

            Returns:
                New Conn instance.
            """
            ...

        def start_transaction(self, opts: TxOpts = ...) -> "async_.Transaction":
            """
            Start a new transaction.

            Args:
                opts: Transaction options.

            Returns:
                New Transaction instance.
            """
            ...

        async def close_prepared_statement(self, stmt: str) -> None:
            """Close a prepared statement (not yet implemented)."""
            ...

        async def ping(self) -> None:
            """Ping the server to check connection."""
            ...

        async def exec(self, query: str, params: Params = None) -> list[Row]:
            """
            Execute a query and return all rows.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.

            Returns:
                List of Row objects.
            """
            ...

        async def exec_first(self, query: str, params: Params = None) -> Row | None:
            """
            Execute a query and return the first row.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.

            Returns:
                First Row or None if no results.
            """
            ...

        async def exec_drop(self, query: str, params: Params = None) -> None:
            """
            Execute a query and discard the results.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.
            """
            ...

        async def exec_batch(self, query: str, params: list[Params] = []) -> None:
            """
            Execute a query multiple times with different parameters.

            Args:
                query: SQL query string with '?' placeholders.
                params: List of parameter sets.
            """
            ...

        async def query(self, query: str) -> list[Row]:
            """
            Execute a query using text protocol and return all rows.

            Args:
                query: SQL query string.

            Returns:
                List of Row objects.
            """
            ...

        async def query_first(self, query: str) -> Row | None:
            """
            Execute a query using text protocol and return the first row.

            Args:
                query: SQL query string.

            Returns:
                First Row or None if no results.
            """
            ...

        async def query_drop(self, query: str) -> None:
            """
            Execute a query using text protocol and discard the results.

            Args:
                query: SQL query string.
            """
            ...

        async def disconnect(self) -> None:
            """
            Disconnect from the MySQL server.
            
            This closes the connection and makes it unusable for further operations.
            """
            ...

    class Pool:
        """
        MySQL connection pool.
        """

        def __init__(self, opts_or_url: str | AsyncOpts) -> None:
            """
            Create a new connection pool.
            Note: new() won't assert server availability.

            Args:
                opts_or_url: MySQL connection URL (e.g., 'mysql://root:password@127.0.0.1:3307/mysql')
                    or AsyncOpts object with connection configuration.
            """
            ...

        async def get_conn(self) -> "async_.Conn":
            """
            Get a connection from the pool.

            Returns:
                Connection from the pool.
            """
            ...

        async def acquire(self) -> "async_.Conn":
            """
            Acquire a connection from the pool (alias for get_conn).

            Returns:
                Connection from the pool.
            """
            ...

        async def disconnect(self) -> None:
            """
            Disconnect and close all connections in the pool.
            """
            ...

# ============================================================================
# Sync API
# ============================================================================

class SyncOpts:
    """MySQL connection options for sync operations."""

    def pool_opts(self, pool_opts: "SyncPoolOpts") -> "SyncOpts":
        """Set pool options for the connection."""
        ...

class SyncOptsBuilder:
    """Builder for SyncOpts with method chaining."""

    def __init__(self) -> None:
        """Create a new SyncOptsBuilder."""
        ...

    @staticmethod
    def from_opts(opts: SyncOpts) -> "SyncOptsBuilder":
        """Create builder from existing SyncOpts."""
        ...

    @staticmethod
    def from_url(url: str) -> "SyncOptsBuilder":
        """Create builder from a MySQL connection URL.

        URL format: mysql://[user[:password]@]host[:port][/database][?param1=value1&...]

        Args:
            url: MySQL connection URL string

        Returns:
            SyncOptsBuilder configured from the URL

        Raises:
            ValueError: If the URL is invalid or cannot be parsed
        """
        ...

    def from_hash_map(self, params: dict[str, str]) -> "SyncOptsBuilder":
        """Initialize from a dictionary of parameters."""
        ...
    # Network/Connection Options
    def ip_or_hostname(self, hostname: str | None) -> "SyncOptsBuilder":
        """Set the hostname or IP address."""
        ...

    def tcp_port(self, port: int) -> "SyncOptsBuilder":
        """Set the TCP port."""
        ...

    def socket(self, path: str | None) -> "SyncOptsBuilder":
        """Set the Unix socket path."""
        ...

    def bind_address(self, address: str | None) -> "SyncOptsBuilder":
        """Set the bind address for outgoing connections."""
        ...
    # Authentication Options
    def user(self, username: str | None) -> "SyncOptsBuilder":
        """Set the username."""
        ...

    def pass_(self, password: str | None) -> "SyncOptsBuilder":
        """Set the password."""
        ...

    def db_name(self, database: str | None) -> "SyncOptsBuilder":
        """Set the database name."""
        ...

    def secure_auth(self, enable: bool) -> "SyncOptsBuilder":
        """Enable or disable secure authentication."""
        ...
    # Performance/Timeout Options
    def read_timeout(self, seconds: float | None) -> "SyncOptsBuilder":
        """Set the read timeout in seconds."""
        ...

    def write_timeout(self, seconds: float | None) -> "SyncOptsBuilder":
        """Set the write timeout in seconds."""
        ...

    def tcp_connect_timeout(self, seconds: float | None) -> "SyncOptsBuilder":
        """Set the TCP connection timeout in seconds."""
        ...

    def stmt_cache_size(self, size: int) -> "SyncOptsBuilder":
        """Set the statement cache size."""
        ...
    # Additional Options
    def tcp_nodelay(self, enable: bool) -> "SyncOptsBuilder":
        """Enable or disable TCP_NODELAY."""
        ...

    def tcp_keepalive_time_ms(self, time_ms: int | None) -> "SyncOptsBuilder":
        """Set TCP keepalive time in milliseconds."""
        ...

    def tcp_keepalive_probe_interval_secs(
        self, interval_secs: int | None
    ) -> "SyncOptsBuilder":
        """Set TCP keepalive probe interval in seconds."""
        ...

    def tcp_keepalive_probe_count(self, count: int | None) -> "SyncOptsBuilder":
        """Set TCP keepalive probe count."""
        ...

    def tcp_user_timeout_ms(self, timeout_ms: int | None) -> "SyncOptsBuilder":
        """Set TCP user timeout in milliseconds."""
        ...

    def max_allowed_packet(self, max_allowed_packet: int | None) -> "SyncOptsBuilder":
        """Set the maximum allowed packet size."""
        ...

    def prefer_socket(self, prefer_socket: bool) -> "SyncOptsBuilder":
        """Prefer Unix socket over TCP."""
        ...

    def init(self, commands: list[str]) -> "SyncOptsBuilder":
        """Set initialization commands."""
        ...

    def connect_attrs(self, attrs: dict[str, str] | None) -> "SyncOptsBuilder":
        """Set connection attributes."""
        ...

    def compress(self, level: int | None) -> "SyncOptsBuilder":
        """Set compression level (0-9)."""
        ...

    def ssl_opts(self, opts: Any | None) -> "SyncOptsBuilder":
        """Set SSL options."""
        ...

    def local_infile_handler(self, handler: Any | None) -> "SyncOptsBuilder":
        """Set local infile handler."""
        ...

    def pool_opts(self, opts: "SyncPoolOpts") -> "SyncOptsBuilder":
        """Set pool options."""
        ...

    def additional_capabilities(self, capabilities: int) -> "SyncOptsBuilder":
        """Set additional capability flags."""
        ...

    def enable_cleartext_plugin(self, enable: bool) -> "SyncOptsBuilder":
        """Enable or disable cleartext plugin."""
        ...

    def build(self) -> SyncOpts:
        """Build the SyncOpts object."""
        ...

class SyncPoolOpts:
    """Pool options for sync connections."""

    def __init__(self) -> None:
        """Create new SyncPoolOpts with default values."""
        ...

    def with_constraints(self, constraints: tuple[int, int]) -> "SyncPoolOpts":
        """
        Set pool constraints as (min_connections, max_connections).

        Args:
            constraints: Tuple of (min, max) connections where min <= max.

        Returns:
            New SyncPoolOpts with updated constraints.
        """
        ...

class SyncPool:
    """Synchronous MySQL connection pool."""

    def __init__(self, opts_or_url: str | SyncOpts) -> None:
        """
        Create a new connection pool.
        Note: new() won't assert server availability.

        Args:
            opts_or_url: MySQL connection URL (e.g., 'mysql://root:password@127.0.0.1:3307/mysql')
                or SyncOpts object with connection configuration.
        """
        ...

    def get_conn(self) -> "SyncPooledConn":
        """
        Get a connection from the pool.

        Returns:
            Connection from the pool.
        """
        ...

    def acquire(self) -> "SyncPooledConn":
        """
        Acquire a connection from the pool (alias for get_conn).

        Returns:
            Connection from the pool.
        """
        ...

    def disconnect(self) -> None:
        """
        Disconnect and close all connections in the pool.
        """
        ...

class SyncPooledConn:
    """
    Synchronous MySQL pooled connection.

    This represents a connection obtained from a SyncPool.
    It has the same interface as SyncConn but wraps a mysql::PooledConn.
    """

    def __init__(self) -> None:
        """
        Direct instantiation is not allowed.
        Use SyncPool.get_conn() or SyncPool.acquire() instead.
        """
        ...

    def run_transaction(
        self,
        callable: Any,
        consistent_snapshot: bool = False,
        isolation_level: IsolationLevel | None = None,
        readonly: bool | None = None,
    ) -> Any:
        """
        Run a transaction with a callable.

        Args:
            callable: A callable that will receive the transaction object.
            consistent_snapshot: Whether to use consistent snapshot.
            isolation_level: Transaction isolation level.
            readonly: Whether the transaction is read-only.

        Returns:
            The return value of the callable.
        """
        ...

    def affected_rows(self) -> int:
        """Get the number of affected rows from the last operation."""
        ...

    def ping(self) -> None:
        """Ping the server to check connection."""
        ...

    def exec(self, query: str, params: Params = None) -> list[Row]:
        """
        Execute a query and return all rows.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.

        Returns:
            List of Row objects.
        """
        ...

    def exec_first(self, query: str, params: Params = None) -> Row | None:
        """
        Execute a query and return the first row.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.

        Returns:
            First Row or None if no results.
        """
        ...

    def exec_drop(self, query: str, params: Params = None) -> None:
        """
        Execute a query and discard the results.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.
        """
        ...

    def exec_batch(self, query: str, params_list: list[Params] = []) -> None:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query string with '?' placeholders.
            params_list: List of parameter sets.
        """
        ...

    def query(self, query: str) -> list[Row]:
        """
        Execute a query using text protocol and return all rows.

        Args:
            query: SQL query string.

        Returns:
            List of Row objects.
        """
        ...

    def query_first(self, query: str) -> Row | None:
        """
        Execute a query using text protocol and return the first row.

        Args:
            query: SQL query string.

        Returns:
            First Row or None if no results.
        """
        ...

    def query_drop(self, query: str) -> None:
        """
        Execute a query using text protocol and discard the results.

        Args:
            query: SQL query string.
        """
        ...

    def query_iter(self, query: str) -> Any:
        """
        Execute a query using text protocol and return an iterator over result sets.

        Args:
            query: SQL query string.

        Returns:
            Iterator over result sets.
        """
        ...

    def exec_iter(self, query: str, params: Params = None) -> "sync.ResultSetIterator":
        """
        Execute a query using binary protocol and return an iterator over the results.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.

        Returns:
            ResultSetIterator object for iterating over rows.
        """
        ...

    def close(self) -> None:
        """Close the connection."""
        ...

class sync:
    """Synchronous MySQL driver components."""

    class ResultSetIterator:
        """Iterator over MySQL result sets."""

        def __iter__(self) -> "sync.ResultSetIterator":
            """Return iterator."""
            ...

        def __next__(self) -> Row:
            """Get next row."""
            ...

    class Transaction:
        """
        Represents a synchronous MySQL transaction.
        """

        def commit(self) -> None:
            """Commit the transaction."""
            ...

        def rollback(self) -> None:
            """Rollback the transaction."""
            ...

        def affected_rows(self) -> int:
            """Get the number of affected rows from the last operation."""
            ...

        def exec(self, query: str, params: Params = None) -> list[Row]:
            """
            Execute a query and return all rows.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.

            Returns:
                List of Row objects.
            """
            ...

        def exec_first(self, query: str, params: Params = None) -> Row | None:
            """
            Execute a query and return the first row.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.

            Returns:
                First Row or None if no results.
            """
            ...

        def exec_drop(self, query: str, params: Params = None) -> None:
            """
            Execute a query and discard the results.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.
            """
            ...

        def exec_batch(self, query: str, params_list: list[Params] = []) -> None:
            """
            Execute a query multiple times with different parameters.

            Args:
                query: SQL query string with '?' placeholders.
                params_list: List of parameter sets.
            """
            ...

        def query(self, query: str) -> list[Row]:
            """
            Execute a query using text protocol and return all rows.

            Args:
                query: SQL query string.

            Returns:
                List of Row objects.
            """
            ...

        def query_first(self, query: str) -> Row | None:
            """
            Execute a query using text protocol and return the first row.

            Args:
                query: SQL query string.

            Returns:
                First Row or None if no results.
            """
            ...

        def query_drop(self, query: str) -> None:
            """
            Execute a query using text protocol and discard the results.

            Args:
                query: SQL query string.
            """
            ...

        def query_iter(self, query: str) -> "sync.ResultSetIterator":
            """
            Execute a query using text protocol and return an iterator over the results.

            Args:
                query: SQL query string.

            Returns:
                ResultSetIterator object for iterating over rows.
            """
            ...

        def exec_iter(
            self, query: str, params: Params = None
        ) -> "sync.ResultSetIterator":
            """
            Execute a query using binary protocol and return an iterator over the results.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.

            Returns:
                ResultSetIterator object for iterating over rows.
            """
            ...

    class Conn:
        """
        Synchronous MySQL connection.
        """

        def __init__(self, url_or_opts: str | SyncOpts) -> None:
            """
            Create a new synchronous connection.

            Args:
                url_or_opts: MySQL connection URL (e.g., 'mysql://user:password@host:port/database') or SyncOpts object.
            """
            ...

        def run_transaction(
            self,
            callable: Any,
            consistent_snapshot: bool = False,
            isolation_level: IsolationLevel | None = None,
            readonly: bool | None = None,
        ) -> Any:
            """
            Run a transaction with a callable.

            Args:
                callable: A callable that will receive the transaction object.
                consistent_snapshot: Whether to use consistent snapshot.
                isolation_level: Transaction isolation level.
                readonly: Whether the transaction is read-only.

            Returns:
                The return value of the callable.
            """
            ...

        def affected_rows(self) -> int:
            """Get the number of affected rows from the last operation."""
            ...

        def ping(self) -> None:
            """Ping the server to check connection."""
            ...

        def exec(self, query: str, params: Params = None) -> list[Row]:
            """
            Execute a query and return all rows.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.

            Returns:
                List of Row objects.
            """
            ...

        def exec_first(self, query: str, params: Params = None) -> Row | None:
            """
            Execute a query and return the first row.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.

            Returns:
                First Row or None if no results.
            """
            ...

        def exec_drop(self, query: str, params: Params = None) -> None:
            """
            Execute a query and discard the results.

            Args:
                query: SQL query string with '?' placeholders.
                params: Query parameters.
            """
            ...

        def exec_batch(self, query: str, params_list: list[Params] = []) -> None:
            """
            Execute a query multiple times with different parameters.

            Args:
                query: SQL query string with '?' placeholders.
                params_list: List of parameter sets.
            """
            ...

        def query(self, query: str) -> list[Row]:
            """
            Execute a query using text protocol and return all rows.

            Args:
                query: SQL query string.

            Returns:
                List of Row objects.
            """
            ...

        def query_first(self, query: str) -> Row | None:
            """
            Execute a query using text protocol and return the first row.

            Args:
                query: SQL query string.

            Returns:
                First Row or None if no results.
            """
            ...

        def query_drop(self, query: str) -> None:
            """
            Execute a query using text protocol and discard the results.

            Args:
                query: SQL query string.
            """
            ...

        def query_iter(self, query: str) -> Any:
            """
            Execute a query using text protocol and return an iterator over result sets.

            Args:
                query: SQL query string.

            Returns:
                Iterator over result sets.
            """
            ...

        def close(self) -> None:
            """Close the connection."""
            ...

        def disconnect(self) -> None:
            """
            Disconnect from the MySQL server.
            
            This closes the connection and makes it unusable for further operations.
            """
            ...

# # Re-export async classes with prefix
# AsyncConn = async_.Conn
# AsyncPool = async_.Pool
# AsyncTransaction = async_.Transaction

# # Re-export sync classes with prefix
# SyncConn = sync.Conn
# SyncTransaction = sync.Transaction
