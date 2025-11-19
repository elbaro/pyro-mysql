import datetime
from types import TracebackType
from typing import Any, Self, Sequence

from pyro_mysql import IsolationLevel, Params, PyroFuture

class PoolOpts:
    """Pool options for async connections."""

    def __init__(self) -> None:
        """Create new AsyncPoolOpts with default values."""
        ...

    def with_constraints(self, constraints: tuple[int, int]) -> "PoolOpts":
        """
        Set pool constraints as (min_connections, max_connections).

        Args:
            constraints: Tuple of (min, max) connections where min <= max.

        Returns:
            New AsyncPoolOpts with updated constraints.
        """
        ...

    def with_inactive_connection_ttl(self, ttl: datetime.timedelta) -> "PoolOpts":
        """
        Set the TTL for inactive connections.

        Args:
            ttl: Time to live for inactive connections.

        Returns:
            New AsyncPoolOpts with updated TTL.
        """
        ...

    def with_ttl_check_interval(self, interval: datetime.timedelta) -> "PoolOpts":
        """
        Set the interval for TTL checks.

        Args:
            interval: How often to check for expired connections.

        Returns:
            New AsyncPoolOpts with updated interval.
        """
        ...

class Opts:
    """MySQL connection options for async operations."""

    def pool_opts(self, pool_opts: PoolOpts) -> "Opts":
        """Set pool options for the connection."""
        ...

class OptsBuilder:
    """Builder for AsyncOpts with method chaining."""

    def __init__(self) -> None:
        """Create a new AsyncOptsBuilder."""
        ...

    @staticmethod
    def from_opts(opts: Opts) -> "OptsBuilder":
        """Create builder from existing AsyncOpts."""
        ...

    @staticmethod
    def from_url(url: str) -> "OptsBuilder":
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
    def ip_or_hostname(self, hostname: str) -> "OptsBuilder":
        """Set the hostname or IP address."""
        ...

    def tcp_port(self, port: int) -> "OptsBuilder":
        """Set the TCP port."""
        ...

    def socket(self, path: str | None) -> "OptsBuilder":
        """Set the Unix socket path."""
        ...
    # Authentication Options
    def user(self, username: str | None) -> "OptsBuilder":
        """Set the username."""
        ...

    def password(self, password: str | None) -> "OptsBuilder":
        """Set the password."""
        ...

    def db_name(self, database: str | None) -> "OptsBuilder":
        """Set the database name."""
        ...

    def secure_auth(self, enable: bool) -> "OptsBuilder":
        """Enable or disable secure authentication."""
        ...
    # Performance/Timeout Options
    def wait_timeout(self, seconds: int | None) -> "OptsBuilder":
        """Set the wait timeout in seconds."""
        ...

    def stmt_cache_size(self, size: int) -> "OptsBuilder":
        """Set the statement cache size."""
        ...
    # Additional Options
    def tcp_nodelay(self, enable: bool) -> "OptsBuilder":
        """Enable or disable TCP_NODELAY."""
        ...

    def tcp_keepalive(self, keepalive_ms: int | None) -> "OptsBuilder":
        """Set TCP keepalive in milliseconds."""
        ...

    def max_allowed_packet(self, max_allowed_packet: int | None) -> "OptsBuilder":
        """Set the maximum allowed packet size."""
        ...

    def prefer_socket(self, prefer_socket: bool) -> "OptsBuilder":
        """Prefer Unix socket over TCP."""
        ...

    def init(self, commands: list[str]) -> "OptsBuilder":
        """Set initialization commands."""
        ...

    def compression(self, level: int | None) -> "OptsBuilder":
        """Set compression level (0-9)."""
        ...

    def ssl_opts(self, opts: Any | None) -> "OptsBuilder":
        """Set SSL options."""
        ...

    def local_infile_handler(self, handler: Any | None) -> "OptsBuilder":
        """Set local infile handler."""
        ...

    def pool_opts(self, opts: "PoolOpts") -> "OptsBuilder":
        """Set pool options."""
        ...

    def enable_cleartext_plugin(self, enable: bool) -> "OptsBuilder":
        """Enable or disable cleartext plugin."""
        ...

    def client_found_rows(self, enable: bool) -> "OptsBuilder":
        """Enable or disable CLIENT_FOUND_ROWS."""
        ...

    def conn_ttl(self, ttl_seconds: float | None) -> "OptsBuilder":
        """Set connection TTL in seconds."""
        ...

    def setup(self, commands: list[str]) -> "OptsBuilder":
        """Set setup commands."""
        ...

    def build(self) -> Opts:
        """Build the AsyncOpts object."""
        ...

class Transaction:
    """
    Represents a MySQL transaction with async context manager support.
    """

    def __aenter__(self) -> PyroFuture[Self]:
        """Enter the async context manager."""
        ...

    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> PyroFuture[None]:
        """Exit the async context manager. Automatically rolls back if not committed."""
        ...

    def commit(self) -> PyroFuture[None]:
        """Commit the transaction."""
        ...

    def rollback(self) -> PyroFuture[None]:
        """Rollback the transaction."""
        ...

    def affected_rows(self) -> PyroFuture[int]:
        """Close a prepared statement (not yet implemented)."""
        ...

    def ping(self) -> PyroFuture[None]:
        """Ping the server to check connection."""
        ...

    def query(
        self, query: str, *, as_dict: bool = False
    ) -> PyroFuture[list[tuple[Any, ...]] | list[dict[str, Any]]]:
        """
        Execute a query using text protocol and return all rows.

        Args:
            query: SQL query string.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            List of tuples (default) or dictionaries.
        """
        ...

    def query_first(
        self, query: str, *, as_dict: bool = False
    ) -> PyroFuture[tuple[Any, ...] | dict[str, Any] | None]:
        """
        Execute a query using text protocol and return the first row.

        Args:
            query: SQL query string.
            as_dict: If True, return row as dictionary. If False (default), return row as tuple.

        Returns:
            First row as tuple (default) or dictionary, or None if no results.
        """
        ...

    def query_drop(self, query: str) -> PyroFuture[None]:
        """
        Execute a query using text protocol and discard the results.

        Args:
            query: SQL query string.
        """
        ...

    def exec(
        self, query: str, params: Params = None, *, as_dict: bool = False
    ) -> PyroFuture[list[tuple[Any, ...]] | list[dict[str, Any]]]:
        """
        Execute a query and return all rows.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            List of tuples (default) or dictionaries.
        """
        ...

    def exec_first(
        self, query: str, params: Params = None, *, as_dict: bool = False
    ) -> PyroFuture[tuple[Any, ...] | dict[str, Any] | None]:
        """
        Execute a query and return the first row.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.
            as_dict: If True, return row as dictionary. If False (default), return row as tuple.

        Returns:
            First row as tuple (default) or dictionary, or None if no results.
        """
        ...

    def exec_drop(self, query: str, params: Params = None) -> PyroFuture[None]:
        """
        Execute a query and discard the results.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.
        """
        ...

    def exec_batch(self, query: str, params: list[Params] = []) -> PyroFuture[None]:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query string with '?' placeholders.
            params: List of parameter sets.
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
    def new(url_or_opts: str | Opts, backend: str = "mysql") -> PyroFuture["Conn"]:
        """
        Create a new connection.

        Args:
            url_or_opts: MySQL connection URL (e.g., 'mysql://user:password@host:port/database')
                or AsyncOpts object with connection configuration.

        Returns:
            New Conn instance.
        """
        ...

    def start_transaction(
        self,
        consistent_snapshot: bool = False,
        isolation_level: IsolationLevel | None = None,
        readonly: bool | None = None,
    ) -> Transaction:
        """
        Start a new transaction.

        Args:
            consistent_snapshot: Whether to use consistent snapshot.
            isolation_level: Transaction isolation level.
            readonly: Whether the transaction is read-only.

        Returns:
            New Transaction instance.
        """
        ...

    async def id(self) -> int: ...
    async def affected_rows(self) -> int: ...
    async def last_insert_id(self) -> int | None: ...
    def ping(self) -> PyroFuture[None]:
        """Ping the server to check connection."""
        ...

    def query(
        self, query: str, *, as_dict: bool = False
    ) -> PyroFuture[list[tuple[Any, ...]] | list[dict[str, Any]]]:
        """
        Execute a query using text protocol and return all rows.

        Args:
            query: SQL query string.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            List of tuples (default) or dictionaries.
        """
        ...

    def query_first(
        self, query: str, *, as_dict: bool = False
    ) -> PyroFuture[tuple[Any, ...] | dict[str, Any] | None]:
        """
        Execute a query using text protocol and return the first row.

        Args:
            query: SQL query string.
            as_dict: If True, return row as dictionary. If False (default), return row as tuple.

        Returns:
            First row as tuple (default) or dictionary, or None if no results.
        """
        ...

    def query_drop(self, query: str) -> PyroFuture[None]:
        """
        Execute a query using text protocol and discard the results.

        Args:
            query: SQL query string.
        """
        ...

    def exec(
        self, query: str, params: Params = None, *, as_dict: bool = False
    ) -> PyroFuture[list[tuple[Any, ...]] | list[dict[str, Any]]]:
        """
        Execute a query and return all rows.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            List of tuples (default) or dictionaries.
        """
        ...

    def exec_first(
        self, query: str, params: Params = None, *, as_dict: bool = False
    ) -> PyroFuture[tuple[Any, ...] | dict[str, Any] | None]:
        """
        Execute a query and return the first row.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.
            as_dict: If True, return row as dictionary. If False (default), return row as tuple.

        Returns:
            First row as tuple (default) or dictionary, or None if no results.
        """
        ...

    def exec_drop(self, query: str, params: Params = None) -> PyroFuture[None]:
        """
        Execute a query and discard the results.

        Args:
            query: SQL query string with '?' placeholders.
            params: Query parameters.
        """
        ...

    def exec_batch(self, query: str, params: Sequence[Params] = []) -> PyroFuture[None]:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query string with '?' placeholders.
            params: List of parameter sets.
        """
        ...

    async def close(self) -> None:
        """
        Disconnect from the MySQL server.

        This closes the connection and makes it unusable for further operations.
        """
        ...

    async def reset(self) -> None:
        """
        Reset the connection state.

        This resets the connection to a clean state without closing it.
        """
        ...

    def server_version(self) -> PyroFuture[tuple[int, int, int]]: ...

class Pool:
    """
    MySQL connection pool.
    """

    def __init__(self, opts_or_url: str | Opts) -> None:
        """
        Create a new connection pool.
        Note: new() won't assert server availability.

        Args:
            opts_or_url: MySQL connection URL (e.g., 'mysql://root:password@127.0.0.1:3307/mysql')
                or AsyncOpts object with connection configuration.
        """
        ...

    async def get(self) -> "Conn":
        """
        Get a connection from the pool.

        Returns:
            Connection from the pool.
        """
        ...

    async def close(self) -> None:
        """
        Disconnect and close all connections in the pool.
        """
        ...
