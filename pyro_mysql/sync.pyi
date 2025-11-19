"""Synchronous MySQL driver components."""

from types import TracebackType
from typing import Any, Self, Sequence

from pyro_mysql import IsolationLevel, Params

class RowIterator:
    def __iter__(self) -> "RowIterator": ...
    def __next__(self) -> tuple[Any, ...]: ...

class ResultSetIterator:
    """Iterator over MySQL result sets."""

    def __iter__(self) -> "ResultSetIterator":
        """Return iterator."""
        ...

    def __next__(self) -> RowIterator: ...

class Transaction:
    """
    Represents a synchronous MySQL transaction.
    """

    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    def commit(self) -> None:
        """Commit the transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the transaction."""
        ...

    def affected_rows(self) -> int:
        """Get the number of affected rows from the last operation."""
        ...

    def query(
        self, query: str, *, as_dict: bool = False
    ) -> list[tuple[Any, ...]] | list[dict[str, Any]]:
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
    ) -> tuple[Any, ...] | dict[str, Any] | None:
        """
        Execute a query using text protocol and return the first row.

        Args:
            query: SQL query string.
            as_dict: If True, return row as dictionary. If False (default), return row as tuple.

        Returns:
            First row as tuple (default) or dictionary, or None if no results.
        """
        ...

    def query_drop(self, query: str) -> None:
        """
        Execute a query using text protocol and discard the results.

        Args:
            query: SQL query string.
        """
        ...

    def query_iter(self, query: str) -> ResultSetIterator:
        """
        Execute a query using text protocol and return an iterator over the results.

        Args:
            query: SQL query string.

        Returns:
            ResultSetIterator object for iterating over rows.
        """
        ...

    def exec(
        self, query: str, params: Params = None, *, as_dict: bool = False
    ) -> list[tuple[Any, ...]] | list[dict[str, Any]]:
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
    ) -> tuple[Any, ...] | dict[str, Any] | None:
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

    def exec_iter(self, query: str, params: Params = None) -> ResultSetIterator:
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

    def __init__(self, url_or_opts: str | Opts, backend: str = "mysql") -> None:
        """
        Create a new synchronous connection.

        Args:
            url_or_opts: MySQL connection URL (e.g., 'mysql://user:password@host:port/database') or SyncOpts object.
        """
        ...

    def start_transaction(
        self,
        consistent_snapshot: bool = False,
        isolation_level: IsolationLevel | None = None,
        readonly: bool | None = None,
    ) -> Transaction: ...
    def id(self) -> int: ...
    def affected_rows(self) -> int:
        """Get the number of affected rows from the last operation."""
        ...

    def last_insert_id(self) -> int | None: ...
    def ping(self) -> None:
        """Ping the server to check connection."""
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

    def query_iter(self, query: str) -> ResultSetIterator:
        """
        Execute a query using text protocol and return an iterator over result sets.

        Args:
            query: SQL query string.

        Returns:
            Iterator over result sets.
        """
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

    def exec_batch(self, query: str, params_list: Sequence[Params] = []) -> None:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query string with '?' placeholders.
            params_list: List of parameter sets.
        """
        ...

    def exec_iter(self, query: str, params: Params = None) -> ResultSetIterator: ...
    def close(self) -> None:
        """
        Disconnect from the MySQL server.

        This closes the connection and makes it unusable for further operations.
        """
        ...

    def reset(self) -> None:
        """
        Reset the connection state.

        This resets the connection to a clean state without closing it.
        """
        ...

    def server_version(self) -> tuple[int, int, int]: ...

class Opts:
    """MySQL connection options for sync operations."""

    def pool_opts(self, pool_opts: PoolOpts) -> "Opts":
        """Set pool options for the connection."""
        ...

class OptsBuilder:
    """Builder for SyncOpts with method chaining."""

    def __init__(self) -> None:
        """Create a new SyncOptsBuilder."""
        ...

    @staticmethod
    def from_opts(opts: Opts) -> "OptsBuilder":
        """Create builder from existing SyncOpts."""
        ...

    @staticmethod
    def from_url(url: str) -> "OptsBuilder":
        """Create builder from a MySQL connection URL.

        URL format: mysql://[user[:password]@]host[:port][/database][?param1=value1&...]

        Args:
            url: MySQL connection URL string

        Returns:
            OptsBuilder configured from the URL

        Raises:
            ValueError: If the URL is invalid or cannot be parsed
        """
        ...

    @staticmethod
    def from_map(params: dict[str, str]) -> "OptsBuilder":
        """Create builder from a dictionary of parameters.

        Note: Boolean values should be encoded as 'true' and 'false' strings.

        Args:
            params: Dictionary mapping parameter names to values

        Returns:
            OptsBuilder configured from the parameters
        """
        ...

    def from_hash_map(self, params: dict[str, str]) -> "OptsBuilder":
        """Initialize from a dictionary of parameters."""
        ...
    # Network/Connection Options
    def ip_or_hostname(self, hostname: str | None) -> "OptsBuilder":
        """Set the hostname or IP address."""
        ...

    def tcp_port(self, port: int) -> "OptsBuilder":
        """Set the TCP port."""
        ...

    def socket(self, path: str | None) -> "OptsBuilder":
        """Set the Unix socket path."""
        ...

    def bind_address(self, address: str | None) -> "OptsBuilder":
        """Set the bind address for outgoing connections."""
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
    def read_timeout(self, seconds: float | None) -> "OptsBuilder":
        """Set the read timeout in seconds."""
        ...

    def write_timeout(self, seconds: float | None) -> "OptsBuilder":
        """Set the write timeout in seconds."""
        ...

    def tcp_connect_timeout(self, seconds: float | None) -> "OptsBuilder":
        """Set the TCP connection timeout in seconds."""
        ...

    def stmt_cache_size(self, size: int) -> "OptsBuilder":
        """Set the statement cache size."""
        ...
    # Additional Options
    def tcp_nodelay(self, enable: bool) -> "OptsBuilder":
        """Enable or disable TCP_NODELAY."""
        ...

    def tcp_keepalive_time_ms(self, time_ms: int | None) -> "OptsBuilder":
        """Set TCP keepalive time in milliseconds."""
        ...

    def tcp_keepalive_probe_interval_secs(
        self, interval_secs: int | None
    ) -> "OptsBuilder":
        """Set TCP keepalive probe interval in seconds."""
        ...

    def tcp_keepalive_probe_count(self, count: int | None) -> "OptsBuilder":
        """Set TCP keepalive probe count."""
        ...

    def tcp_user_timeout_ms(self, timeout_ms: int | None) -> "OptsBuilder":
        """Set TCP user timeout in milliseconds."""
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

    def connect_attrs(self, attrs: dict[str, str] | None) -> "OptsBuilder":
        """Set connection attributes."""
        ...

    def compress(self, level: int | None) -> "OptsBuilder":
        """Set compression level (0-9)."""
        ...

    def ssl_opts(self, opts: Any | None) -> "OptsBuilder":
        """Set SSL options."""
        ...

    def local_infile_handler(self, handler: Any | None) -> "OptsBuilder":
        """Set local infile handler."""
        ...

    def pool_opts(self, opts: PoolOpts) -> "OptsBuilder":
        """Set pool options."""
        ...

    def additional_capabilities(self, capabilities: int) -> "OptsBuilder":
        """Set additional capability flags."""
        ...

    def enable_cleartext_plugin(self, enable: bool) -> "OptsBuilder":
        """Enable or disable cleartext plugin."""
        ...

    def build(self) -> Opts:
        """Build the SyncOpts object."""
        ...

class PoolOpts:
    """Pool options for sync connections."""

    def __init__(self) -> None:
        """Create new SyncPoolOpts with default values."""
        ...

    def with_constraints(self, constraints: tuple[int, int]) -> "PoolOpts":
        """
        Set pool constraints as (min_connections, max_connections).

        Args:
            constraints: Tuple of (min, max) connections where min <= max.

        Returns:
            New SyncPoolOpts with updated constraints.
        """
        ...

class PooledConn:
    """
    Synchronous MySQL pooled connection.

    This represents a connection obtained from a SyncPool.
    It has the same interface as SyncConn but wraps a mysql::PooledConn.
    """

    def __init__(self) -> None:
        """
        Direct instantiation is not allowed.
        Use SyncPool.get() instead.
        """
        ...

    def start_transaction(
        self,
        consistent_snapshot: bool = False,
        isolation_level: IsolationLevel | None = None,
        readonly: bool | None = None,
    ) -> Transaction: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, _, __, ___) -> None: ...
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

    def exec_iter(self, query: str, params: Params = None) -> ResultSetIterator:
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

class Pool:
    """Synchronous MySQL connection pool."""

    def __init__(self, opts_or_url: str | Opts) -> None:
        """
        Create a new connection pool.
        Note: new() won't assert server availability.

        Args:
            opts_or_url: MySQL connection URL (e.g., 'mysql://root:password@127.0.0.1:3307/mysql')
                or SyncOpts object with connection configuration.
        """
        ...

    def get(self) -> PooledConn:
        """
        Get a connection from the pool.

        Returns:
            Connection from the pool.
        """
        ...

    def close(self) -> None:
        """
        Disconnect and close all connections in the pool.
        """
        ...
