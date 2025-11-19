import datetime
from types import TracebackType
from typing import Any, Self, Sequence

from pyro_mysql import IsolationLevel, Opts, Params, PyroFuture

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
                or Opts object with connection configuration.

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
