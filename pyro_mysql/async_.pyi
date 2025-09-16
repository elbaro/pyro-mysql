"""Async MySQL driver components."""

import datetime
import decimal
import time
from types import TracebackType
from typing import Any, Self

type Value = None | bool | int | float | str | bytes | bytearray | tuple[Any] | list[
    Any
] | set[Any] | frozenset[Any] | dict[
    str, Any
] | datetime.datetime | datetime.date | datetime.time | datetime.timedelta | time.struct_time | decimal.Decimal
type Params = None | tuple[Value, ...] | list[Value] | dict[str, Value]

from . import Row, TxOpts, IsolationLevel

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
    async def new(url: str) -> "Conn":
        """
        Create a new connection.

        Args:
            url: MySQL connection URL (e.g., 'mysql://user:password@host:port/database').

        Returns:
            New Conn instance.
        """
        ...

    def start_transaction(self, opts: TxOpts = ...) -> Transaction:
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

class Pool:
    """
    MySQL connection pool.
    """

    def __init__(self, url: str) -> None:
        """
        Create a new connection pool.
        Note: new() won't assert server availability.

        Args:
            url: MySQL connection URL (e.g., 'mysql://root:password@127.0.0.1:3307/mysql').
        """
        ...

    async def get_conn(self) -> Conn:
        """
        Get a connection from the pool.

        Returns:
            Connection from the pool.
        """
        ...