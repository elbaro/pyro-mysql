"""Synchronous MySQL driver components."""

import datetime
import decimal
import time
from typing import Any

type Value = None | bool | int | float | str | bytes | bytearray | tuple[Any] | list[
    Any
] | set[Any] | frozenset[Any] | dict[
    str, Any
] | datetime.datetime | datetime.date | datetime.time | datetime.timedelta | time.struct_time | decimal.Decimal
type Params = None | tuple[Value, ...] | list[Value] | dict[str, Value]

from . import Row, IsolationLevel

class ResultSetIterator:
    """Iterator over MySQL result sets."""
    
    def __iter__(self) -> "ResultSetIterator":
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

    def query_iter(self, query: str) -> ResultSetIterator:
        """
        Execute a query using text protocol and return an iterator over the results.

        Args:
            query: SQL query string.

        Returns:
            ResultSetIterator object for iterating over rows.
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

    def __init__(self, url: str) -> None:
        """
        Create a new synchronous connection.

        Args:
            url: MySQL connection URL (e.g., 'mysql://user:password@host:port/database').
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