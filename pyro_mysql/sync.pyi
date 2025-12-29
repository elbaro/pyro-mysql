"""Synchronous MySQL driver components."""

from types import TracebackType
from typing import Any, Literal, Self, Sequence, overload

from pyro_mysql import IsolationLevel, Opts, Params

class Transaction:
    """
    Represents a synchronous MySQL transaction.

    Note: Query and exec methods are NOT available on Transaction.
    Use the connection's query/exec methods while the transaction is active.
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

class Conn:
    """
    Synchronous MySQL connection.
    """

    def __init__(self, url_or_opts: str | Opts) -> None:
        """
        Create a new synchronous connection.

        Args:
            url_or_opts: MySQL connection URL (e.g., 'mysql://user:password@host:port/database') or Opts object.
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

    @overload
    def query(
        self, query: str, *, as_dict: Literal[False] = False
    ) -> list[tuple[Any, ...]]: ...
    @overload
    def query(self, query: str, *, as_dict: Literal[True]) -> list[dict[str, Any]]: ...
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

    @overload
    def query_first(
        self, query: str, *, as_dict: Literal[False] = False
    ) -> tuple[Any, ...] | None: ...
    @overload
    def query_first(
        self, query: str, *, as_dict: Literal[True]
    ) -> dict[str, Any] | None: ...
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

    @overload
    def exec(
        self, query: str, params: Params = None, *, as_dict: Literal[False] = False
    ) -> list[tuple[Any, ...]]: ...
    @overload
    def exec(
        self, query: str, params: Params = None, *, as_dict: Literal[True]
    ) -> list[dict[str, Any]]: ...
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

    @overload
    def exec_first(
        self, query: str, params: Params = None, *, as_dict: Literal[False] = False
    ) -> tuple[Any, ...] | None: ...
    @overload
    def exec_first(
        self, query: str, params: Params = None, *, as_dict: Literal[True]
    ) -> dict[str, Any] | None: ...
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

    def exec_batch(self, query: str, params_list: Sequence[Params] = []) -> None:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query string with '?' placeholders.
            params_list: List of parameter sets.
        """
        ...

    @overload
    def exec_bulk_insert_or_update(
        self,
        query: str,
        params_list: Sequence[Params] = [],
        *,
        as_dict: Literal[False] = False,
    ) -> list[tuple[Any, ...]]: ...
    @overload
    def exec_bulk_insert_or_update(
        self, query: str, params_list: Sequence[Params] = [], *, as_dict: Literal[True]
    ) -> list[dict[str, Any]]: ...
    def exec_bulk_insert_or_update(
        self, query: str, params_list: Sequence[Params] = [], *, as_dict: bool = False
    ) -> list[tuple[Any, ...]] | list[dict[str, Any]]:
        """
        Execute a query multiple times with different parameters and return all results.

        Args:
            query: SQL query string with '?' placeholders.
            params_list: List of parameter sets.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            List of tuples (default) or dictionaries.
        """
        ...

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

    def server_version(self) -> str: ...
