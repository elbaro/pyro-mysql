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
    "async_",
    "sync",
    "AsyncConn",
    "AsyncPool",
    "AsyncTransaction",
    "SyncConn",
    "SyncTransaction",
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
        async def new(url: str) -> "async_.Conn":
            """
            Create a new connection.

            Args:
                url: MySQL connection URL (e.g., 'mysql://user:password@host:port/database').

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

        async def get_conn(self) -> "async_.Conn":
            """
            Get a connection from the pool.

            Returns:
                Connection from the pool.
            """
            ...

# ============================================================================
# Sync API
# ============================================================================

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

# # Re-export async classes with prefix
# AsyncConn = async_.Conn
# AsyncPool = async_.Pool
# AsyncTransaction = async_.Transaction

# # Re-export sync classes with prefix
# SyncConn = sync.Conn
# SyncTransaction = sync.Transaction
