"""
pyro_mysql - High-performance MySQL driver for Python, written in Rust.

- pyro_mysql.sync: The synchronous API using the `mysql` crate.
- pyro_mysql.async_: The asynchronous API using the `mysql_async` crate.
- pyro_mysql.error: Exceptions.

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
from collections.abc import Generator, Sequence
from typing import Any, Awaitable, TypeVar

from . import async_, sync
from . import dbapi as dbapi
from . import error as error

def init(worker_threads: int | None = 1, thread_name: str | None = None) -> None:
    """
    Initialize the Tokio runtime for async operations.
    This function can be called multiple times until Any async operation is called.

    Args:
        worker_threads: Number of worker threads for the Tokio runtime. If None, set to the number of CPUs.
        thread_name: Name prefix for worker threads.
    """
    ...

# Compatibility aliases for backward compatibility
AsyncConn = async_.Conn
AsyncPool = async_.Pool
AsyncTransaction = async_.Transaction
AsyncOpts = async_.Opts
AsyncOptsBuilder = async_.OptsBuilder
AsyncPoolOpts = async_.PoolOpts

SyncConn = sync.Conn
SyncPool = sync.Pool
SyncPooledConn = sync.PooledConn
SyncTransaction = sync.Transaction
SyncOpts = sync.Opts
SyncOptsBuilder = sync.OptsBuilder
SyncPoolOpts = sync.PoolOpts

JsonEncodable = (
    dict[str, "JsonEncodable"] | list["JsonEncodable"] | str | int | float | bool | None
)

type Value = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | bytearray
    | tuple[JsonEncodable, ...]
    | list[JsonEncodable]
    | set[JsonEncodable]
    | frozenset[JsonEncodable]
    | dict[str, JsonEncodable]
    | datetime.datetime
    | datetime.date
    | datetime.time
    | datetime.timedelta
    | time.struct_time
    | decimal.Decimal
)

"""
Parameters that can be passed to query execution methods:
- `None`: No parameters
- `tuple[Value, ...]`: Positional parameters for queries with ? placeholders
- `list[Value]`: List of parameters for queries with ? placeholders  
- `dict[str, Value]`: Named parameters for queries with named placeholders

Examples:
No parameters:

    `await conn.exec("SELECT * FROM users")`

Positional parameters:

    `await conn.exec("SELECT * FROM users WHERE id = ?", (123,))`

Multiple positional parameters:

    `await conn.exec("SELECT * FROM users WHERE age > ? AND city = ?", (18, "NYC"))`

Named parameters:

    `await conn.exec("SELECT * FROM users WHERE age > :age AND city = :city", dict(age=18, name="NYC"))`
"""
type Params = None | tuple[Value, ...] | Sequence[Value] | dict[str, Value]

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

T = TypeVar("T")

class PyroFuture(Awaitable[T]):
    def __await__(self) -> Generator[Any, Any, T]: ...
    def cancel(self) -> bool: ...
    def get_loop(self): ...

class IsolationLevel:
    """Transaction isolation level enum."""

    ReadUncommitted: "IsolationLevel"
    ReadCommitted: "IsolationLevel"
    RepeatableRead: "IsolationLevel"
    Serializable: "IsolationLevel"

    def as_str(self) -> str:
        """Return the isolation level as a string."""
        ...

class CapabilityFlags:
    """MySQL capability flags for client connections."""

    CLIENT_LONG_PASSWORD: int
    CLIENT_FOUND_ROWS: int
    CLIENT_LONG_FLAG: int
    CLIENT_CONNECT_WITH_DB: int
    CLIENT_NO_SCHEMA: int
    CLIENT_COMPRESS: int
    CLIENT_ODBC: int
    CLIENT_LOCAL_FILES: int
    CLIENT_IGNORE_SPACE: int
    CLIENT_PROTOCOL_41: int
    CLIENT_INTERACTIVE: int
    CLIENT_SSL: int
    CLIENT_IGNORE_SIGPIPE: int
    CLIENT_TRANSACTIONS: int
    CLIENT_RESERVED: int
    CLIENT_SECURE_CONNECTION: int
    CLIENT_MULTI_STATEMENTS: int
    CLIENT_MULTI_RESULTS: int
    CLIENT_PS_MULTI_RESULTS: int
    CLIENT_PLUGIN_AUTH: int
    CLIENT_CONNECT_ATTRS: int
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: int
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: int
    CLIENT_SESSION_TRACK: int
    CLIENT_DEPRECATE_EOF: int
    CLIENT_OPTIONAL_RESULTSET_METADATA: int
    CLIENT_ZSTD_COMPRESSION_ALGORITHM: int
    CLIENT_QUERY_ATTRIBUTES: int
    MULTI_FACTOR_AUTHENTICATION: int
    CLIENT_PROGRESS_OBSOLETE: int
    CLIENT_SSL_VERIFY_SERVER_CERT: int
    CLIENT_REMEMBER_OPTIONS: int
