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

from . import async_, sync

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

type Value = None | bool | int | float | str | bytes | bytearray | tuple[Any] | list[
    Any
] | set[Any] | frozenset[Any] | dict[
    str, Any
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

# Re-export async classes with prefix
AsyncConn = async_.Conn
AsyncPool = async_.Pool
AsyncTransaction = async_.Transaction

# Re-export sync classes with prefix
SyncConn = sync.Conn
SyncTransaction = sync.Transaction



