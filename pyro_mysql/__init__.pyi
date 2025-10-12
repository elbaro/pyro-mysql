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

from . import async_, sync
from . import dbapi as dbapi
from . import error as error
from .base import (
    CapabilityFlags as CapabilityFlags,
)
from .base import (
    IsolationLevel as IsolationLevel,
)
from .base import (
    JsonEncodable as JsonEncodable,
)
from .base import (
    Params as Params,
)
from .base import (
    PyroFuture as PyroFuture,
)
from .base import (
    Row as Row,
)
from .base import (
    Value as Value,
)

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
# These are exposed at module level in lib.rs
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
