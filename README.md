# pyro-mysql

A high-performance MySQL driver for Python, backed by Rust.

- [API Overview](#api-overview)
- [Usage](#usage)
- [DataType Mapping](#datatype-mapping)
- [Logging](#logging)
- [PEP-249, sqlalchemy](#pep-249-sqlalchemy)
- [Benchmark](https://github.com/elbaro/pyro-mysql/blob/main/BENCHMARK.md)

<img src="https://github.com/elbaro/pyro-mysql/blob/main/report/chart.png?raw=true" width="800px" />


## Usage


### 0. Import

```py
# Async
from pyro_mysql.async_ import Conn
from pyro_mysql import AsyncConn

# Sync
from pyro_mysql.sync import Conn
from pyro_mysql import SyncConn
```

### 1. Connection


```py
from pyro_mysql.async_ import Conn
from pyro_mysql import Opts

async def example():
    conn1 = await Conn.new(f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    conn2 = await Conn.new(
        Opts(f"mysql://{USER}:{PASSWORD}@localhost")
            .tcp_nodelay(False)
    )
    conn3 = await Conn.new(
        Opts()
            .socket("../unix.socket")
            .user("username")
            .db("db")
    )
```


### 2. Query Execution

`SyncConn` and `AsyncConn` provides the following 8 methods (SyncConn doesn't return Awaitable):

```py
# Text Protocols - supports multiple statements concatenated with ';' but accepts no argument
def query(self, query: str, *, as_dict: bool = False) -> Awaitable[list[tuple] | list[dict]]: ...
def query_first(self, query: str, *, as_dict: bool = False) -> Awaitable[tuple | dict | None]: ...
def query_drop(self, query: str) -> Awaitable[None]: ...

# Binary Protocols - supports arguments but no multiple statement
def exec(self, query: str, params: Params = None, *, as_dict: bool = False) -> Awaitable[list[tuple] | list[dict]]: ...
def exec_first(self, query: str, params: Params = None, *, as_dict: bool = False) -> Awaitable[tuple | dict | None]: ...
def exec_drop(self, query: str, params: Params = None) -> Awaitable[None]: ...
def exec_batch(self, query: str, params: Sequence[Params] = []) -> Awaitable[None]: ...
def exec_bulk_insert_or_update(self, query: str, params: Sequence[Params] = [], *, as_dict: bool = False) -> Awaitable[list[tuple] | list[dict]]: ...

# Examples
rows = await conn.exec("SELECT * FROM my_table WHERE a=? AND b=?", (a, b))  # returns list of tuples
rows_as_dicts = await conn.exec("SELECT * FROM my_table WHERE a=? AND b=?", (a, b), as_dict=True)  # returns list of dicts
await conn.exec_batch("SELECT * FROM my_table WHERE a=? AND b=?", [(a1, b1), (a2, b2)])
```

`Awaitable` is a coroutine or `PyroFuture`, which is a Future-like object that tracks a task in the Rust thread. If the returned object is dropped before completion or cancellation, the corresponding task in the Rust thread is cancelled as well.

### 3. Transaction

```py
# async API
async with conn.start_transaction() as tx:
    await conn.exec('INSERT ..')
    await conn.exec('INSERT ..')
    await tx.commit()

# sync API
with conn.start_transaction() as tx:
    conn.exec('INSERT ..')
    conn.exec('INSERT ..')
    tx.rollback()
```

## DataType Mapping

### Python -> MySQL

| Python Type | MySQL Binary Protocol Encoding |
|-------------|------------|
| `None` | `NULL` |
| `bool` | `Int64` |
| `int` | `Int64` |
| `float` | `Double(Float64)` |
| `str \| bytes \| bytearray` | `Bytes` |
| `tuple \| list \| set \| frozenset \| dict` | json-encoded string as `Bytes` |
| `datetime.datetime` | `Date(year, month, day, hour, minute, second, microsecond)` |
| `datetime.date` | `Date(year, month, day, 0, 0, 0, 0)` |
| `datetime.time` | `Time(false, 0, hour, minute, second, microsecond)` |
| `datetime.timedelta` | `Time(is_negative, days, hours, minutes, seconds, microseconds)` |
| `time.struct_time` | `Date(year, month, day, hour, minute, second, 0)` |
| `decimal.Decimal` | `Bytes(str(Decimal))` |
| `uuid.UUID` | `Bytes(UUID.hex)` |

### MySQL -> Python

| MySQL Column | Python |
|-------------|------------|
| `NULL` | `None` |
| `INT` / `TINYINT` / `SMALLINT` / `MEDIUMINT` / `BIGINT` / `YEAR` | `int` |
| `FLOAT` / `DOUBLE` | `float` |
| `DECIMAL` / `NUMERIC` | `decimal.Decimal` |
| `DATE` | `datetime.date` or `None` (0000-00-00) |
| `DATETIME` / `TIMESTAMP` | `datetime.datetime` or `None` (0000-00-00 00:00:00) |
| `TIME` | `datetime.timedelta` |
| `CHAR` / `VARCHAR` / `TEXT` / `TINYTEXT` / `MEDIUMTEXT` / `LONGTEXT` | `str` |
| `BINARY` / `VARBINARY` / `BLOB` / `TINYBLOB` / `MEDIUMBLOB` / `LONGBLOB` | `bytes` |
| `JSON` | `str` or the result of `json.loads()` |
| `ENUM` / `SET` | `str` |
| `BIT` | `bytes` |
| `GEOMETRY` | `bytes` |

## Logging

pyro-mysql sends the Rust logs to the Python logging system, which can be configured with `logging.getLogger("pyro_mysql")`.

```py
# Queries are logged with the DEBUG level
logging.getLogger("pyro_mysql").setLevel(logging.DEBUG)
```

## PEP-249, sqlalchemy

<img src="https://github.com/elbaro/pyro-mysql/blob/main/report/chart_sqlalchemy.png?raw=true" width="800px" />

`pyro_mysql.dbapi` implements PEP-249.
This only exists for compatibility with ORM libraries.
The primary API set (`pyro_mysql.sync`, `pyro_mysql.async_`) is simpler and faster.

```sh
pyro_mysql.dbapi
    # classes
    ├─Connection
    ├─Cursor
    # exceptions
    ├─Warning
    ├─Error
    ├─IntegrityError
    ├─..
```

In sqlalchemy, the following dialects are supported.
- `mysql+pyro_mysql://` (sync)
- `mariadb+pyro_mysql://` (sync)
- `mysql+pyro_mysql_async://` (async)
- `mariadb+pyro_mysql_async://` (async)

Connection options can be configured using the `Opts` builder class. See `pyro_mysql.Opts` for available options.

```py
from sqlalchemy import create_engine, text

engine = create_engine("mysql+pyro_mysql://test:1234@localhost/test")
conn = engine.connect()
cursor_result = conn.execute(text("SHOW TABLES"))
for row in cursor_result:
    print(row)
```

```
('information_schema',)
('mysql',)
('performance_schema',)
('sys',)
('test',)
```

To run sqlalchemy tests on pyro_mysql, use this command in the sqlalchemy repo:

```sh
pytest -p pyro_mysql.testing.sqlalchemy_pytest_plugin --dburi=mariadb+pyro_mysql://test:1234@localhost/test -v t
```

`sqlalchemy_pytest_plugin` is required to skip incompatible tests.


## API Overview

- [pyro_mysql](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/__init__.pyi)
- [pyro_mysql,sync](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/sync.pyi)
- [pyro_mysql.async_](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/async_.pyi)
- [pyro_mysql.dbapi](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/dbapi.pyi)
- [pyro_mysql.dbapi_async](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/dbapi_async.pyi)
- [pyro_mysql.error](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/error.pyi)

There is no auto-generated API Reference. *.pyi files are manually synced.

```
.
└── pyro_mysql/
    ├── (common classes)/
    │   ├── Opts
    │   ├── BufferPool
    │   ├── IsolationLevel
    │   ├── CapabilityFlags
    │   └── PyroFuture
    ├── sync/
    │   ├── Conn
    │   └── Transaction
    ├── async_/
    │   ├── Conn
    │   └── Transaction
    ├── dbapi/
    │   ├── connect()
    │   ├── Connection
    │   ├── Cursor
    │   └── (exceptions)
    │       ├── Warning
    │       ├── Error
    │       ├── InterfaceError
    │       ├── DatabaseError
    │       ├── DataError
    │       ├── OperationalError
    │       ├── IntegrityError
    │       ├── InternalError
    │       ├── ProgrammingError
    │       └── NotSupportedError
    ├── dbapi_async/
    │   ├── connect()
    │   ├── Connection
    │   ├── Cursor
    │   └── (exceptions)
    │       ├── Warning
    │       ├── Error
    │       ├── InterfaceError
    │       ├── DatabaseError
    │       ├── DataError
    │       ├── OperationalError
    │       ├── IntegrityError
    │       ├── InternalError
    │       ├── ProgrammingError
    │       └── NotSupportedError
    └── (aliases)/
        ├── SyncConn
        ├── SyncTransaction
        ├── AsyncConn
        ├── AsyncTransaction
        └── Opts
```

## Perf Notes
- Prefer MariaDB to MySQL
- Prefer UnixSocket to TCP
- Use BufferPool to reuse allocations between connections
- Use Conn.exec_bulk_insert_or_update to group 2~1000 INSERTTs or UPDATEEs
- The async API is fast but still far from optimal due to GIL. Wait for Python 3.14 + mature free-threaded build for faster asyncio performance
- The sync API is optimized for single-thread. The library does not actively release the GIL during operations. When free-threaded python gets mature, the optimal API will be reconsidered.
