# pyro-mysql

A high-performance MySQL driver for Python, backed by Rust.

- [API Overview](#api-overview)
- [Usage](#usage)
- [DataType Mapping](#datatype-mapping)
- [Logging](#logging)
- [PEP-249, sqlalchemy](#pep-249-sqlalchemy)
- [Benchmark](https://htmlpreview.github.io/?https://github.com/elbaro/pyro-mysql/blob/main/BENCHMARK.md)

<img src="https://github.com/elbaro/pyro-mysql/blob/main/report/chart.png?raw=true" width="800px" />


## Usage


### 0. Import

```py
# Async
from pyro_mysql.async_ import Conn, Pool
from pyro_mysql import AsyncConn, AsyncPool

# Sync
from pyro_mysql.sync import Conn, Transaction
from pyro_mysql import SyncConn, SyncTransaction
````

### 1. Connection


```py
from pyro_mysql.async_ import Conn, Pool, OptsBuilder


# Optionally configure the number of Rust threads
# pyro_mysql.init(worker_threads=1)

def example1():
    conn = await Conn.new(f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

def example2():
    pool = Pool(
        OptsBuilder()
            .ip_or_hostname("localhost")
            .port(3333)
            .user("username")
            .db_name("db")
            .wait_timeout(100)
            .tcp_nodelay(True)
            .compression(3)
            .build()
    )
    conn = await pool.get()

def example3(pool):
    with pool.get() as conn:
        ...
```


### 2. Query Execution

`AsyncConn` and `AsyncTransaction` provide the following methods.
`SyncConn`, `SyncPooledConn` and `SyncTransaction` provide similar API.

```py

# Text Protocols - supports multiple statements concatenated with ';' but accepts no arguemnt
def query(self, query: str) -> Awaitable[list[Row]]: ...
def query_first(self, query: str) -> Awaitable[Row | None]: ...
def query_drop(self, query: str) -> Awaitable[None]: ...
def query_batch(self, query: str) -> Awaitable[None]: ...

# Binary Protocols - supports arguments but no multiple statement
def exec(self, query: str, params: Params) -> Awaitable[list[Row]]: ...
def exec_first(self, query: str, params: Params) -> Awaitable[Row | None]: ...
def exec_drop(self, query: str, params: Params) -> Awaitable[None]: ...
def exec_batch(self, query: str, params: Iterable[Params]) -> Awaitable[None]: ...

# Examples
rows = await conn.exec("SELECT * FROM my_table WHERE a=? AND b=?", (a, b))
rows = await conn.exec("SELECT * FROM my_table WHERE a=:x AND b=:y AND c=:y", {'x': 100, 'y': 200})
await conn.exec_batch("SELECT * FROM my_table WHERE a=? AND b=?", [(a1, b1), (a2, b2)])
```

`Awaitable` is a coroutine or `PyroFuture`, which is a Future-like object that tracks a task in the Rust thread. If the returned object is dropped before completion or cancellation, the corresponding task in the Rust thread is cancelled as well.

### 3. Transaction

```py
# async API
async with conn.start_transaction() as tx:
    await tx.exec('INSERT ..')
    await tx.exec('INSERT ..')
    await tx.commit()
    await conn.exec(..)  # this is not allowed

# sync API
with conn.start_transaction() as tx:
    tx.exec('INSERT ..')
    tx.exec('INSERT ..')
    conn.exec('INSERT ..')  # this is not allowed
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
| `FLOAT` | `float` |
| `DOUBLE` | `float` |
| `DECIMAL` / `NUMERIC` | `decimal.Decimal` |
| `DATE` | `datetime.date` |
| `TIME` | `datetime.timedelta` |
| `DATETIME` | `datetime.datetime` |
| `TIMESTAMP` | `datetime.datetime` |
| `CHAR` / `VARCHAR` / `TEXT` / `TINYTEXT` / `MEDIUMTEXT` / `LONGTEXT` | `str` |
| `BINARY` / `VARBINARY` / `BLOB` / `TINYBLOB` / `MEDIUMBLOB` / `LONGBLOB` | `bytes` |
| `JSON` | the result of json.loads() |
| `ENUM` / `SET` | `str` |
| `BIT` | `bytes` |
| `GEOMETRY` | `bytes` (WKB format) |

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

The supported connection parameters are [the docs](https://docs.rs/mysql/latest/mysql/struct.OptsBuilder.html#method.from_hash_map) and [`capabilities`](https://docs.rs/mysql/latest/mysql/consts/struct.CapabilityFlags.html) (default 2).

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

```
.
└── pyro_mysql/
    ├── init()
    ├── (common classes)/
    │   ├── Row
    │   ├── IsolationLevel
    │   ├── CapabilityFlags
    │   └── PyroFuture
    ├── sync/
    │   ├── Conn
    │   ├── Transaction
    │   ├── Pool
    │   ├── Opts
    │   ├── OptsBuilder
    │   └── PoolOpts
    ├── async_/
    │   ├── Conn
    │   ├── Transaction
    │   ├── Pool
    │   ├── Opts
    │   ├── OptsBuilder
    │   └── PoolOpts
    ├── dbapi/
    │   ├── connect()
    │   ├── Connection
    │   ├── Cursor
    │   └── (exceptions)/
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
    │   └── (exceptions)/
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
        ├── SyncPool
        ├── SyncOpts
        ├── SyncOptsBuilder
        ├── SyncPoolOpts
        ├── AsyncConn
        ├── AsyncTransaction
        ├── AsyncPool
        ├── AsyncOpts
        ├── AsyncOptsBuilder
        └── AsyncPoolOpts
```
