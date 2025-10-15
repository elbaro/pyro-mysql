# pyro-mysql

A high-performance MySQL driver for Python, backed by Rust.

- [API Reference](https://htmlpreview.github.io/?https://raw.githubusercontent.com/elbaro/pyro-mysql/main/docs/pyro_mysql.html)
- [Benchmark](https://htmlpreview.github.io/?https://github.com/elbaro/pyro-mysql/blob/main/report/report/index.html)


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
import pyro_mysql
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
def query(self, query: str) -> PyroFuture[list[Row]]: ...
def query_first(self, query: str) -> PyroFuture[Row | None]: ...
def query_drop(self, query: str) -> PyroFuture[None]: ...
def query_batch(self, query: str) -> PyroFuture[None]: ...

# Binary Protocols - supports arguments but no multiple statement
def exec(self, query: str, params: Params) -> PyroFuture[list[Row]]: ...
def exec_first(self, query: str, params: Params) -> PyroFuture[Row | None]: ...
def exec_drop(self, query: str, params: Params) -> PyroFuture[None]: ...
def exec_batch(self, query: str, params: Iterable[Params]) -> PyroFuture[None]: ...

# Examples
rows = await conn.exec("SELECT * FROM my_table WHERE a=? AND b=?", (a, b))
rows = await conn.exec("SELECT * FROM my_table WHERE a=:x AND b=:y AND c=:y", {'x': 100, 'y': 200})
await conn.exec_batch("SELECT * FROM my_table WHERE a=? AND b=?", [(a1, b1), (a2, b2)])
```

`PyroFuture` is a Future-like object that tracks a task in the Rust thread. When an object of `PyroFuture` is dropped before completion or cancellation, the corresponding task in the Rust thread is cancelled.

```py
fut = conn.exec("SELECT ...")  # the Rust thread starts to execute the query before we await the Python future.

print(fut.get_loop())  # get the associated Python event loop
fut.cancel()  # cancels the Rust task
del fut  # this is equivalent to .cancel()
```

### 3. Transaction

```py
# async API
async with conn.start_transaction() as tx:
    await tx.exec('INSERT ..')
    await tx.exec('INSERT ..')
    await tx.commit()  # tx cannot be used anymore
    await conn.exec(..)  # error

# sync API
with conn.start_transaction() as tx:
    tx.exec('INSERT ..')
    tx.exec('INSERT ..')
    conn.exec('INSERT ..')  # error
    tx.commit()  # tx cannot be used anymore
```

## DataType Mapping

### Python -> MySQL

| Python Type | MySQL Binary Protocol Encoding |
|-------------|------------|
| `None` | `NULL` |
| `bool` | `Int64` |
| `int` | `Int64` |
| `float` | `Double(Float64)` |
| `str \| bytearray` | `Bytes` |
| `tuple \| list \| set \| frozenset \| dict` | json-encoded string as `Bytes` |
| `datetime.datetime` | `Date(year, month, day, hour, minute, second, microsecond)` |
| `datetime.date` | `Date(year, month, day, 0, 0, 0, 0)` |
| `datetime.time` | `Time(false, 0, hour, minute, second, microsecond)` |
| `datetime.timedelta` | `Time(is_negative, days, hours, minutes, seconds, microseconds)` |
| `time.struct_time` | `Date(year, month, day, hour, minute, second, 0)` |
| `decimal.Decimal` | `Bytes` |

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

In sqlalchemy, use `mysql+pyro_mysql://` or `mariadb+pyro_mysql://`.
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

## Perf Notes
- Text Protocol vs Binary Protocol
    - Binary Protocol is slower for one-time statements because it always prepares the statement and then execute (2 round trips), but is safer against SQL injections. If you reuse the prepared statement many times, Binary Protocol is faster. Most Python MySQL libraries use Text Protocol.
- executemany() vs exec_batch()
    - `pyro_mysql.Conn.exec_batch()` prepares a statement once and make a network call for each parameter item. MySQL libraries using Text Protocol implement `executemany()` as one large sql statement: they repeat the SQL statement, concatenate, stringify parameters in the client-side and then send the large string to the server. This manual sql building is actually twice faster. If it's worth, you can try manually building the statement and call `pyro_mysql.Conn.exec()`.
