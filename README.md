# pyro-mysql

A high-performance MySQL driver for Python, backed by Rust.

- [API Reference](https://htmlpreview.github.io/?https://github.com/elbaro/pyro-mysql/blob/main/docs.html)
- [Benchmark](https://htmlpreview.github.io/?https://github.com/elbaro/pyro-mysql/blob/main/report/report/index.html)


<img src="https://github.com/elbaro/pyro-mysql/blob/main/report/chart.png?raw=true" width="800px" />


## Usage

### 1. Connection

```py
mysql_url = f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

# Directly connect
conn = await pyro_mysql.Conn.new(mysql_url)

# Acquire from a pool
pool = pyro_mysql.Pool(mysql_url)
conn = await pool.acquire()

# Sync API
pool = pyro_mysql.SyncPool(mysql_url)
conn = pyro_mysql.SyncConn(mysql_url) or pool.acquire()
```

### 2. Query Execution

`Conn` and `Transaction` provides the following methods.
`SyncConn` and `SyncTransaction` provides the sync versions.

```py
async def exec(self, query: str, params: Params) -> list[Row]
async def exec_first(self, query: str, params: Params) -> Row | None
async def exec_drop(self, query: str, params: Params) -> None
async def exec_batch(self, query: str, params: Iterable[Params]) -> None

# Examples
rows = await conn.exec("SELECT * FROM my_table WHERE a=? AND b=?", (a, b))
rows = await conn.exec("SELECT * FROM my_table WHERE a=:x AND b=:y AND c=:y", {'x': 100, 'y': 200})
await conn.exec_batch("SELECT * FROM my_table WHERE a=? AND b=?", [(a1, b1), (a2, b2)])
```

For exact description of each API, refer to [the Rust doc](https://docs.rs/mysql/latest/mysql/prelude/trait.Queryable.html).

### 3. Transaction

```py
# async API
async with conn.start_transaction() as tx:
    await tx.exec('INSERT ..')
    await tx.exec('INSERT ..')
    await tx.commit()  # tx cannot be used anymore
    # await conn.exec(..)  # error: conn cannot be used while tx is active

# sync API
def func(tx: SyncTransaction):
    tx.exec('INSERT ..')
    tx.exec('INSERT ..')
    tx.commit()  # tx cannot be used anymore
conn.run_transaction(func)
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
