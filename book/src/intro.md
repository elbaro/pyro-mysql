# Introduction

pyro-mysql is a high-performance MySQL driver for Python, backed by Rust.

## Features

- Fast sync and async connection API
- Binary protocol with prepared statements
- Automatic statement caching
- Connection pooling with buffer reuse
- MariaDB bulk execution support
- PEP-249 compatible DBAPI for SQLAlchemy

## Limitations

- No cursor-based iteration (all results are fetched into memory)
- No connection pooling in the primary API (use external pool or DBAPI)
- Async API performance is limited by GIL (will improve with Python 3.14 free-threaded builds)

## DataType Mapping

### Python -> MySQL

| Python Type | MySQL Binary Protocol Encoding |
|-------------|--------------------------------|
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
|--------------|--------|
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
