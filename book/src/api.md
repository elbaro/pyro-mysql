# API Reference

## Module Structure

```
pyro_mysql              # Opts, IsolationLevel, Row
├── sync                # Conn, Pool, PooledConn, Transaction
├── async_              # Conn, Pool, PooledConn, Transaction (async)
├── dbapi               # PEP-249 DBAPI for SQLAlchemy
├── dbapi_async         # Async DBAPI
└── error               # Exception classes
```

## Type Stubs

- [`pyro_mysql`](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/__init__.pyi) - Core types and options
- [`pyro_mysql.sync`](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/sync.pyi) - Synchronous API
- [`pyro_mysql.async_`](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/async_.pyi) - Asynchronous API
- [`pyro_mysql.dbapi`](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/dbapi.pyi) - PEP-249 DBAPI
- [`pyro_mysql.dbapi_async`](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/dbapi_async.pyi) - Async DBAPI
- [`pyro_mysql.error`](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/error.pyi) - Exceptions
