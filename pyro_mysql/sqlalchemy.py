"""
SQLAlchemy dialect for pyro-mysql driver.

Provides both synchronous and asynchronous dialect implementations for
integrating pyro-mysql with SQLAlchemy.
"""

from typing import Any, cast, override

from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.interfaces import ConnectArgsType, DBAPIConnection, DBAPIModule
from sqlalchemy.engine.url import URL

from pyro_mysql.dbapi import Error


class MySQLDialect_pyro(MySQLDialect):
    """Synchronous SQLAlchemy dialect for pyro-mysql."""

    driver: str = "pyro_mysql"
    supports_statement_cache: bool = False
    supports_unicode_statements: bool = True
    # TODO:
    # supports_sane_rowcount = True
    # supports_sane_multi_rowcount = True
    supports_server_side_cursors: bool = False
    supports_native_decimal: bool = True
    default_paramstyle: str = "qmark"

    @override
    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """Import and return the DBAPI module."""
        from pyro_mysql import dbapi

        return dbapi  # pyright: ignore [reportReturnType]

    @override
    def create_connect_args(self, url: URL) -> ConnectArgsType:
        """Convert SQLAlchemy URL to connection arguments for pyro-mysql."""

        dic: dict[str, Any] = (
            url.translate_connect_args(
                database="db_name",
                username="user",
                password="password",
                host="host",
                port="port",
            )
            | url.query
        )

        str_dic: dict[str, str] = {}
        for k, v in dic.items():
            if isinstance(v, str):
                str_dic[k] = v
            elif isinstance(v, int):
                str_dic[k] = str(v)
            elif isinstance(v, bool):
                str_dic[k] = "true" if v else "false"
            else:
                raise Error("the connection argument should be str, int, or bool")

        # https://docs.rs/mysql/latest/src/mysql/conn/opts/mod.rs.html#593-595
        from pyro_mysql.sync import OptsBuilder

        try:
            opts = OptsBuilder.from_map(str_dic).build()
        except Exception as e:
            raise Error("wrong connection argument") from e

        return cast(ConnectArgsType, ((opts,), {}))

    def do_ping(self, dbapi_connection: DBAPIConnection) -> bool:
        """Check if connection is alive."""
        try:
            return dbapi_connection.ping()
        except Exception:
            return False

    def _detect_charset(self, connection: Connection) -> str:
        # TODO
        return "utf8mb4"

    def _extract_error_code(self, exception: Exception) -> int | None:
        """Extract MySQL error code from exception."""
        # MySQL error format: "ERROR 1146 (42S02): Table 'test.asdf' doesn't exist"
        import re

        error_str = str(exception)
        match = re.search(r"ERROR\s+(\d+)\s+\([^)]+\):", error_str)
        if match:
            return int(match.group(1))
        return None


# class AsyncAdapt_pyro_mysql_cursor:
#     """Async adapter for pyro-mysql cursor."""

#     def __init__(self, cursor):
#         self._cursor = cursor
#         self.description = cursor.description
#         self.rowcount = cursor.rowcount
#         self.lastrowid = cursor.lastrowid

#     async def execute(self, query: str, parameters: Any | None = None):
#         """Execute a query asynchronously."""
#         return await self._cursor.execute(query, parameters)

#     async def executemany(self, query: str, parameters: list[Any]):
#         """Execute a query with multiple parameter sets."""
#         return await self._cursor.executemany(query, parameters)

#     async def fetchone(self):
#         """Fetch one row."""
#         return await self._cursor.fetchone()

#     async def fetchmany(self, size: int | None = None):
#         """Fetch multiple rows."""
#         return await self._cursor.fetchmany(size)

#     async def fetchall(self):
#         """Fetch all rows."""
#         return await self._cursor.fetchall()

#     async def close(self):
#         """Close the cursor."""
#         return await self._cursor.close()

#     def __aiter__(self):
#         """Make cursor async iterable."""
#         return self

#     async def __anext__(self):
#         """Get next row in async iteration."""
#         row = await self.fetchone()
#         if row is None:
#             raise StopAsyncIteration
#         return row


# class AsyncAdapt_pyro_mysql_connection(AdaptedConnection):
#     """Async adapter for pyro-mysql connection."""

#     __slots__ = ("_connection",)

#     def __init__(self, connection):
#         self._connection = connection

#     @property
#     def driver_connection(self):
#         """Return the underlying driver connection."""
#         return self._connection

#     async def ping(self, reconnect: bool = True):
#         """Ping the server to check if connection is alive."""
#         if hasattr(self._connection, "ping"):
#             return await self._connection.ping()
#         else:
#             # Fallback to executing a simple query
#             cursor = await self.cursor()
#             await cursor.execute("SELECT 1")
#             await cursor.close()

#     async def cursor(self):
#         """Create a new cursor."""
#         cursor = await self._connection.cursor()
#         return AsyncAdapt_pyro_mysql_cursor(cursor)

#     async def commit(self):
#         """Commit the current transaction."""
#         return await self._connection.commit()

#     async def rollback(self):
#         """Rollback the current transaction."""
#         return await self._connection.rollback()

#     async def close(self):
#         """Close the connection."""
#         return await self._connection.close()

#     async def begin(self):
#         """Begin a transaction."""
#         if hasattr(self._connection, "begin"):
#             return await self._connection.begin()
#         # Some drivers start transactions implicitly


# class AsyncAdapt_pyro_mysql_dbapi:
#     """Async DBAPI adapter for pyro-mysql."""

#     def __init__(self):
#         self._dbapi = None

#     @property
#     def dbapi(self):
#         """Lazy import of async module."""
#         if self._dbapi is None:
#             import pyro_mysql.async_ as async_module

#             self._dbapi = async_module
#         return self._dbapi

#     @property
#     def paramstyle(self):
#         """Parameter style used by the driver."""
#         return "qmark"

#     @property
#     def Error(self):
#         """Base error class."""
#         return self.dbapi.Error if hasattr(self.dbapi, "Error") else Exception

#     @property
#     def InterfaceError(self):
#         """Interface error class."""
#         return (
#             self.dbapi.InterfaceError
#             if hasattr(self.dbapi, "InterfaceError")
#             else self.Error
#         )

#     @property
#     def DatabaseError(self):
#         """Database error class."""
#         return (
#             self.dbapi.DatabaseError
#             if hasattr(self.dbapi, "DatabaseError")
#             else self.Error
#         )

#     @property
#     def DataError(self):
#         """Data error class."""
#         return (
#             self.dbapi.DataError
#             if hasattr(self.dbapi, "DataError")
#             else self.DatabaseError
#         )

#     @property
#     def OperationalError(self):
#         """Operational error class."""
#         return (
#             self.dbapi.OperationalError
#             if hasattr(self.dbapi, "OperationalError")
#             else self.DatabaseError
#         )

#     @property
#     def IntegrityError(self):
#         """Integrity error class."""
#         return (
#             self.dbapi.IntegrityError
#             if hasattr(self.dbapi, "IntegrityError")
#             else self.DatabaseError
#         )

#     @property
#     def InternalError(self):
#         """Internal error class."""
#         return (
#             self.dbapi.InternalError
#             if hasattr(self.dbapi, "InternalError")
#             else self.DatabaseError
#         )

#     @property
#     def ProgrammingError(self):
#         """Programming error class."""
#         return (
#             self.dbapi.ProgrammingError
#             if hasattr(self.dbapi, "ProgrammingError")
#             else self.DatabaseError
#         )

#     @property
#     def NotSupportedError(self):
#         """Not supported error class."""
#         return (
#             self.dbapi.NotSupportedError
#             if hasattr(self.dbapi, "NotSupportedError")
#             else self.DatabaseError
#         )

#     async def connect(self, *args, **kwargs):
#         """Create an async connection."""
#         # Convert URL if provided as first argument
#         if args and isinstance(args[0], str):
#             conn_url = args[0]
#             connection = await self.dbapi.connect(conn_url)
#         else:
#             connection = await self.dbapi.connect(**kwargs)

#         return AsyncAdapt_pyro_mysql_connection(connection)


# class MySQLDialect_pyro_async(MySQLDialect_pyro):
#     """Asynchronous SQLAlchemy dialect for pyro-mysql."""

#     driver = "pyro_mysql_async"
#     is_async = True
#     supports_statement_cache = True

#     @classmethod
#     def import_dbapi(cls):
#         """Import and return the async DBAPI adapter."""
#         return AsyncAdapt_pyro_mysql_dbapi()

#     @classmethod
#     def get_pool_class(cls, url: URL):
#         """Return the pool class to use."""
#         # Use NullPool for async to avoid connection sharing issues
#         return NullPool

#     def create_connect_args(
#         self, url: URL, _translate_args: dict | None = None
#     ) -> tuple[tuple, dict]:
#         """Convert SQLAlchemy URL to async connection arguments."""
#         # Use the parent class method to build the URL
#         args, kwargs = super().create_connect_args(url, _translate_args)
#         return args, kwargs

#     async def _do_ping_async(
#         self, dbapi_connection: AsyncAdapt_pyro_mysql_connection
#     ) -> bool:
#         """Async version of connection ping."""
#         try:
#             await dbapi_connection.ping()
#             return True
#         except Exception:
#             return False

#     def do_ping(self, dbapi_connection: DBAPIConnection) -> bool:
#         """Check if connection is alive (async version)."""
#         return await_fallback(self._do_ping_async(dbapi_connection))


# # Register the dialects
# def register_dialects():
#     """Register pyro-mysql dialects with SQLAlchemy."""
#     try:
#         from sqlalchemy.dialects import registry

#         registry.register(
#             "mysql.pyro_mysql", "pyro_mysql.sqlalchemy", "MySQLDialect_pyro"
#         )

#         # registry.register(
#         #     "mysql.pyro_mysql_async", "pyro_mysql.sqlalchemy", "MySQLDialect_pyro_async"
#         # )
#     except ImportError:
#         # SQLAlchemy not installed or version incompatible
#         pass


# Auto-register on import
# register_dialects()
