"""
SQLAlchemy async dialect for pyro-mysql driver.

Provides asynchronous dialect implementations for integrating pyro-mysql
with SQLAlchemy's asyncio extension.

Usage:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        "mysql+pyro_mysql[async]://user:pass@host/db"
    )
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any, NoReturn

from typing_extensions import override

from sqlalchemy import util
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
    AsyncAdapt_dbapi_cursor,
    # AsyncAdapt_dbapi_module,
    AsyncAdapt_terminate,
)
from sqlalchemy.dialects.mysql.base import MySQLDialect, MySQLExecutionContext
from sqlalchemy.dialects.mysql.mariadb import MariaDBDialect
from sqlalchemy.engine.interfaces import BindTyping
from sqlalchemy.sql import sqltypes
from sqlalchemy.util import await_only

from .sqlalchemy_sync import PyroMySQLCompiler, PyroMySQLNumeric

await_ = await_only

if TYPE_CHECKING:
    from sqlalchemy.connectors.asyncio import AsyncIODBAPICursor
    from sqlalchemy.engine.interfaces import (
        ConnectArgsType,
        DBAPIConnection,
        DBAPICursor,
        DBAPIModule,
        PoolProxiedConnection,
    )
    from sqlalchemy.engine.url import URL


class AsyncAdapt_pyro_mysql_cursor(AsyncAdapt_dbapi_cursor):
    """Async adapter for pyro-mysql cursor."""

    __slots__ = ()

    def _aenter_cursor(self, cursor: AsyncIODBAPICursor) -> AsyncIODBAPICursor:
        """Override to skip __aenter__ since pyro_mysql cursors are not async context managers."""
        return cursor


class AsyncAdapt_pyro_mysql_connection(
    AsyncAdapt_terminate, AsyncAdapt_dbapi_connection
):
    """Async adapter for pyro-mysql connection."""

    __slots__ = ()

    _cursor_cls = AsyncAdapt_pyro_mysql_cursor

    @classmethod
    def _handle_exception_no_connection(cls, dbapi: Any, error: Exception) -> NoReturn:
        """Handle exceptions when no connection is available."""
        if isinstance(error, AttributeError):
            raise dbapi.InternalError(
                "network operation failed due to pyro_mysql attribute error"
            )
        raise error

    def ping(self, reconnect: bool = False) -> None:
        """Ping the connection to check if it's alive."""
        assert not reconnect
        return await_(self._do_ping())

    async def _do_ping(self) -> None:
        """Async implementation of ping."""
        try:
            await self._connection.ping()
        except Exception as error:
            self._handle_exception(error)

    def close(self) -> None:
        """Close the connection."""
        await_(self._connection.close())

    async def _terminate_graceful_close(self) -> None:
        """Gracefully close the connection during termination."""
        await self._connection.close()

    def _terminate_force_close(self) -> None:
        """Force close the connection (synchronous)."""
        # pyro_mysql close() is async-only, so we can't force close synchronously
        # The graceful close should be sufficient
        pass


class AsyncAdapt_pyro_mysql_dbapi:
    """Async adapter for pyro-mysql DBAPI module."""

    def __init__(self, pyro_mysql: ModuleType):
        # Initialize parent with driver and dbapi_module
        # pyro_mysql is the driver, and pyro_mysql.dbapi provides the exception hierarchy
        # super().__init__(driver=pyro_mysql, dbapi_module=pyro_mysql.dbapi)
        self.driver = pyro_mysql
        self.dbapi_module = pyro_mysql.dbapi
        self.pyro_mysql = pyro_mysql
        self.paramstyle = "qmark"
        self._init_dbapi_attributes()

    def _init_dbapi_attributes(self) -> None:
        """Initialize DBAPI exception attributes."""
        for name in (
            "Warning",
            "Error",
            "InterfaceError",
            "DataError",
            "DatabaseError",
            "OperationalError",
            "InterfaceError",
            "IntegrityError",
            "ProgrammingError",
            "InternalError",
            "NotSupportedError",
        ):
            setattr(self, name, getattr(self.pyro_mysql.dbapi, name))

    STRING = util.symbol("STRING")
    NUMBER = util.symbol("NUMBER")
    BINARY = util.symbol("BINARY")
    DATETIME = util.symbol("DATETIME")
    TIMESTAMP = util.symbol("TIMESTAMP")
    Binary = staticmethod(bytes)

    def connect(self, *arg: Any, **kw: Any) -> AsyncAdapt_pyro_mysql_connection:
        """Create a new async connection."""
        # Extract async_creator_fn if provided, otherwise use default
        async_creator_fn = kw.pop("async_creator_fn", None)

        async def _create_connection() -> Any:
            """
            Async function that creates the connection.
            This ensures the event loop is running when the coroutine is created,
            which is required by pyro-mysql.
            """
            if async_creator_fn is None:
                # Use the Opts object or URL string passed as the first argument
                # This is what create_connect_args returns
                from pyro_mysql.dbapi_async import connect

                if arg:
                    url_or_opts = arg[0]
                    # Create the async connection using dbapi_async.connect
                    # This returns a Connection object with cursor(), commit(), rollback()
                    # url_or_opts can be either an Opts object or a URL string
                    return await connect(url_or_opts)
                else:
                    raise self.InterfaceError("No connection options provided")
            else:
                # Use the provided creator function
                return await async_creator_fn(*arg, **kw)

        # Call the awaitable creator function through await_
        return AsyncAdapt_pyro_mysql_connection(self, await_(_create_connection()))


class MySQLDialect_async(MySQLDialect):
    """Asynchronous SQLAlchemy dialect for pyro-mysql with MySQL."""

    driver: str = "pyro_mysql_async"
    supports_unicode_statements: bool = True
    supports_sane_rowcount: bool = True
    supports_sane_multi_rowcount: bool = True
    supports_statement_cache: bool = True
    supports_server_side_cursors: bool = False
    supports_native_decimal: bool = True
    default_paramstyle: str = "qmark"
    execution_ctx_cls = MySQLExecutionContext
    statement_compiler = PyroMySQLCompiler

    is_async: bool = True
    has_terminate: bool = True

    # Enable bind parameter type casting to ensure MySQL treats DECIMAL parameters
    # correctly and doesn't convert results to DOUBLE
    bind_typing = BindTyping.RENDER_CASTS

    # Map Numeric type to our custom PyroMySQLNumeric with render_bind_cast support
    colspecs = util.update_copy(
        MySQLDialect.colspecs,
        {
            sqltypes.Numeric: PyroMySQLNumeric,
        },
    )

    @override
    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """Import and return the async DBAPI module."""
        import pyro_mysql

        return AsyncAdapt_pyro_mysql_dbapi(
            pyro_mysql
        )  # pyright: ignore [reportReturnType]

    @override
    def create_connect_args(self, url: URL) -> ConnectArgsType:
        """Convert SQLAlchemy URL to connection arguments for pyro-mysql."""
        from pyro_mysql import Opts

        opts = Opts()

        if url.host:
            opts = opts.host(url.host)
        if url.port:
            opts = opts.port(url.port)
        if url.username:
            opts = opts.user(url.username)
        if url.password:
            opts = opts.password(url.password)
        if url.database:
            opts = opts.db(url.database)

        # Handle query parameters
        query = dict(url.query)
        if "capabilities" in query:
            caps = query.pop("capabilities")
            if isinstance(caps, str):
                opts = opts.capabilities(int(caps))
        else:
            # Default capabilities for compatibility with other mysql dialects
            # 2 = CLIENT_FOUND_ROWS: return matched rows instead of changed rows
            opts = opts.capabilities(2)

        return ((opts,), {})

    @override
    def do_terminate(self, dbapi_connection: DBAPIConnection) -> None:
        """Terminate a connection."""
        dbapi_connection.terminate()

    @override
    def do_ping(self, dbapi_connection: DBAPIConnection) -> bool:
        """Check if connection is alive."""
        dbapi_connection.ping()
        return True

    @override
    def _detect_charset(self, connection: Any) -> str:
        return "utf8mb4"

    @override
    def _extract_error_code(self, exception: Exception) -> int | None:
        """Extract MySQL error code from exception."""
        import re

        error_str = str(exception)
        # Match pattern: ERROR SQLSTATE (error_code): message
        # Example: ERROR 42S02 (1146): Table 'test.t' doesn't exist
        match = re.search(r"ERROR\s+(\d+)\s+\([^)]+\):", error_str)
        if match:
            return int(match.group(1))
        return None

    @override
    def is_disconnect(
        self,
        e: Exception,
        connection: PoolProxiedConnection | DBAPIConnection | None,
        cursor: DBAPICursor | None,
    ) -> bool:
        """Check if an exception indicates a disconnect."""
        if super().is_disconnect(e, connection, cursor):
            return True

        from pyro_mysql.dbapi import Error

        if isinstance(e, Error):
            str_e = str(e).lower()
            return (
                "not connected" in str_e
                or "network operation failed" in str_e
                or "connection is already closed" in str_e
            )

        return False

    @override
    @classmethod
    def load_provisioning(cls):
        import sqlalchemy.dialects.mysql.provision

    @classmethod
    def get_dialect_pool_class(cls, url: URL) -> Any:
        """Return the async-compatible pool class."""
        from sqlalchemy.pool import AsyncAdaptedQueuePool

        return AsyncAdaptedQueuePool

    def get_driver_connection(self, connection: DBAPIConnection) -> Any:
        """Get the underlying driver connection object."""
        return connection._connection  # type: ignore[no-any-return]


class MariaDBDialect_async(MariaDBDialect, MySQLDialect_async):
    """Asynchronous SQLAlchemy dialect for pyro-mysql with MariaDB."""

    # Required by SQLAlchemy test suite
    supports_statement_cache: bool = True
    supports_native_uuid: bool = True  # MariaDB supports native 128-bit UUID data type

    is_async: bool = True
    has_terminate: bool = True

    # Override colspecs to ensure our PyroMySQLNumeric is used (MariaDBDialect has its own colspecs)
    colspecs = util.update_copy(
        MariaDBDialect.colspecs,
        {
            sqltypes.Numeric: PyroMySQLNumeric,
        },
    )

    # MariaDB does not support parameter in 'XA BEGIN ?'
    @override
    def do_commit_twophase(
        self,
        connection: Any,
        xid: Any,
        is_prepared: bool = True,
        recover: bool = False,
    ) -> None:
        from sqlalchemy import sql

        if not is_prepared:
            self.do_prepare_twophase(connection, xid)
        connection.execute(
            sql.text("XA COMMIT :xid").bindparams(
                sql.bindparam("xid", xid, literal_execute=True)
            )
        )

    @override
    def do_rollback_twophase(
        self,
        connection: Any,
        xid: Any,
        is_prepared: bool = True,
        recover: bool = False,
    ) -> None:
        from sqlalchemy import sql

        if not is_prepared:
            connection.execute(
                sql.text("XA END :xid").bindparams(
                    sql.bindparam("xid", xid, literal_execute=True)
                )
            )
        connection.execute(
            sql.text("XA ROLLBACK :xid").bindparams(
                sql.bindparam("xid", xid, literal_execute=True)
            )
        )

    @override
    def do_begin_twophase(self, connection: Any, xid: Any) -> None:
        from sqlalchemy import sql

        connection.execute(
            sql.text("XA BEGIN :xid").bindparams(
                sql.bindparam("xid", xid, literal_execute=True)
            )
        )

    @override
    def do_prepare_twophase(self, connection: Any, xid: Any) -> None:
        from sqlalchemy import sql

        connection.execute(
            sql.text("XA END :xid").bindparams(
                sql.bindparam("xid", xid, literal_execute=True)
            )
        )
        connection.execute(
            sql.text("XA PREPARE :xid").bindparams(
                sql.bindparam("xid", xid, literal_execute=True)
            )
        )

    @override
    def is_disconnect(
        self,
        e: Exception,
        connection: PoolProxiedConnection | DBAPIConnection | None,
        cursor: DBAPICursor | None,
    ) -> bool:
        """Check if an exception indicates a disconnect."""
        return MySQLDialect_async.is_disconnect(self, e, connection, cursor)


dialect = MySQLDialect_async
