"""
SQLAlchemy dialect for pyro-mysql driver.

Provides both synchronous and asynchronous dialect implementations for
integrating pyro-mysql with SQLAlchemy.
"""

from types import ModuleType
from typing import Any, cast, override

from pyro_mysql.dbapi import Error
from sqlalchemy import PoolProxiedConnection, sql, util
from sqlalchemy.dialects.mysql import types as mysql_types
from sqlalchemy.dialects.mysql.base import (
    MySQLCompiler,
    MySQLDialect,
    MySQLExecutionContext,
    MySQLIdentifierPreparer,
)
from sqlalchemy.dialects.mysql.mariadb import MariaDBDialect
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.interfaces import (
    BindTyping,
    ConnectArgsType,
    DBAPIConnection,
    DBAPICursor,
    ExecutionContext,
)
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import sqltypes


class PyroMySQLNumeric(mysql_types.NUMERIC):
    """Custom Numeric type for pyro-mysql that enables bind parameter type casting.

    MySQL protocol doesn't support sending Decimal values directly, so parameters
    are sent as strings. To ensure MySQL treats them as DECIMAL (not DOUBLE),
    we add explicit CAST expressions in the SQL.

    This is similar to asyncpg's approach with PostgreSQL's ::NUMERIC syntax,
    but uses MySQL's CAST(? AS DECIMAL) syntax instead.

    Inherits from mysql_types.NUMERIC (not sqltypes.Numeric) to ensure:
    - Compatibility with MySQL's visit_typeclause which checks for sqltypes.NUMERIC
    - MySQL-specific attributes like unsigned and zerofill are available
    - Consistent with other MySQL dialect implementations
    """

    render_bind_cast = True


class PyroMySQLCompiler(MySQLCompiler):
    """Custom compiler for pyro-mysql."""

    def visit_frame_clause(self, frameclause, **kw):
        """Override frame clause rendering to use literal execution.

        MySQL/MariaDB doesn't support bind parameters in window function
        frame clauses (ROWS BETWEEN / RANGE BETWEEN) when using server-side
        prepared statements. The numeric values must be rendered as literals.

        This is different from drivers like PyMySQL which use client-side
        parameter interpolation (pyformat/format paramstyle) where parameters
        are substituted before sending to the server.
        """
        kw["literal_execute"] = True
        return super().visit_frame_clause(frameclause, **kw)

    def render_bind_cast(self, type_, dbapi_type, sqltext):
        """Render type cast for bind parameters.

        Adds explicit CAST expressions so MySQL treats parameters as the correct type.
        For example: CAST(? AS DECIMAL(8,4))

        This ensures that arithmetic operations with DECIMAL columns return DECIMAL
        results instead of DOUBLE.

        Args:
            type_: The SQLAlchemy type
            dbapi_type: The DBAPI type for rendering
            sqltext: The SQL text (parameter placeholder like '?')

        Returns:
            String with CAST syntax applied
        """
        type_string = self.dialect.type_compiler_instance.process(
            dbapi_type, identifier_preparer=self.preparer
        )

        # MySQL uses DECIMAL in CAST expressions, not NUMERIC
        # Even though NUMERIC is valid for column definitions
        type_string = type_string.replace("NUMERIC", "DECIMAL")

        # If DECIMAL/NUMERIC has no precision/scale specified, use a reasonable default
        # to avoid MySQL's DECIMAL(10,0) default which would truncate decimal places
        if type_string == "DECIMAL" and isinstance(dbapi_type, sqltypes.Numeric):
            if dbapi_type.precision is None and dbapi_type.scale is None:
                # Use DECIMAL(65,30) as default - MySQL's max precision is 65
                # and 30 decimal places should handle most Decimal literals
                type_string = "DECIMAL(65, 30)"

        return f"CAST({sqltext} AS {type_string})"

    def _render_values(self, element, **kw):
        """Override VALUES rendering to use UNION ALL when parameters are present.

        MariaDB's prepared statement protocol doesn't properly handle parameter
        binding in VALUES clauses, so we convert to UNION ALL syntax instead.
        """
        # Check if we're using literal binds
        if element.literal_binds:
            # Use default compilation
            return super()._render_values(element, **kw)

        # Check if there's data to process
        if not element._data:
            return super()._render_values(element, **kw)

        # Convert VALUES to UNION ALL for parameter binding workaround
        # Build SELECT statements for each row
        from sqlalchemy.sql import elements

        kw.setdefault("literal_binds", element.literal_binds)

        # Process the data the same way as the parent class
        # but then convert VALUES to UNION ALL
        tuples = []
        for chunk in element._data:
            for elem in chunk:
                tuple_elem = elements.Tuple(
                    types=element._column_types, *elem
                ).self_group()
                tuples.append(tuple_elem)

        # Build SELECT statements for each tuple
        select_parts: list[str] = []
        columns = list(element.columns)

        for tuple_elem in tuples:
            # Process the tuple to get the parameter placeholders
            processed_tuple = self.process(tuple_elem, **kw)
            # Remove the parentheses from the tuple
            values_part = processed_tuple.strip("()")

            # Split the values and pair with column names
            values = values_part.split(", ")
            select_list: list[str] = []
            for value, column in zip(values, columns):
                select_list.append(f"{value} AS {self.preparer.format_column(column)}")

            select_parts.append(f"SELECT {', '.join(select_list)}")

        # Return the UNION ALL statement wrapped in parentheses
        # to match the VALUES syntax for CTEs
        return "(" + " UNION ALL ".join(select_parts) + ")"


class MySQLDialect_sync(MySQLDialect):
    """Synchronous SQLAlchemy dialect for pyro-mysql."""

    driver: str = "pyro_mysql"
    supports_unicode_statements: bool = True
    supports_sane_rowcount: bool = True
    supports_sane_multi_rowcount: bool = True
    supports_statement_cache: bool = True
    supports_server_side_cursors: bool = False  # sqlalchemy converts 1/0 to True/False
    supports_native_decimal: bool = True
    default_paramstyle: str = "qmark"
    execution_ctx_cls: type[ExecutionContext] = MySQLExecutionContext
    statement_compiler: type[MySQLCompiler] = PyroMySQLCompiler
    preparer: type[MySQLIdentifierPreparer] = MySQLIdentifierPreparer

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
    def import_dbapi(cls) -> ModuleType:
        """Import and return the DBAPI module."""
        from pyro_mysql import dbapi

        return dbapi

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
            opts = opts.capabilities(2)

        return cast(ConnectArgsType, ((opts,), {}))

    @override
    def do_ping(self, dbapi_connection: DBAPIConnection) -> bool:
        """Check if connection is alive."""
        dbapi_connection.ping()
        return True

    @override
    def _detect_charset(self, connection: Connection) -> str:
        return "utf8mb4"

    @override
    def _extract_error_code(self, exception: Exception) -> int | None:
        """Extract MySQL error code from exception."""
        # MySQL error format: "ERROR 1146 (42S02): Table 'test.asdf' doesn't exist"
        import re

        error_str = str(exception)
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

        # Check for pyro_mysql specific disconnect errors
        if isinstance(e, Error):
            return "Connection is already closed" in str(e)

        return False

    @override
    @classmethod
    def load_provisioning(cls):
        import sqlalchemy.dialects.mysql.provision


class MariaDBDialect_sync(MariaDBDialect, MySQLDialect_sync):
    # although parent classes already have this attribute, sqlalchemy test requires this
    supports_statement_cache: bool = True
    supports_native_uuid: bool = True  # mariadb supports native 128-bit UUID data type

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
        connection: Connection,
        xid: Any,
        is_prepared: bool = True,
        recover: bool = False,
    ) -> None:
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
        connection: Connection,
        xid: Any,
        is_prepared: bool = True,
        recover: bool = False,
    ) -> None:
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
    def do_begin_twophase(self, connection: Connection, xid: Any) -> None:
        connection.execute(
            sql.text("XA BEGIN :xid").bindparams(
                sql.bindparam("xid", xid, literal_execute=True)
            )
        )

    @override
    def do_prepare_twophase(self, connection: Connection, xid: Any) -> None:
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
        return MySQLDialect_sync.is_disconnect(self, e, connection, cursor)
