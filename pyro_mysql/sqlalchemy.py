"""
SQLAlchemy dialect for pyro-mysql driver.

Provides both synchronous and asynchronous dialect implementations for
integrating pyro-mysql with SQLAlchemy.
"""

from typing import Any, cast, override

from sqlalchemy import sql
from sqlalchemy.dialects.mysql.base import (
    MySQLCompiler,
    MySQLDialect,
    MySQLExecutionContext,
    MySQLIdentifierPreparer,
)
from sqlalchemy.dialects.mysql.mariadb import MariaDBDialect
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.interfaces import (
    ConnectArgsType,
    DBAPIConnection,
    DBAPIModule,
    ExecutionContext,
)
from sqlalchemy.engine.url import URL

from pyro_mysql.dbapi import Error


class MySQLDialect_pyro(MySQLDialect):
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
    statement_compiler: type[MySQLCompiler] = MySQLCompiler
    preparer: type[MySQLIdentifierPreparer] = MySQLIdentifierPreparer

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

        if capabilities := dic.get("capabilities"):
            del dic["capabilities"]
        else:
            capabilities = 2  # for compatibility with other mysql dialects

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
            opts = (
                OptsBuilder.from_map(str_dic)
                .additional_capabilities(capabilities)
                .build()
            )
        except Exception as e:
            raise Error("wrong connection argument") from e

        return cast(ConnectArgsType, ((opts,), {}))

    @override
    def do_ping(self, dbapi_connection: DBAPIConnection) -> bool:
        """Check if connection is alive."""
        try:
            return dbapi_connection.ping()
        except Exception:
            return False

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
        connection: DBAPIConnection | None,
        cursor: Any | None,
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


class MariaDBDialect_pyro(MariaDBDialect, MySQLDialect_pyro):
    # although parent classes already have this attribute, sqlalchemy test requires this
    supports_statement_cache: bool = True
    supports_native_uuid: bool = True  # mariadb supports native 128-bit UUID data type

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
        connection: DBAPIConnection | None,
        cursor: Any | None,
    ) -> bool:
        return MySQLDialect_pyro.is_disconnect(self, e, connection, cursor)
