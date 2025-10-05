import datetime
import decimal
import time
from collections.abc import Awaitable, Generator
from typing import Any, Sequence, TypeVar

T = TypeVar("T")

JsonEncodable = (
    dict[str, "JsonEncodable"] | list["JsonEncodable"] | str | int | float | bool | None
)
type Value = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | bytearray
    | tuple[JsonEncodable, ...]
    | list[JsonEncodable]
    | set[JsonEncodable]
    | frozenset[JsonEncodable]
    | dict[str, JsonEncodable]
    | datetime.datetime
    | datetime.date
    | datetime.time
    | datetime.timedelta
    | time.struct_time
    | decimal.Decimal
)
type Params = (None | tuple[Value, ...] | Sequence[Value] | dict[str, Value])

class PyroFuture(Awaitable[T]):
    def __await__(self) -> Generator[Any, Any, T]: ...
    def cancel(self) -> bool: ...
    def get_loop(self): ...

class IsolationLevel:
    """Transaction isolation level enum."""

    ReadUncommitted: "IsolationLevel"
    ReadCommitted: "IsolationLevel"
    RepeatableRead: "IsolationLevel"
    Serializable: "IsolationLevel"

    def as_str(self) -> str:
        """Return the isolation level as a string."""
        ...

class CapabilityFlags:
    """MySQL capability flags for client connections."""

    CLIENT_LONG_PASSWORD: int = 0x00000001
    CLIENT_FOUND_ROWS: int = 0x00000002
    CLIENT_LONG_FLAG: int = 0x00000004
    CLIENT_CONNECT_WITH_DB: int = 0x00000008
    CLIENT_NO_SCHEMA: int = 0x00000010
    CLIENT_COMPRESS: int = 0x00000020
    CLIENT_ODBC: int = 0x00000040
    CLIENT_LOCAL_FILES: int = 0x00000080
    CLIENT_IGNORE_SPACE: int = 0x00000100
    CLIENT_PROTOCOL_41: int = 0x00000200
    CLIENT_INTERACTIVE: int = 0x00000400
    CLIENT_SSL: int = 0x00000800
    CLIENT_IGNORE_SIGPIPE: int = 0x00001000
    CLIENT_TRANSACTIONS: int = 0x00002000
    CLIENT_RESERVED: int = 0x00004000
    CLIENT_SECURE_CONNECTION: int = 0x00008000
    CLIENT_MULTI_STATEMENTS: int = 0x00010000
    CLIENT_MULTI_RESULTS: int = 0x00020000
    CLIENT_PS_MULTI_RESULTS: int = 0x00040000
    CLIENT_PLUGIN_AUTH: int = 0x00080000
    CLIENT_CONNECT_ATTRS: int = 0x00100000
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: int = 0x00200000
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: int = 0x00400000
    CLIENT_SESSION_TRACK: int = 0x00800000
    CLIENT_DEPRECATE_EOF: int = 0x01000000
    CLIENT_OPTIONAL_RESULTSET_METADATA: int = 0x02000000
    CLIENT_ZSTD_COMPRESSION_ALGORITHM: int = 0x04000000
    CLIENT_QUERY_ATTRIBUTES: int = 0x08000000
    MULTI_FACTOR_AUTHENTICATION: int = 0x10000000
    CLIENT_PROGRESS_OBSOLETE: int = 0x20000000
    CLIENT_SSL_VERIFY_SERVER_CERT: int = 0x40000000
    CLIENT_REMEMBER_OPTIONS: int = 0x80000000

class Row:
    """
    A row returned from a MySQL query.
    to_tuple() / to_dict() copies the data, and should not be called many times.
    """

    def to_tuple(self) -> tuple[Value, ...]:
        """Convert the row to a Python list."""
        ...

    def to_dict(self) -> dict[str, Value]:
        f"""
        Convert the row to a Python dictionary with column names as keys.
        If there are multiple columns with the same name, a later column wins.

            row = await conn.exec_first("SELECT 1, 2, 2 FROM some_table")
            assert row.as_dict() == {"1": 1, "2": 2}
        """
        ...
