"""
pyro_mysql - High-performance MySQL driver for Python, written in Rust.

- pyro_mysql.sync: The synchronous API
- pyro_mysql.async_: The asynchronous API
- pyro_mysql.error: Exceptions
"""

import datetime
import decimal
import time
from collections.abc import Generator, Sequence
from typing import Any, Awaitable, TypeVar

from . import async_, sync
from . import dbapi as dbapi
from . import error as error

def init(worker_threads: int | None = 1, thread_name: str | None = None) -> None:
    """
    Initialize the Tokio runtime for async operations.
    This function can be called multiple times until Any async operation is called.

    Args:
        worker_threads: Number of worker threads for the Tokio runtime. If None, set to the number of CPUs.
        thread_name: Name prefix for worker threads.
    """
    ...

# Compatibility aliases for backward compatibility
AsyncConn = async_.Conn
AsyncTransaction = async_.Transaction

SyncConn = sync.Conn
SyncTransaction = sync.Transaction

class Opts:
    """
    Connection options for MySQL connections.

    This class provides a builder API for configuring MySQL connection parameters.
    Methods can be chained to configure multiple options.

    Examples:
        # Create from URL
        opts = Opts("mysql://user:pass@localhost:3306/mydb")

        # Create with builder pattern
        opts = Opts().host("localhost").port(3306).user("root").password("secret").db("mydb")
    """

    def __new__(cls, url: str | None = None) -> "Opts":
        """
        Create a new Opts instance.

        Args:
            url: Optional MySQL connection URL. If provided, parses the URL.
                 If not provided, creates default opts.
        """
        ...

    def host(self, hostname: str) -> "Opts":
        """Set the hostname or IP address."""
        ...

    def port(self, port: int) -> "Opts":
        """Set the TCP port number."""
        ...

    def socket(self, path: str | None) -> "Opts":
        """Set the Unix socket path for local connections."""
        ...

    def user(self, username: str) -> "Opts":
        """Set the username for authentication."""
        ...

    def password(self, password: str | None) -> "Opts":
        """Set the password for authentication."""
        ...

    def db(self, database: str | None) -> "Opts":
        """Set the database name to connect to."""
        ...

    def tcp_nodelay(self, enable: bool) -> "Opts":
        """Enable or disable TCP_NODELAY socket option."""
        ...

    def compress(self, enable: bool) -> "Opts":
        """Enable or disable compression for the connection."""
        ...

    def capabilities(self, capabilities: int) -> "Opts":
        """Set MySQL client capability flags."""
        ...

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

"""
Parameters that can be passed to query execution methods:
- `None`: No parameters
- `tuple[Value, ...]`: Positional parameters for queries with ? placeholders
- `list[Value]`: List of parameters for queries with ? placeholders

Examples:
No parameters:

    `await conn.exec("SELECT * FROM users")`

Positional parameters:

    `await conn.exec("SELECT * FROM users WHERE id = ?", (123,))`

Multiple positional parameters:

    `await conn.exec("SELECT * FROM users WHERE age > ? AND city = ?", (18, "NYC"))`
"""
type Params = None | tuple[Value, ...] | Sequence[Value]

T = TypeVar("T")

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

    CLIENT_LONG_PASSWORD: int
    CLIENT_FOUND_ROWS: int
    CLIENT_LONG_FLAG: int
    CLIENT_CONNECT_WITH_DB: int
    CLIENT_NO_SCHEMA: int
    CLIENT_COMPRESS: int
    CLIENT_ODBC: int
    CLIENT_LOCAL_FILES: int
    CLIENT_IGNORE_SPACE: int
    CLIENT_PROTOCOL_41: int
    CLIENT_INTERACTIVE: int
    CLIENT_SSL: int
    CLIENT_IGNORE_SIGPIPE: int
    CLIENT_TRANSACTIONS: int
    CLIENT_RESERVED: int
    CLIENT_SECURE_CONNECTION: int
    CLIENT_MULTI_STATEMENTS: int
    CLIENT_MULTI_RESULTS: int
    CLIENT_PS_MULTI_RESULTS: int
    CLIENT_PLUGIN_AUTH: int
    CLIENT_CONNECT_ATTRS: int
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: int
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: int
    CLIENT_SESSION_TRACK: int
    CLIENT_DEPRECATE_EOF: int
    CLIENT_OPTIONAL_RESULTSET_METADATA: int
    CLIENT_ZSTD_COMPRESSION_ALGORITHM: int
    CLIENT_QUERY_ATTRIBUTES: int
    MULTI_FACTOR_AUTHENTICATION: int
    CLIENT_PROGRESS_OBSOLETE: int
    CLIENT_SSL_VERIFY_SERVER_CERT: int
    CLIENT_REMEMBER_OPTIONS: int
