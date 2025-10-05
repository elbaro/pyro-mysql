import datetime
import decimal
import time
from collections.abc import Sequence

from pyro_mysql import (
    CapabilityFlags,
    IsolationLevel,
    PyroFuture,
    Row,
)

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
