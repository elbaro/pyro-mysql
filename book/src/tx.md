# Transaction

Transactions ensure a group of operations either all succeed (commit) or all fail (rollback).

```py
class Conn:
  def start_transaction(
    self,
    consistent_snapshot: bool = False,
    isolation_level: IsolationLevel | None = None,
    readonly: bool | None = None,
  ) -> Transaction: ...

class Transaction:
  def commit(self) -> None: ...
  def rollback(self) -> None: ...
```

The transactions should be entered as a context manager.
You must call `commit()` explicitly. If neither `commit()` nor `rollback()` is called, the transaction rolls back on exit.

```py
with conn.start_transaction() as tx:
    conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
    conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Bob",))
    tx.commit()
# committed
```

```py
with conn.start_transaction() as tx:
    conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
    raise ValueError("oops")
# rolled back, no data inserted
```

## Explicit Commit / Rollback

You can call `commit()` or `rollback()` explicitly inside the context manager.
After the call, the transaction object cannot be used anymore.

```py
with conn.start_transaction() as tx:
    conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
    if some_condition:
        tx.commit()
    else:
        tx.rollback()
```

## Isolation Level

```py
from pyro_mysql import IsolationLevel

with conn.start_transaction(isolation_level=IsolationLevel.Serializable) as tx:
    ...
    tx.commit()
```

| Level | Description |
|-------|-------------|
| `ReadUncommitted` | Allows dirty reads |
| `ReadCommitted` | Only sees committed data |
| `RepeatableRead` | Snapshot at transaction start (InnoDB default) |
| `Serializable` | Full serializability |

You can also create isolation levels from strings:

```py
level = IsolationLevel("READ COMMITTED")
level = IsolationLevel("repeatable_read")
level = IsolationLevel("sErIaLiZaBle")

assert level.as_str() == "SERIALIZABLE"
```

## Read-Only Transactions

Set `readonly=True` for read-only transactions. This can improve performance.

```py
with conn.start_transaction(readonly=True) as tx:
    rows = conn.query("SELECT * FROM users")
    tx.commit()
```

## Consistent Snapshot

For InnoDB, you can request a consistent snapshot at transaction start:

```py
with conn.start_transaction(consistent_snapshot=True) as tx:
    rows = conn.query("SELECT * FROM users")
    tx.commit()
```

This executes `START TRANSACTION WITH CONSISTENT SNAPSHOT`.

## Async

For async connections, use `async with` and `await`:

```py
async with conn.start_transaction() as tx:
    await conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
    await tx.commit()

# explicit rollback
async with conn.start_transaction() as tx:
    await conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
    await tx.rollback()
```
