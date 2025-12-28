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

## Context Manager

The recommended way to use transactions is with a context manager.
If neither `commit()` nor `rollback()` is called before exit, the transaction is rolled back automatically.

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
# auto-rolled back, no data inserted
```

## Explicit Commit / Rollback

You can call `commit()` or `rollback()` inside the context manager:

```py
with conn.start_transaction() as tx:
    conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
    if some_condition:
        tx.commit()
    else:
        tx.rollback()
```

## Isolation Level

MySQL supports four isolation levels. Pass `isolation_level` to `start_transaction()`.

```py
from pyro_mysql import IsolationLevel

with conn.start_transaction(isolation_level=IsolationLevel.Serializable) as tx:
    ...
    tx.commit()
```

| Level | Description |
|-------|-------------|
| `ReadUncommitted` | Allows dirty reads |
| `ReadCommitted` | Default. Only sees committed data |
| `RepeatableRead` | Snapshot at transaction start (InnoDB default) |
| `Serializable` | Full serializability |

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

## Combining Options

You can combine isolation level, readonly, and consistent snapshot:

```py
with conn.start_transaction(
    isolation_level=IsolationLevel.RepeatableRead,
    readonly=True,
    consistent_snapshot=True
) as tx:
    ...
    tx.commit()
```

## Async

For async connections, use `async with` and `await`:

```py
async with conn.start_transaction() as tx:
    await conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
    await tx.commit()

# explicit commit/rollback
async with conn.start_transaction() as tx:
    await conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
    await tx.rollback()
```
