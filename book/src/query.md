# Query

There are two sets of query API: Text Protocol and Binary Protocol.

## Text Protocol

Text protocol is simple and supports multiple statements separated by `;`, but does not support parameter binding.

```py
class Conn:
  def query(self, sql: str, *, as_dict: bool = False) -> list[tuple] | list[dict]: ...
  def query_first(self, sql: str, *, as_dict: bool = False) -> tuple | dict | None: ...
  def query_drop(self, sql: str) -> None: ...
```

- `query`: executes `sql` and returns the list of rows
- `query_first`: executes `sql` and returns the first row (or None)
- `query_drop`: executes `sql` and discards the result

### Example

```py
rows = conn.query("SELECT field1, field2 FROM users")
row = conn.query_first("SELECT * FROM users WHERE id = 1")
conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")
```

## Binary Protocol

Binary protocol uses prepared statements with parameter binding. Use `?` as the placeholder.

```py
class Conn:
  def exec(self, query: str, params = (), *, as_dict: bool = False) -> list[tuple] | list[dict]: ...
  def exec_first(self, query: str, params = (), *, as_dict: bool = False) -> tuple | dict | None: ...
  def exec_drop(self, query: str, params = ()) -> None: ...
  def exec_batch(self, query: str, params_list = []) -> None: ...
  def exec_bulk(self, query: str, params_list = [], *, as_dict: bool = False) -> list[tuple] | list[dict]: ...
```

- `exec`: execute a prepared statement and return the list of rows
- `exec_first`: execute a prepared statement and return the first row (or None)
- `exec_drop`: execute a prepared statement and discard the result
- `exec_batch`: execute a prepared statement multiple times with different parameters
- `exec_bulk`: execute a prepared statement with bulk parameters (MariaDB only, single round trip)

### Example: basic

```py
# One-off query with parameters
row = conn.exec_first("SELECT * FROM users WHERE id = ?", (300,))

# Multiple queries with the same prepared statement (automatically cached)
for user_id in [100, 200, 300]:
    row = conn.exec_first("SELECT * FROM users WHERE id = ?", (user_id,))
```

### Example: batch execution

For executing many similar statements (e.g., bulk INSERT):

```py
conn.exec_batch("INSERT INTO users (age, name) VALUES (?, ?)", [
    (20, "Alice"),
    (21, "Bob"),
    (22, "Charlie"),
])
```

### Example: bulk execution (MariaDB)

MariaDB supports bulk execution which sends all parameters in a single packet:

```py
conn.exec_bulk("INSERT INTO users (age, name) VALUES (?, ?)", [
    (20, "Alice"),
    (21, "Bob"),
    (22, "Charlie"),
])
```

## Statement Caching

Prepared statements are automatically cached per connection. The first `exec*` call with a query string prepares the statement, and subsequent calls reuse it.

```py
# First call: prepares and executes
conn.exec("SELECT * FROM users WHERE id = ?", (1,))

# Second call: reuses prepared statement
conn.exec("SELECT * FROM users WHERE id = ?", (2,))
```

## Result Format

By default, rows are returned as tuples. Use `as_dict=True` to get dictionaries with column names as keys:

```py
# As tuples (default)
rows = conn.query("SELECT id, name FROM users")
# [(1, 'Alice'), (2, 'Bob')]

# As dictionaries
rows = conn.query("SELECT id, name FROM users", as_dict=True)
# [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
```

## Async

For async connections, use `await`:

```py
rows = await conn.query("SELECT * FROM users")
row = await conn.exec_first("SELECT * FROM users WHERE id = ?", (1,))
await conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
```
