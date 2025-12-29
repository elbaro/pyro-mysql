# Introduction

pyro-mysql is a high-performance MySQL driver for Python, backed by Rust.

```bash
pip install pyro-mysql
```

## Quick Start

```py
from pyro_mysql.sync import Conn

conn = Conn("mysql://user:password@localhost/mydb")

# Simple query
rows = conn.query("SELECT id, name FROM users")

# Parameterized query
user = conn.exec_first("SELECT * FROM users WHERE id = ?", (42,))

# Transaction
with conn.start_transaction() as tx:
    conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Alice",))
    conn.exec_drop("INSERT INTO users (name) VALUES (?)", ("Bob",))
    tx.commit()
```

## Features

- **High Performance**: Minimal allocations and copies
- **Sync and Async**: The library provides both sync and async APIs
- **Binary Protocol**: Prepared statements with automatic caching
- **MariaDB Bulk Execution**: Single round-trip bulk operations

## Limitations

- **No Streaming**: All results are fetched into memory
- **Limited Performance Gain in Async API**: Due to the overhead of Python GIL, the async module pays a significant cost switching between Python thread and Rust thread. The async performance has a potential to be much faster with Python 3.14 free-threaded builds.
