# Logging

pyro-mysql sends Rust logs to the Python logging system.

## Setup

Configure logging using Python's standard logging module:

```py
import logging

# Enable debug logging for pyro-mysql
logging.getLogger("pyro_mysql").setLevel(logging.DEBUG)

# Or configure with a handler
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
logging.getLogger("pyro_mysql").addHandler(handler)
logging.getLogger("pyro_mysql").setLevel(logging.DEBUG)
```

## Log Levels

| Level | Content |
|-------|---------|
| `DEBUG` | Query execution, connection events |
| `INFO` | Connection establishment, pool events |
| `WARNING` | Transaction auto-rollback, deprecation warnings |
| `ERROR` | Query failures, connection errors |
