# Logging

pyro_mysql uses Python's standard `logging` module. The logs from Rust code are automatically bridged to Python logging when the module is imported.

The logger name is `pyro_mysql`.

## Basic Setup

```py
import logging
from pyro_mysql.sync import Conn

logging.basicConfig(level=logging.DEBUG)
conn = Conn("mysql://test:1234@localhost:3306/test_db")  # logs will appear
```

```py
import logging

logger = logging.getLogger("pyro_mysql")
```
