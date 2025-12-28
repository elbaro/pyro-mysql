# Connection

A connection can be made with a URL string or `Opts`.

The URL format is:

```
mysql://[user[:password]@]host[:port][/database]
```

The URL `mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}` is equivalent to:

```py
Opts()
  .user('USER')
  .password('PASSWORD')
  .host('HOST')
  .port(PORT)
  .db('DATABASE')
```

### Example: basic

```py
from pyro_mysql.sync import Conn
from pyro_mysql import Opts

# url
conn1 = Conn("mysql://test:1234@localhost:3306/test_db")

# url + Opts
conn2 = Conn(Opts("mysql://test@localhost").tcp_nodelay(True))

# Opts
conn3 = Conn(
    Opts()
        .socket("/tmp/mysql.sock")
        .user("root")
        .db("test_db")
)
```

### Example: async

```py
from pyro_mysql.async_ import Conn
from pyro_mysql import Opts

conn = await Conn.new("mysql://test:1234@localhost:3306/test_db")
```

### Example: unix socket

```py
from pyro_mysql.sync import Conn

# hostname 'localhost' is ignored when socket is set
conn = Conn(Opts().socket("/var/run/mysqld/mysqld.sock").db("test"))
```

## Connection Options

| Method | Description | Default |
|--------|-------------|---------|
| `host(str)` | Hostname or IP address | `"127.0.0.1"` |
| `port(int)` | TCP port number | `3306` |
| `socket(str)` | Unix socket path | `None` |
| `user(str)` | Username | `""` |
| `password(str)` | Password | `""` |
| `db(str)` | Database name | `None` |
| `tcp_nodelay(bool)` | Disable Nagle's algorithm | `True` |
| `compress(bool)` | Enable compression | `False` |
| `tls(bool)` | Enable TLS | `False` |
| `init_command(str)` | SQL to execute on connect | `None` |
| `buffer_pool(BufferPool)` | Custom buffer pool | global pool |

## Advanced: Upgrade to Unix Socket

By default, `upgrade_to_unix_socket` is `True`.

If the connection is made via TCP to localhost, the driver queries `SELECT @@socket` to get the Unix socket path, then reconnects using the socket for better performance.

```py
conn = Conn("mysql://test:1234@localhost")
# If localhost, conn may be a Unix socket connection
```

For production, disable this flag and use TCP or manually specify the socket address:

```py
conn = Conn(Opts("mysql://test:1234@localhost").upgrade_to_unix_socket(False))
```
