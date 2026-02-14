# Connection

A connection can be made with an URL string or `Opts`.

The URL format is:

```
mysql://[user[:password]@]host[:port][/database][?tls=true&compress=true]
```

The URL `mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?tls=true&compress=true` is equivalent to:

```py
Opts()
  .user('USER')
  .password('PASSWORD')
  .host('HOST')
  .port(PORT)
  .db('DATABASE')
```

For the full list of options, see the [type stub](https://github.com/elbaro/pyro-mysql/blob/main/pyro_mysql/__init__.pyi).

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
