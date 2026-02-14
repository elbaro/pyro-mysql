# Performance Tips

- Prefer MariaDB to MySQL.
- Prefer Unix socket to TCP.
- Use `BufferPool` to reuse allocations between connections.
- Use `Conn.exec_bulk_insert_or_update` to group 2~1000 `INSERT`s or `UPDATE`s.
- The async API is fast, but still far from optimal because of the GIL. Wait for Python 3.14 and mature free-threaded builds for faster asyncio performance.
- The sync API is optimized for single-thread usage. The library does not actively release the GIL during operations. When free-threaded Python becomes mature, the optimal API will be reconsidered.
