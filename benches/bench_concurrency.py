import asyncio
import sys

import aiomysql
import asyncmy

import pyro_mysql

HOST = "127.0.0.1"
PORT = 3306
USER = "test"
PASSWORD = "1234"
DATABASE = "test"

loop = asyncio.new_event_loop()

pyro_mysql.init(worker_threads=1)


# ─── Concurrent Select ────────────────────────────────────────────────────────


async def concurrent_select_pyro_async(concurrency: int):
    """Run concurrent queries using multiple async connections."""
    batch = 10
    total_queries = 100  # Process ~1000 rows total
    queries_per_conn = total_queries // concurrency

    async def worker(conn_id: int):
        conn = await pyro_mysql.AsyncConn.new(
            f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        )
        for i in range(queries_per_conn):
            offset = (conn_id * queries_per_conn + i) * batch
            rows = await conn.exec(
                "SELECT * FROM benchmark_test WHERE id >= ? AND id < ?",
                (offset + 1, offset + 1 + batch),
            )
            for row in rows:
                row.to_tuple()
        await conn.close()

    await asyncio.gather(*[worker(i) for i in range(concurrency)])


async def concurrent_select_async(connect_fn, concurrency: int):
    """Run concurrent queries using multiple async connections."""
    batch = 10
    total_queries = 100  # Process ~1000 rows total
    queries_per_conn = total_queries // concurrency

    async def worker(conn_id: int):
        conn = await connect_fn(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            db=DATABASE,
            autocommit=True,
        )
        async with conn.cursor() as cursor:
            for i in range(queries_per_conn):
                offset = (conn_id * queries_per_conn + i) * batch
                await cursor.execute(
                    "SELECT * FROM benchmark_test WHERE id >= %s AND id < %s",
                    (offset + 1, offset + 1 + batch),
                )
                await cursor.fetchall()
            await cursor.close()
        await conn.ensure_closed()

    await asyncio.gather(*[worker(i) for i in range(concurrency)])


# ─── Concurrent Insert ────────────────────────────────────────────────────────


async def concurrent_insert_pyro_async(concurrency: int):
    """Run concurrent inserts using multiple async connections."""
    batch = 10
    total_inserts = 100  # Process ~1000 inserts total
    inserts_per_conn = total_inserts // concurrency

    async def worker(conn_id: int):
        conn = await pyro_mysql.AsyncConn.new(
            f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        )
        for i in range(inserts_per_conn):
            base_idx = conn_id * inserts_per_conn * batch + i * batch
            for j in range(batch):
                idx = base_idx + j
                await conn.exec(
                    "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
                    (
                        f"user_{idx}",
                        20 + (idx % 50),
                        f"user{idx}@example.com",
                        float(idx % 100),
                        f"User description {idx}",
                    ),
                )
        await conn.close()

    await asyncio.gather(*[worker(i) for i in range(concurrency)])


async def concurrent_insert_async(connect_fn, concurrency: int):
    """Run concurrent inserts using multiple async connections."""
    batch = 10
    total_inserts = 100  # Process ~1000 inserts total
    inserts_per_conn = total_inserts // concurrency

    async def worker(conn_id: int):
        conn = await connect_fn(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            db=DATABASE,
            autocommit=True,
        )
        async with conn.cursor() as cursor:
            for i in range(inserts_per_conn):
                base_idx = conn_id * inserts_per_conn * batch + i * batch
                for j in range(batch):
                    idx = base_idx + j
                    await cursor.execute(
                        "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (%s, %s, %s, %s, %s)",
                        (
                            f"user_{idx}",
                            20 + (idx % 50),
                            f"user{idx}@example.com",
                            float(idx % 100),
                            f"User description {idx}",
                        ),
                    )
            await cursor.close()
        await conn.ensure_closed()

    await asyncio.gather(*[worker(i) for i in range(concurrency)])
