import sys

sys.path = [".venv/lib/python3.14/site-packages"] + sys.path

import asyncio
import pyro_mysql

HOST = "127.0.0.1"
PORT = 3306
USER = "test"
PASSWORD = "1234"
DATABASE = "test"

loop = asyncio.new_event_loop()

DATA = [
    (
        f"user_{i}",
        20 + (i % 5),
        f"user{i}@example.com",
        float(i % 10),
        f"Description for user {i}",
    )
    for i in range(10000)
]


pyro_mysql.init(worker_threads=1)


async def insert_pyro_async(n):
    conn = await pyro_mysql.AsyncConn.new(
        f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    )
    for i in range(n):
        await conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i],
        )


def insert_pyro_sync(n):
    conn = pyro_mysql.SyncConn(f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    for i in range(n):
        conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i],
        )


async def select_pyro_async(n: int, batch: int):
    conn = await pyro_mysql.AsyncConn.new(
        f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    )
    for i in range(0, n * batch, batch):
        rows = await conn.exec(
            "SELECT * FROM benchmark_test WHERE id >= ? AND id < ?",
            (i + 1, i + 1 + batch),
        )
        for row in rows:
            row.to_tuple()


def select_pyro_sync(n: int, batch: int):
    conn = pyro_mysql.SyncConn(f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    for i in range(0, n * batch, batch):
        rows = conn.exec(
            "SELECT * FROM benchmark_test WHERE id >= ? AND id < ?",
            (i + 1, i + 1 + batch),
        )
        for row in rows:
            row.to_tuple()
