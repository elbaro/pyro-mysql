import sys

sys.path = [".venv/lib/python3.14/site-packages"] + sys.path

import asyncio

import aiomysql
import asyncmy
import MySQLdb
import pymysql
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

# Create a reusable buffer for better performance
buffer = pyro_mysql.BufferObj.new()


async def insert_pyro_async(n):
    conn = await pyro_mysql.AsyncConn.new(
        f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", buffer
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


async def insert_async(connect_fn, n: int):
    conn = await connect_fn(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        db=DATABASE,
        autocommit=True,
    )

    async with conn.cursor() as cursor:
        for i in range(n):
            await cursor.execute(
                """INSERT INTO benchmark_test (name, age, email, score, description) 
                    VALUES (%s, %s, %s, %s, %s)""",
                DATA[i],
            )
        await cursor.close()
    await conn.ensure_closed()


def insert_sync(connect_fn, n: int):
    conn = connect_fn(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        autocommit=True,
    )

    cursor = conn.cursor()
    for i in range(n):
        cursor.execute(
            """INSERT INTO benchmark_test (name, age, email, score, description) 
                VALUES (%s, %s, %s, %s, %s)""",
            DATA[i],
        )
    cursor.close()
    conn.close()


# ─── Select ───────────────────────────────────────────────────────────────────


async def select_pyro_async(n: int, batch: int):
    conn = await pyro_mysql.AsyncConn.new(
        f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", buffer
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


async def select_async(connect_fn, n: int, batch: int):
    conn = await connect_fn(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        db=DATABASE,
        autocommit=True,
    )

    async with conn.cursor() as cursor:
        for i in range(0, n * batch, batch):
            await cursor.execute(
                "SELECT * FROM benchmark_test WHERE id >= %s AND id < %s",
                (i + 1, i + 1 + batch),
            )
            await cursor.fetchall()
        await cursor.close()
    await conn.ensure_closed()


def select_sync(connect_fn, n: int, batch: int):
    conn = connect_fn(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        autocommit=True,
    )

    cursor = conn.cursor()
    for i in range(0, n * batch, batch):
        cursor.execute(
            "SELECT * FROM benchmark_test WHERE id >= %s AND id < %s",
            (i + 1, i + 1 + batch),
        )
        cursor.fetchall()
    cursor.close()
    conn.close()


# ─── Long Text Select ─────────────────────────────────────────────────────────


async def select_long_text_pyro_async(n: int, batch: int):
    conn = await pyro_mysql.AsyncConn.new(
        f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", buffer
    )
    for i in range(0, n * batch, batch):
        rows = await conn.exec(
            "SELECT * FROM long_text_test WHERE id >= ? AND id < ?",
            (i + 1, i + 1 + batch),
        )
        for row in rows:
            row.to_tuple()


def select_long_text_pyro_sync(n: int, batch: int):
    conn = pyro_mysql.SyncConn(f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    for i in range(0, n * batch, batch):
        rows = conn.exec(
            "SELECT * FROM long_text_test WHERE id >= ? AND id < ?",
            (i + 1, i + 1 + batch),
        )
        for row in rows:
            row.to_tuple()


async def select_long_text_async(connect_fn, n: int, batch: int):
    conn = await connect_fn(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        db=DATABASE,
        autocommit=True,
    )

    async with conn.cursor() as cursor:
        for i in range(0, n * batch, batch):
            await cursor.execute(
                "SELECT * FROM long_text_test WHERE id >= %s AND id < %s",
                (i + 1, i + 1 + batch),
            )
            await cursor.fetchall()
        await cursor.close()
    await conn.ensure_closed()


def select_long_text_sync(connect_fn, n: int, batch: int):
    conn = connect_fn(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        autocommit=True,
    )

    cursor = conn.cursor()
    for i in range(0, n * batch, batch):
        cursor.execute(
            "SELECT * FROM long_text_test WHERE id >= %s AND id < %s",
            (i + 1, i + 1 + batch),
        )
        cursor.fetchall()
    cursor.close()
    conn.close()
