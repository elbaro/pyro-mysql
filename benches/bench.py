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
asyncio.set_event_loop(loop)

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


# ─── Connection Setup Helpers ─────────────────────────────────────────────────


async def create_pyro_async_conn():
    return await pyro_mysql.AsyncConn.new(
        "mysql://test:1234@127.0.0.1:3306/test"
    )


async def create_pyro_wtx_conn():
    return await pyro_mysql.AsyncConn.new_wtx(
        "mysql://test:1234@127.0.0.1:3306/test",
        max_statements=32,
        buffer_size=(512, 512, 8192, 512, 32),
    )


async def create_asyncmy_conn():
    return await asyncmy.connect(
        host="127.0.0.1", port=3306, user="test", password="1234", db="test", autocommit=True
    )


async def create_aiomysql_conn():
    return await aiomysql.connect(
        host="127.0.0.1", port=3306, user="test", password="1234", db="test", autocommit=True
    )


# ─── Insert ───────────────────────────────────────────────────────────────────


async def insert_pyro_async(conn, n):
    for i in range(n):
        await conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i],
        )


async def insert_pyro_wtx(conn, n):
    for i in range(n):
        await conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i],
        )


def insert_pyro_sync(conn, n):
    for i in range(n):
        conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i],
        )


async def insert_async(conn, n: int):
    async with conn.cursor() as cursor:
        for i in range(n):
            await cursor.execute(
                """INSERT INTO benchmark_test (name, age, email, score, description)
                    VALUES (%s, %s, %s, %s, %s)""",
                DATA[i],
            )
        await cursor.close()


def insert_sync(conn, n: int):
    cursor = conn.cursor()
    for i in range(n):
        cursor.execute(
            """INSERT INTO benchmark_test (name, age, email, score, description)
                VALUES (%s, %s, %s, %s, %s)""",
            DATA[i],
        )
    cursor.close()


# ─── Select ───────────────────────────────────────────────────────────────────


async def select_pyro_async(conn):
    rows = await conn.exec("SELECT * FROM benchmark_test")
    for row in rows:
        row.to_tuple()


async def select_pyro_wtx(conn):
    rows = await conn.exec("SELECT * FROM benchmark_test")
    for row in rows:
        row.to_tuple()


def select_pyro_sync(conn):
    rows = conn.exec("SELECT * FROM benchmark_test")
    for row in rows:
        row.to_tuple()


async def select_async(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM benchmark_test")
        await cursor.fetchall()
        await cursor.close()


def select_sync(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM benchmark_test")
    cursor.fetchall()
    cursor.close()


