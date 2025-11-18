import asyncio
import sys

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
        "mysql://test:1234@127.0.0.1:3306/test?prefer_socket=false"
    )


async def create_pyro_wtx_conn():
    return await pyro_mysql.AsyncConn.new(
        "mysql://test:1234@127.0.0.1:3306/test", backend="wtx"
    )


async def create_pyro_zero_mysql_conn():
    return await pyro_mysql.AsyncConn.new(
        "mysql://test:1234@127.0.0.1:3306/test", backend="zero-mysql"
    )


async def create_asyncmy_conn():
    return await asyncmy.connect(
        host="127.0.0.1",
        port=3306,
        user="test",
        password="1234",
        db="test",
        autocommit=True,
    )


async def create_aiomysql_conn():
    return await aiomysql.connect(
        host="127.0.0.1",
        port=3306,
        user="test",
        password="1234",
        db="test",
        autocommit=True,
    )


# ─── Insert ───────────────────────────────────────────────────────────────────


async def insert_pyro_async(conn, n):
    for i in range(n):
        await conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i % 10000],
        )


async def insert_pyro_wtx(conn, n):
    for i in range(n):
        await conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i % 10000],
        )


async def insert_pyro_zero_mysql_async(conn, n):
    for i in range(n):
        await conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i % 10000],
        )
        # await conn.query_drop(
        #     "INSERT INTO benchmark_test (name, age, email, score, description) VALUES ('%s', %s, '%s', %s, '%s')"
        #     % DATA[i % 10000],
        # )


def insert_pyro_sync(conn, n):
    for i in range(n):
        conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i % 10000],
        )


def insert_pyro_diesel(conn, n):
    for i in range(n):
        conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i % 10000],
        )


def insert_pyro_zero_mysql(conn, n):
    for i in range(n):
        conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i % 10000],
        )


import time


async def insert_async(conn, n: int):
    async with conn.cursor() as cursor:
        for i in range(n):
            await cursor.execute(
                """INSERT INTO benchmark_test (name, age, email, score, description)
                    VALUES (%s, %s, %s, %s, %s)""",
                DATA[i % 10000],
            )
        await cursor.close()


def insert_sync(conn, n: int):
    cursor = conn.cursor()
    for i in range(n):
        cursor.execute(
            """INSERT INTO benchmark_test (name, age, email, score, description)
                VALUES (%s, %s, %s, %s, %s)""",
            DATA[i % 10000],
        )
    cursor.close()


# ─── Select ───────────────────────────────────────────────────────────────────


async def select_pyro_async(conn):
    rows = await conn.exec("SELECT * FROM benchmark_test")


async def select_pyro_wtx(conn):
    rows = await conn.exec("SELECT * FROM benchmark_test")


async def select_pyro_zero_mysql_async(conn):
    rows = await conn.exec("SELECT * FROM benchmark_test")


def select_pyro_sync(conn):
    rows = conn.exec("SELECT * FROM benchmark_test")


def select_pyro_diesel(conn):
    rows = conn.exec("SELECT * FROM benchmark_test")


def select_pyro_zero_mysql(conn):
    rows = conn.exec("SELECT * FROM benchmark_test")


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
