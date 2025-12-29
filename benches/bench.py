import asyncio
import sys

import aiomysql
import asyncmy
import mariadb
import MySQLdb
import pymysql
import pyro_mysql

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


# ─── Connection Setup Helpers ─────────────────────────────────────────────────


async def create_pyro_async_conn():
    url = "mysql://test:1234@localhost:3306/test"
    return await pyro_mysql.AsyncConn.new(url)


async def create_asyncmy_conn():
    return await asyncmy.connect(
        host="localhost",
        port=3306,
        user="test",
        password="1234",
        db="test",
        autocommit=True,
    )


async def create_aiomysql_conn():
    return await aiomysql.connect(
        host="localhost",
        port=3306,
        user="test",
        password="1234",
        db="test",
        autocommit=True,
    )


def create_mariadb_conn():
    return mariadb.connect(
        host="localhost",
        port=3306,
        user="test",
        password="1234",
        database="test",
        autocommit=True,
    )


# ─── Insert ───────────────────────────────────────────────────────────────────


async def insert_pyro_async(conn, n):
    for i in range(n):
        await conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i % 10000],
        )


async def insert_pyro_async_bulk(conn, n):
    """Insert using exec_bulk_insert_or_update with batches of up to 1000 rows"""
    batch_size = 1000
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        batch_data = [DATA[i % 10000] for i in range(batch_start, batch_end)]
        await conn.exec_bulk_insert_or_update(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            batch_data,
        )


def insert_pyro_sync(conn, n):
    for i in range(n):
        conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            DATA[i % 10000],
        )


def insert_pyro_sync_bulk(conn, n):
    """Insert using exec_bulk_insert_or_update with batches of up to 1000 rows"""
    batch_size = 1000
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        batch_data = [DATA[i % 10000] for i in range(batch_start, batch_end)]
        conn.exec_bulk_insert_or_update(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (?, ?, ?, ?, ?)",
            batch_data,
        )


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


def insert_mariadb(conn, n: int):
    cursor = conn.cursor()
    for i in range(n):
        cursor.execute(
            """INSERT INTO benchmark_test (name, age, email, score, description)
                VALUES (?, ?, ?, ?, ?)""",
            DATA[i % 10000],
        )
    cursor.close()


def insert_mariadb_bulk(conn, n: int):
    """Insert using executemany with batches of up to 1000 rows"""
    cursor = conn.cursor()
    batch_size = 1000
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        batch_data = [DATA[i % 10000] for i in range(batch_start, batch_end)]
        cursor.executemany(
            """INSERT INTO benchmark_test (name, age, email, score, description)
                VALUES (?, ?, ?, ?, ?)""",
            batch_data,
        )
    cursor.close()


# ─── Select ───────────────────────────────────────────────────────────────────


async def select_pyro_async(conn):
    rows = await conn.exec("SELECT * FROM benchmark_test")


def select_pyro_sync(conn):
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


def select_mariadb(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM benchmark_test")
    cursor.fetchall()
    cursor.close()
