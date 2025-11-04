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

pyro_mysql.init(worker_threads=1)


# ─── Sync Connection ──────────────────────────────────────────────────────────


def connect_mysqldb():
    conn = MySQLdb.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    conn.close()


def connect_pymysql():
    conn = pymysql.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    conn.close()


def connect_pyro_sync():
    conn = pyro_mysql.SyncConn("mysql://test:1234@127.0.0.1:3306/test")
    # SyncConn closes automatically when dropped


# ─── Async Connection ─────────────────────────────────────────────────────────


async def connect_pyro_async():
    conn = await pyro_mysql.AsyncConn.new("mysql://test:1234@127.0.0.1:3306/test")
    # AsyncConn closes automatically when dropped


async def connect_pyro_wtx():
    conn = await pyro_mysql.AsyncConn.new_wtx(
        "mysql://test:1234@127.0.0.1:3306/test",
        max_statements=32,
        buffer_size=(512, 512, 8192, 512, 32),
    )
    # AsyncConn closes automatically when dropped


async def connect_asyncmy():
    conn = await asyncmy.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, db=DATABASE
    )
    await conn.ensure_closed()


async def connect_aiomysql():
    conn = await aiomysql.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, db=DATABASE
    )
    conn.close()
    await conn.ensure_closed()
