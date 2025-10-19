import asyncio

from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

HOST = "127.0.0.1"
PORT = 3306
USER = "test"
PASSWORD = "1234"
DATABASE = "test"

Base = declarative_base()


class BenchmarkTest(Base):
    __tablename__ = "benchmark_test"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))
    age = Column(Integer)
    email = Column(String(100))
    score = Column(Float)
    description = Column(String(500))


# Pre-generated test data
DATA = [
    {
        "name": f"user_{i}",
        "age": 20 + (i % 5),
        "email": f"user{i}@example.com",
        "score": float(i % 10),
        "description": f"Description for user {i}",
    }
    for i in range(10000)
]


def create_async_session(driver_name):
    """Create async SQLAlchemy session with specified driver"""
    if driver_name == "pyro_mysql":
        url = f"mariadb+pyro_mysql_async://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    elif driver_name == "aiomysql":
        url = f"mysql+aiomysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    elif driver_name == "asyncmy":
        url = f"mysql+asyncmy://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    else:
        raise ValueError(f"Unknown driver: {driver_name}")

    engine = create_async_engine(url, echo=False)
    Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    return Session(), engine


# Individual INSERT operations
async def insert_individual(driver_name, n):
    session, engine = create_async_session(driver_name)
    for i in range(n):
        obj = BenchmarkTest(**DATA[i])
        session.add(obj)
    await session.commit()
    await session.close()
    await engine.dispose()


# SELECT operations
async def select_query(driver_name, n, batch):
    from sqlalchemy import text

    session, engine = create_async_session(driver_name)
    for i in range(0, n * batch, batch):
        result = await session.execute(
            text(
                f"SELECT * FROM benchmark_test WHERE id >= {i + 1} AND id < {i + 1 + batch}"
            )
        )
        rows = result.fetchall()
        # Force evaluation
        for row in rows:
            _ = tuple(row)
    await session.close()
    await engine.dispose()


# Individual UPDATE operations
async def update_individual(driver_name, n):
    from sqlalchemy import text

    session, engine = create_async_session(driver_name)
    for i in range(1, n + 1):
        await session.execute(
            text(f"UPDATE benchmark_test SET age = {30 + (i % 10)} WHERE id = {i}")
        )
    await session.commit()
    await session.close()
    await engine.dispose()


# Helper function to run async benchmarks from sync context
def run_async_benchmark(driver_name, benchmark_type, *args):
    """Run async benchmark function synchronously"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        if benchmark_type == "insert_individual":
            return loop.run_until_complete(insert_individual(driver_name, *args))
        elif benchmark_type == "select_query":
            return loop.run_until_complete(select_query(driver_name, *args))
        elif benchmark_type == "update_individual":
            return loop.run_until_complete(update_individual(driver_name, *args))
        else:
            raise ValueError(f"Unknown benchmark type: {benchmark_type}")
    finally:
        loop.close()
