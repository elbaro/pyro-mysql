import asyncio

from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.dialects import registry
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Manually register pyro_mysql async dialects
registry.register(
    "mysql.pyro_mysql_async", "pyro_mysql.sqlalchemy_async", "MySQLDialect_async"
)
registry.register(
    "mariadb.pyro_mysql_async", "pyro_mysql.sqlalchemy_async", "MariaDBDialect_async"
)

HOST = "127.0.0.1"
PORT = 3306
USER = "test"
PASSWORD = "1234"
DATABASE = "test"

Base = declarative_base()
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


class BenchmarkTest(Base):
    __tablename__ = "benchmark_test"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))
    age = Column(Integer)
    email = Column(String(100))
    score = Column(Float)
    description = Column(String(100))


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
    elif driver_name == "pyro_wtx":
        # Pass wtx=true as query parameter to use wtx backend
        url = f"mariadb+pyro_mysql_async://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?wtx=true"
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
async def insert_individual(session, n):
    for i in range(n):
        obj = BenchmarkTest(**DATA[i])
        session.add(obj)
    await session.commit()


# SELECT operations
async def select_query(session):
    from sqlalchemy import text

    result = await session.execute(text("SELECT * FROM benchmark_test"))
    rows = result.fetchall()
    # Force evaluation
    for row in rows:
        _ = tuple(row)
