import sys

sys.path = [".venv/lib/python3.14/site-packages"] + sys.path

from sqlalchemy import Column, Float, Integer, String, Text, create_engine
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


def create_session(driver_name):
    """Create SQLAlchemy session with specified driver"""
    if driver_name == "pyro_mysql":
        url = f"mysql+pyro_mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    elif driver_name == "pymysql":
        url = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    elif driver_name == "mysqldb":
        url = f"mysql+mysqldb://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    else:
        raise ValueError(f"Unknown driver: {driver_name}")

    engine = create_engine(url, echo=False)
    Session = sessionmaker(bind=engine)
    return Session(), engine


# Individual INSERT operations
def insert_individual(driver_name, n):
    session, engine = create_session(driver_name)
    for i in range(n):
        obj = BenchmarkTest(**DATA[i])
        session.add(obj)
    session.commit()
    session.close()
    engine.dispose()


# SELECT operations
def select_query(driver_name, n, batch):
    session, engine = create_session(driver_name)
    for i in range(0, n * batch, batch):
        results = (
            session.query(BenchmarkTest)
            .filter(BenchmarkTest.id >= i + 1, BenchmarkTest.id < i + 1 + batch)
            .all()
        )
        # Force evaluation
        for row in results:
            _ = (row.id, row.name, row.age, row.email, row.score, row.description)
    session.close()
    engine.dispose()


# Individual UPDATE operations
def update_individual(driver_name, n):
    session, engine = create_session(driver_name)
    for i in range(1, n + 1):
        session.query(BenchmarkTest).filter(BenchmarkTest.id == i).update(
            {"age": 30 + (i % 10)}
        )
    session.commit()
    session.close()
    engine.dispose()


# Bulk UPDATE operations
def update_bulk(driver_name, n):
    session, engine = create_session(driver_name)
    session.query(BenchmarkTest).filter(BenchmarkTest.id <= n).update({"age": 25})
    session.commit()
    session.close()
    engine.dispose()
