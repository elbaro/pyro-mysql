import sys

sys.path = [".venv/lib/python3.14/site-packages"] + sys.path

from sqlalchemy import Column, Float, Integer, String, Text, create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Manually register pyro_mysql dialects
registry.register("mysql.pyro_mysql", "pyro_mysql.sqlalchemy_sync", "MySQLDialect_sync")
registry.register(
    "mariadb.pyro_mysql", "pyro_mysql.sqlalchemy_sync", "MariaDBDialect_sync"
)

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


# SELECT operations
def select_query():
    global session
    results = session.query(BenchmarkTest).all()
    # Force evaluation
    for row in results:
        _ = (row.id, row.name, row.age, row.email, row.score, row.description)


# INSERT operations with existing session
def insert_query(session, n):
    for i in range(n):
        obj = BenchmarkTest(**DATA[i])
        session.add(obj)
    session.commit()
