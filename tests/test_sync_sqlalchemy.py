"""Tests for SQLAlchemy integration with pyro-mysql sync dialect."""

import pytest
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.orm import Session, declarative_base

from .conftest import get_test_db_url

Base = declarative_base()


class User(Base):
    __tablename__ = "sqlalchemy_test_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255))
    age = Column(Integer)


def get_sqlalchemy_url() -> str:
    """Convert mysql:// URL to pyro_mysql:// for SQLAlchemy."""
    url = get_test_db_url()
    return url.replace("mysql://", "pyro_mysql://", 1)


@pytest.fixture
def engine():
    """Create a SQLAlchemy engine with pyro-mysql dialect."""
    url = get_sqlalchemy_url()
    engine = create_engine(url, echo=False)
    yield engine
    engine.dispose()


@pytest.fixture
def setup_table(engine):
    """Create test table before tests and drop after."""
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


def test_raw_sql_query(engine):
    """Test executing raw SQL through SQLAlchemy."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS value"))
        row = result.fetchone()
        assert row is not None
        assert row[0] == 1


def test_create_and_query_table(engine, setup_table):
    """Test creating a table and querying it."""
    with Session(engine) as session:
        user = User(name="Alice", age=30)
        session.add(user)
        session.commit()

        result = session.query(User).filter_by(name="Alice").first()
        assert result is not None
        assert result.name == "Alice"
        assert result.age == 30


def test_insert_and_select_multiple(engine, setup_table):
    """Test inserting and selecting multiple rows."""
    with Session(engine) as session:
        users = [
            User(name="Alice", age=30),
            User(name="Bob", age=25),
            User(name="Charlie", age=35),
        ]
        session.add_all(users)
        session.commit()

        all_users = session.query(User).order_by(User.age).all()
        assert len(all_users) == 3
        assert all_users[0].name == "Bob"
        assert all_users[1].name == "Alice"
        assert all_users[2].name == "Charlie"


def test_update_row(engine, setup_table):
    """Test updating a row."""
    with Session(engine) as session:
        user = User(name="Alice", age=30)
        session.add(user)
        session.commit()

        user.age = 31
        session.commit()

        updated = session.query(User).filter_by(name="Alice").first()
        assert updated is not None
        assert updated.age == 31


def test_delete_row(engine, setup_table):
    """Test deleting a row."""
    with Session(engine) as session:
        user = User(name="Alice", age=30)
        session.add(user)
        session.commit()

        session.delete(user)
        session.commit()

        result = session.query(User).filter_by(name="Alice").first()
        assert result is None


def test_transaction_rollback(engine, setup_table):
    """Test transaction rollback."""
    with Session(engine) as session:
        user = User(name="Alice", age=30)
        session.add(user)
        session.commit()

    with Session(engine) as session:
        user = session.query(User).filter_by(name="Alice").first()
        user.age = 99
        session.rollback()

        session.expire_all()
        refreshed = session.query(User).filter_by(name="Alice").first()
        assert refreshed.age == 30


def test_filter_with_parameters(engine, setup_table):
    """Test filtering with bound parameters."""
    with Session(engine) as session:
        users = [
            User(name="Alice", age=30),
            User(name="Bob", age=25),
            User(name="Charlie", age=35),
        ]
        session.add_all(users)
        session.commit()

        older_users = session.query(User).filter(User.age > 28).all()
        assert len(older_users) == 2

        young_users = session.query(User).filter(User.age < 28).all()
        assert len(young_users) == 1
        assert young_users[0].name == "Bob"
