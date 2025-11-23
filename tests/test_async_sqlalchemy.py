"""Tests for SQLAlchemy async integration with pyro-mysql dialect."""

import pytest
import pytest_asyncio
from sqlalchemy import Column, Integer, String, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base

from .conftest import get_test_db_url

Base = declarative_base()


class User(Base):
    __tablename__ = "sqlalchemy_async_test_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255))
    age = Column(Integer)


def get_sqlalchemy_async_url() -> str:
    """Convert mysql:// URL to mysql+pyro_mysql_async:// for SQLAlchemy async."""
    url = get_test_db_url()
    return url.replace("mysql://", "mysql+pyro_mysql_async://", 1)


@pytest_asyncio.fixture
async def engine():
    """Create an async SQLAlchemy engine with pyro-mysql dialect."""
    url = get_sqlalchemy_async_url()
    engine = create_async_engine(url, echo=False)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def setup_table(engine):
    """Create test table before tests and drop after."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.mark.asyncio
async def test_raw_sql_query(engine):
    """Test executing raw SQL through SQLAlchemy async."""
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT 1 AS value"))
        row = result.fetchone()
        assert row is not None
        assert row[0] == 1


@pytest.mark.asyncio
async def test_create_and_query_table(engine, setup_table):
    """Test creating a table and querying it."""
    async with AsyncSession(engine) as session:
        user = User(name="Alice", age=30)
        session.add(user)
        await session.commit()

        result = await session.execute(
            text("SELECT * FROM sqlalchemy_async_test_users WHERE name = :name").bindparams(
                name="Alice"
            )
        )
        row = result.fetchone()
        assert row is not None
        assert row.name == "Alice"
        assert row.age == 30


@pytest.mark.asyncio
async def test_insert_and_select_multiple(engine, setup_table):
    """Test inserting and selecting multiple rows."""
    async with AsyncSession(engine) as session:
        users = [
            User(name="Alice", age=30),
            User(name="Bob", age=25),
            User(name="Charlie", age=35),
        ]
        session.add_all(users)
        await session.commit()

        result = await session.execute(
            text("SELECT * FROM sqlalchemy_async_test_users ORDER BY age")
        )
        all_users = result.fetchall()
        assert len(all_users) == 3
        assert all_users[0].name == "Bob"
        assert all_users[1].name == "Alice"
        assert all_users[2].name == "Charlie"


@pytest.mark.asyncio
async def test_update_row(engine, setup_table):
    """Test updating a row."""
    async with AsyncSession(engine) as session:
        user = User(name="Alice", age=30)
        session.add(user)
        await session.commit()
        await session.refresh(user)
        user_id = user.id

    async with AsyncSession(engine) as session:
        await session.execute(
            text("UPDATE sqlalchemy_async_test_users SET age = :age WHERE id = :id").bindparams(
                age=31, id=user_id
            )
        )
        await session.commit()

        result = await session.execute(
            text("SELECT age FROM sqlalchemy_async_test_users WHERE id = :id").bindparams(
                id=user_id
            )
        )
        row = result.fetchone()
        assert row is not None
        assert row.age == 31


@pytest.mark.asyncio
async def test_delete_row(engine, setup_table):
    """Test deleting a row."""
    async with AsyncSession(engine) as session:
        user = User(name="Alice", age=30)
        session.add(user)
        await session.commit()
        await session.refresh(user)
        user_id = user.id

    async with AsyncSession(engine) as session:
        await session.execute(
            text("DELETE FROM sqlalchemy_async_test_users WHERE id = :id").bindparams(
                id=user_id
            )
        )
        await session.commit()

        result = await session.execute(
            text("SELECT * FROM sqlalchemy_async_test_users WHERE id = :id").bindparams(
                id=user_id
            )
        )
        row = result.fetchone()
        assert row is None


@pytest.mark.asyncio
async def test_transaction_rollback(engine, setup_table):
    """Test transaction rollback."""
    async with AsyncSession(engine) as session:
        user = User(name="Alice", age=30)
        session.add(user)
        await session.commit()

    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT * FROM sqlalchemy_async_test_users WHERE name = :name").bindparams(
                name="Alice"
            )
        )
        row = result.fetchone()

        await session.execute(
            text("UPDATE sqlalchemy_async_test_users SET age = 99 WHERE name = :name").bindparams(
                name="Alice"
            )
        )
        await session.rollback()

        # After rollback, the age should still be 30
        result = await session.execute(
            text("SELECT age FROM sqlalchemy_async_test_users WHERE name = :name").bindparams(
                name="Alice"
            )
        )
        row = result.fetchone()
        assert row.age == 30


@pytest.mark.asyncio
async def test_filter_with_parameters(engine, setup_table):
    """Test filtering with bound parameters."""
    async with AsyncSession(engine) as session:
        users = [
            User(name="Alice", age=30),
            User(name="Bob", age=25),
            User(name="Charlie", age=35),
        ]
        session.add_all(users)
        await session.commit()

        result = await session.execute(
            text("SELECT * FROM sqlalchemy_async_test_users WHERE age > :age").bindparams(
                age=28
            )
        )
        older_users = result.fetchall()
        assert len(older_users) == 2

        result = await session.execute(
            text("SELECT * FROM sqlalchemy_async_test_users WHERE age < :age").bindparams(
                age=28
            )
        )
        young_users = result.fetchall()
        assert len(young_users) == 1
        assert young_users[0].name == "Bob"


@pytest.mark.asyncio
async def test_concurrent_connections(engine, setup_table):
    """Test using concurrent async connections."""
    import asyncio

    async def insert_user(name: str, age: int):
        async with AsyncSession(engine) as session:
            user = User(name=name, age=age)
            session.add(user)
            await session.commit()
            await session.refresh(user)
            return user.id

    # Insert users concurrently
    tasks = [
        insert_user("Alice", 30),
        insert_user("Bob", 25),
        insert_user("Charlie", 35),
    ]
    ids = await asyncio.gather(*tasks)
    assert len(ids) == 3

    # Verify all users were inserted
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT COUNT(*) as cnt FROM sqlalchemy_async_test_users")
        )
        row = result.fetchone()
        assert row.cnt == 3
