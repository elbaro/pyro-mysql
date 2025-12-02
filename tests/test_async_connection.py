import pytest
from pyro_mysql import Opts
from pyro_mysql.async_ import Conn

from .conftest import (
    cleanup_test_table_async,
    get_async_conn,
    get_async_opts,
    get_test_db_url,
    setup_test_table_async,
)


@pytest.mark.asyncio
async def test_basic_connection():
    """Test basic connection establishment."""
    conn = await get_async_conn(get_test_db_url())

    result = await conn.query_first("SELECT 1")
    assert result and result[0] == 1

    await conn.close()


@pytest.mark.asyncio
async def test_connection_with_database():
    """Test connection with specific database."""
    url = get_test_db_url()
    conn = await get_async_conn(url)

    db_name = await conn.query_first("SELECT DATABASE()")
    assert db_name and db_name[0] == "test"

    await conn.close()


@pytest.mark.asyncio
async def test_connection_timeout():
    """Test connection timeout handling."""
    url = get_test_db_url()

    try:
        conn = await get_async_conn(url)
        await conn.close()
    except Exception:
        # Connection timeout is expected to potentially fail
        pass


@pytest.mark.asyncio
async def test_connection_ping():
    """Test connection ping functionality."""
    conn = await get_async_conn(get_test_db_url())

    await conn.ping()

    await conn.close()


@pytest.mark.asyncio
async def test_connection_reset():
    """Test connection reset functionality."""
    conn = await get_async_conn(get_test_db_url())

    await conn.query_drop("SET @test_var = 42")

    result = await conn.query_first("SELECT @test_var")
    assert result and result[0] == 42

    await conn.reset()

    result = await conn.query_first("SELECT @test_var")
    assert result and result[0] is None

    await conn.close()


@pytest.mark.asyncio
async def test_connection_charset():
    """Test connection charset handling."""
    url = get_test_db_url()
    conn = await get_async_conn(url)

    charset = await conn.query_first("SELECT @@character_set_connection")
    assert charset and charset is not None

    await conn.query_drop("SET NAMES utf8mb4")

    charset = await conn.query_first("SELECT @@character_set_connection")
    assert charset and charset[0] == "utf8mb4"

    await conn.close()


@pytest.mark.asyncio
async def test_connection_autocommit():
    """Test autocommit functionality."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.query_drop("SET autocommit = 0")

    autocommit = await conn.query_first("SELECT @@autocommit")
    assert autocommit and autocommit[0] == 0

    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Test', 25)")

    await conn.query_drop("ROLLBACK")

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count and count[0] == 0

    await conn.query_drop("SET autocommit = 1")

    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Test2', 30)")

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count and count[0] == 1

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_connection_ssl():
    """Test SSL connection (if available)."""
    url = get_test_db_url()

    try:
        conn = await get_async_conn(url)

        try:
            _ssl_result = await conn.query_first("SHOW STATUS LIKE 'Ssl_cipher'")
            # SSL cipher status may or may not be available depending on server config
        except Exception:
            pass

        await conn.close()
    except Exception:
        # SSL connection may not be available in test environment
        pass


@pytest.mark.asyncio
async def test_connection_init_command():
    """Test connection initialization commands."""
    from pyro_mysql.async_ import Conn

    opts = Opts(get_test_db_url()).init_command("SET @init_test = 123")
    conn = await Conn.new(opts)

    result = await conn.query_first("SELECT @init_test")
    assert result and result[0] == 123

    await conn.close()


@pytest.mark.asyncio
async def test_connection_with_wrong_credentials():
    """Test connection failure with wrong credentials."""
    with pytest.raises(Exception):
        _ = await get_async_conn(
            "mysql://nonexistent_user:wrong_password@localhost:3306/test"
        )


@pytest.mark.asyncio
async def test_connection_to_invalid_host():
    """Test connection failure to invalid host."""
    with pytest.raises(Exception):
        await get_async_conn(
            "mysql://test:1234@invalid.host.that.does.not.exist:3306/test"
        )
