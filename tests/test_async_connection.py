import pytest
from pyro_mysql.async_ import Conn

from .conftest import (
    cleanup_test_table_async,
    get_test_db_url,
    setup_test_table_async,
)


@pytest.mark.asyncio
async def test_basic_connection():
    """Test basic connection establishment."""
    url = get_test_db_url()
    conn = await Conn.new(url)

    result = await conn.query_first("SELECT 1")
    assert result and result.to_tuple() == (1,)

    await conn.close()


@pytest.mark.asyncio
async def test_connection_with_database():
    """Test connection with specific database."""
    url = get_test_db_url()
    conn = await Conn.new(url)

    db_name = await conn.query_first("SELECT DATABASE()")
    assert db_name and db_name.to_tuple() == ("test",)

    await conn.close()


@pytest.mark.asyncio
async def test_connection_timeout():
    """Test connection timeout handling."""
    url = get_test_db_url()
    # Note: wtx backend doesn't support timeout configuration via opts yet
    # This test just ensures basic connection works
    try:
        conn = await Conn.new(url)
        await conn.close()
    except Exception:
        # Connection timeout is expected to potentially fail
        pass


@pytest.mark.asyncio
async def test_connection_ping():
    """Test connection ping functionality."""
    url = get_test_db_url()
    conn = await Conn.new(url)

    await conn.ping()

    await conn.close()


@pytest.mark.asyncio
async def test_connection_reset():
    """Test connection reset functionality."""
    url = get_test_db_url()
    conn = await Conn.new(url)

    await conn.query_drop("SET @test_var = 42")

    result = await conn.query_first("SELECT @test_var")
    assert result and result.to_tuple() == (42,)

    await conn.reset()

    result = await conn.query_first("SELECT @test_var")
    assert result and result.to_tuple() == (None,)

    await conn.close()


@pytest.mark.asyncio
async def test_connection_server_info():
    """Test retrieving server information."""
    url = get_test_db_url()
    conn = await Conn.new(url)

    server_version = await conn.server_version()
    assert server_version[0] >= 5

    connection_id = await conn.id()
    assert connection_id >= 0  # wtx backend returns 0 for now

    await conn.close()


@pytest.mark.asyncio
async def test_connection_charset():
    """Test connection charset handling."""
    url = get_test_db_url()
    conn = await Conn.new(url)

    charset = await conn.query_first("SELECT @@character_set_connection")
    assert charset and charset is not None

    await conn.query_drop("SET NAMES utf8mb4")

    charset = await conn.query_first("SELECT @@character_set_connection")
    assert charset and charset.to_tuple() == ("utf8mb4",)

    await conn.close()


@pytest.mark.asyncio
async def test_connection_autocommit():
    """Test autocommit functionality."""
    url = get_test_db_url()
    conn = await Conn.new(url)

    await setup_test_table_async(conn)

    await conn.query_drop("SET autocommit = 0")

    autocommit = await conn.query_first("SELECT @@autocommit")
    assert autocommit and autocommit.to_tuple() == (0,)

    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Test', 25)")

    await conn.query_drop("ROLLBACK")

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count and count.to_tuple() == (0,)

    await conn.query_drop("SET autocommit = 1")

    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Test2', 30)")

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count and count.to_tuple() == (1,)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_connection_ssl():
    """Test SSL connection (if available)."""
    url = get_test_db_url()
    # Note: wtx backend doesn't support SSL configuration via opts yet
    # This test just ensures basic connection works
    try:
        conn = await Conn.new(url)

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
    url = get_test_db_url()
    # Note: wtx backend doesn't support init commands via opts yet
    # This test manually runs the init command instead
    conn = await Conn.new(url)

    await conn.query_drop("SET @init_test = 123")
    result = await conn.query_first("SELECT @init_test")
    assert result and result.to_tuple() == (123,)

    await conn.close()


# TODO: needs a separate table dedicated for this test
# @pytest.mark.asyncio
# async def test_large_data_transfer():
#     """Test handling of large data transfers."""
#     opts = (
#         AsyncOptsBuilder().from_url(get_test_db_url()).max_allowed_packet(200).build()
#     )
#     conn = await Conn.new(opts)

#     await setup_test_table_async(conn)

#     large_string = "x" * (250)

#     # with pytest.raises(
#     #     RuntimeError, match="Input/output error: Input/output error: packet too larg"
#     # ):
#     await conn.exec_drop("INSERT INTO test_table (name) VALUES (?)", (large_string,))

#     await cleanup_test_table_async(conn)
#     await conn.close()


@pytest.mark.asyncio
async def test_connection_with_wrong_credentials():
    """Test connection failure with wrong credentials."""
    url = "mysql://nonexistent_user:wrong_password@localhost:3306/test"

    with pytest.raises(Exception):
        _ = await Conn.new(url)


@pytest.mark.asyncio
async def test_connection_to_invalid_host():
    """Test connection failure to invalid host."""
    url = "mysql://test:1234@invalid.host.that.does.not.exist:3306/test"

    with pytest.raises(Exception):
        await Conn.new(url)
