import pytest
from pyro_mysql import Opts
from pyro_mysql.async_ import Conn

from .conftest import (
    cleanup_test_table_async,
    get_async_conn_with_backend,
    get_async_opts,
    get_test_db_url,
    setup_test_table_async,
)


@pytest.mark.asyncio
async def test_basic_connection(async_backend):
    """Test basic connection establishment."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    result = await conn.query_first("SELECT 1")
    assert result and result[0] == 1

    await conn.close()


@pytest.mark.asyncio
async def test_connection_with_database(async_backend):
    """Test connection with specific database."""
    url = get_test_db_url()

    if async_backend == "mysql_async":
        opts = Opts(url).db("test")
        conn = await Conn.new(opts, backend=async_backend)
    else:
        # wtx and zero backends use URL strings
        conn = await get_async_conn_with_backend(url, async_backend)

    db_name = await conn.query_first("SELECT DATABASE()")
    assert db_name and db_name[0] == "test"

    await conn.close()


@pytest.mark.asyncio
async def test_connection_timeout(async_backend):
    """Test connection timeout handling."""
    url = get_test_db_url()

    try:
        # Note: wait_timeout is backend-specific and not in unified Opts
        conn = await get_async_conn_with_backend(url, async_backend)
        await conn.close()
    except Exception:
        # Connection timeout is expected to potentially fail
        pass


@pytest.mark.asyncio
async def test_connection_ping(async_backend):
    """Test connection ping functionality."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.ping()

    await conn.close()


@pytest.mark.asyncio
async def test_connection_reset(async_backend):
    """Test connection reset functionality."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.query_drop("SET @test_var = 42")

    result = await conn.query_first("SELECT @test_var")
    assert result and result[0] == 42

    await conn.reset()

    result = await conn.query_first("SELECT @test_var")
    assert result and result[0] is None

    await conn.close()


@pytest.mark.asyncio
async def test_connection_charset(async_backend):
    """Test connection charset handling."""
    url = get_test_db_url()

    if async_backend == "mysql_async":
        opts = Opts(url)
        conn = await Conn.new(opts, backend=async_backend)
    else:
        conn = await get_async_conn_with_backend(url, async_backend)

    charset = await conn.query_first("SELECT @@character_set_connection")
    assert charset and charset is not None

    await conn.query_drop("SET NAMES utf8mb4")

    charset = await conn.query_first("SELECT @@character_set_connection")
    assert charset and charset[0] == "utf8mb4"

    await conn.close()


@pytest.mark.asyncio
async def test_connection_autocommit(async_backend):
    """Test autocommit functionality."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

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
async def test_connection_ssl(async_backend):
    """Test SSL connection (if available)."""
    url = get_test_db_url()

    try:
        conn = await get_async_conn_with_backend(url, async_backend)

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
async def test_connection_init_command(async_backend):
    """Test connection initialization commands."""
    url = get_test_db_url()

    # Note: init commands are backend-specific and not in unified Opts
    # Test manually setting variable after connection
    conn = await get_async_conn_with_backend(url, async_backend)

    await conn.query_drop("SET @init_test = 123")

    result = await conn.query_first("SELECT @init_test")
    assert result and result[0] == 123

    await conn.close()


# TODO: needs a separate table dedicated for this test
# @pytest.mark.asyncio
# async def test_large_data_transfer():
#     """Test handling of large data transfers."""
#     opts = (
#         OptsBuilder().from_url(get_test_db_url()).max_allowed_packet(200).build()
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
async def test_connection_with_wrong_credentials(async_backend):
    """Test connection failure with wrong credentials."""
    if async_backend == "mysql_async":
        opts = (
            Opts().host("localhost").user("nonexistent_user").password("wrong_password")
        )

        with pytest.raises(Exception):
            _ = await Conn.new(opts, backend=async_backend)
    else:
        # For wtx and zero, construct URL string with wrong credentials
        with pytest.raises(Exception):
            _ = await get_async_conn_with_backend(
                "mysql://nonexistent_user:wrong_password@localhost:3306/test",
                async_backend,
            )


@pytest.mark.asyncio
async def test_connection_to_invalid_host(async_backend):
    """Test connection failure to invalid host."""
    if async_backend == "mysql_async":
        opts = Opts().host("invalid.host.that.does.not.exist").port(3306)

        with pytest.raises(Exception):
            await Conn.new(opts, backend=async_backend)
    else:
        # For wtx and zero, use URL string with invalid host
        with pytest.raises(Exception):
            await get_async_conn_with_backend(
                "mysql://test:1234@invalid.host.that.does.not.exist:3306/test",
                async_backend,
            )
