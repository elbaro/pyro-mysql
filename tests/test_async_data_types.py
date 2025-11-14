from datetime import date, datetime, time
from decimal import Decimal

import pytest

from tests.conftest import get_async_conn_with_backend, get_test_db_url


@pytest.mark.asyncio
async def test_integer_types(async_backend):
    """Test various integer types."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.query_drop("DROP TABLE IF EXISTS test_int_types")
    await conn.query_drop(
        """
        CREATE TABLE test_int_types (
            tiny_int TINYINT,
            small_int SMALLINT,
            medium_int MEDIUMINT,
            regular_int INT,
            big_int BIGINT,
            unsigned_int INT UNSIGNED
        )
    """
    )

    await conn.exec_drop(
        "INSERT INTO test_int_types VALUES (?, ?, ?, ?, ?, ?)",
        (127, 32767, 8388607, 2147483647, 9223372036854775807, 4294967295),
    )

    result = await conn.query_first("SELECT * FROM test_int_types")
    assert result and (result[0], result[1], result[2], result[3], result[4], result[5]) == (
        127,
        32767,
        8388607,
        2147483647,
        9223372036854775807,
        4294967295,
    )

    await conn.query_drop("DROP TABLE test_int_types")
    await conn.close()


@pytest.mark.asyncio
async def test_float_types(async_backend):
    """Test float and double types."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.query_drop("DROP TABLE IF EXISTS test_float_types")
    await conn.query_drop(
        """
        CREATE TABLE test_float_types (
            float_val FLOAT,
            double_val DOUBLE
        )
    """
    )

    await conn.exec_drop(
        "INSERT INTO test_float_types VALUES (?, ?)", (3.14159, 2.718281828)
    )

    result = await conn.query_first("SELECT * FROM test_float_types")
    assert result
    float_val, double_val = result[0], result[1]
    assert isinstance(float_val, float) and abs(float_val - 3.14159) < 0.001
    assert isinstance(double_val, float) and abs(double_val - 2.718281828) < 0.000001

    await conn.query_drop("DROP TABLE test_float_types")
    await conn.close()


@pytest.mark.asyncio
async def test_string_types(async_backend):
    """Test various string types."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.query_drop("DROP TABLE IF EXISTS test_string_types")
    await conn.query_drop(
        """
        CREATE TABLE test_string_types (
            varchar_val VARCHAR(255),
            char_val CHAR(10),
            text_val TEXT,
            longtext_val LONGTEXT
        )
    """
    )

    await conn.exec_drop(
        "INSERT INTO test_string_types VALUES (?, ?, ?, ?)",
        (
            "Hello World",
            "Fixed",
            "This is a text field",
            "This is a very long text field",
        ),
    )

    result = await conn.query_first("SELECT * FROM test_string_types")
    assert result
    assert (result[0], result[1], result[2], result[3]) == (
        "Hello World",
        "Fixed",
        "This is a text field",
        "This is a very long text field",
    )

    await conn.query_drop("DROP TABLE test_string_types")
    await conn.close()


@pytest.mark.asyncio
async def test_date_time_types(async_backend):
    """Test date and time types."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.query_drop("DROP TABLE IF EXISTS test_datetime_types")
    await conn.query_drop(
        """
        CREATE TABLE test_datetime_types (
            date_val DATE,
            time_val TIME,
            datetime_val DATETIME,
            timestamp_val TIMESTAMP
        )
    """
    )

    test_date = date(2023, 12, 25)
    test_time = time(15, 30, 45)
    test_datetime = datetime(2023, 12, 25, 15, 30, 45)

    await conn.exec_drop(
        "INSERT INTO test_datetime_types VALUES (?, ?, ?, ?)",
        (test_date, test_time, test_datetime, test_datetime),
    )

    result = await conn.query_first("SELECT * FROM test_datetime_types")
    assert result is not None

    await conn.query_drop("DROP TABLE test_datetime_types")
    await conn.close()


@pytest.mark.asyncio
async def test_decimal_types(async_backend):
    """Test decimal and numeric types."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.query_drop("DROP TABLE IF EXISTS test_decimal_types")
    await conn.query_drop(
        """
        CREATE TABLE test_decimal_types (
            from_bigint DECIMAL(40,2),
            decimal_val DECIMAL(10,2),
            numeric_val NUMERIC(15,4)
        )
    """
    )

    await conn.exec_drop(
        "INSERT INTO test_decimal_types VALUES (?, ?, ?)",
        (123456789012345678901234567890, Decimal("123.45"), Decimal("12345.6789")),
    )

    result = await conn.query_first("SELECT * FROM test_decimal_types")
    assert result
    assert (result[0], result[1], result[2]) == (
        Decimal("123456789012345678901234567890"),
        Decimal("123.45"),
        Decimal("12345.6789"),
    )

    await conn.query_drop("DROP TABLE test_decimal_types")
    await conn.close()


@pytest.mark.asyncio
async def test_binary_types(async_backend):
    """Test binary data types."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.query_drop("DROP TABLE IF EXISTS test_binary_types")
    await conn.query_drop(
        """
        CREATE TABLE test_binary_types (
            binary_val BINARY(10),
            varbinary_val VARBINARY(255),
            blob_val BLOB
        )
    """
    )

    binary_data = b"Hello\x00\x01\x02\x03\x04"
    blob_data = b"This is binary blob data"

    await conn.exec_drop(
        "INSERT INTO test_binary_types VALUES (?, ?, ?)",
        (binary_data, binary_data[:5], blob_data),
    )

    result = await conn.query_first("SELECT * FROM test_binary_types")
    assert result is not None

    await conn.query_drop("DROP TABLE test_binary_types")
    await conn.close()


@pytest.mark.asyncio
async def test_null_values(async_backend):
    """Test NULL value handling."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.query_drop("DROP TABLE IF EXISTS test_null_types")
    await conn.query_drop(
        """
        CREATE TABLE test_null_types (
            int_val INT,
            string_val VARCHAR(255),
            date_val DATE
        )
    """
    )

    await conn.exec_drop(
        "INSERT INTO test_null_types VALUES (?, ?, ?)", (None, None, None)
    )

    result = await conn.query_first("SELECT * FROM test_null_types")
    assert result
    assert (result[0], result[1], result[2]) == (None, None, None)

    await conn.query_drop("DROP TABLE test_null_types")
    await conn.close()


@pytest.mark.asyncio
async def test_boolean_type(async_backend):
    """Test boolean type handling."""
    conn = await get_async_conn_with_backend(get_test_db_url(), async_backend)

    await conn.query_drop("DROP TABLE IF EXISTS test_boolean_types")
    await conn.query_drop(
        """
        CREATE TABLE test_boolean_types (
            bool_val BOOLEAN
        )
    """
    )

    await conn.exec_drop(
        "INSERT INTO test_boolean_types VALUES (?), (?)", (True, False)
    )

    results = await conn.query("SELECT * FROM test_boolean_types ORDER BY bool_val")
    assert len(results) == 2
    assert results[0][0] == 0 or results[0][0] == False
    assert results[1][0] == 1 or results[1][0] == True

    await conn.query_drop("DROP TABLE test_boolean_types")
    await conn.close()
