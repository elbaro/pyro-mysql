import time
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from decimal import Decimal

from pyro_mysql.sync import Conn
from tests.conftest import get_test_db_url


def test_none_type(backend):
    """Test None type handling."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_none_types")
    conn.query_drop(
        """
        CREATE TABLE test_none_types (
            int_val INT,
            string_val VARCHAR(255),
            date_val DATE
        )
    """
    )

    conn.exec_drop("INSERT INTO test_none_types VALUES (?, ?, ?)", (None, None, None))

    result = conn.query_first("SELECT * FROM test_none_types")
    assert result
    assert (result[0], result[1], result[2]) == (None, None, None)

    conn.query_drop("DROP TABLE test_none_types")
    conn.close()


def test_int_type(backend):
    """Test int type handling."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_int_types")
    conn.query_drop(
        """
        CREATE TABLE test_int_types (
            small_val SMALLINT,
            regular_val INT,
            big_val BIGINT
        )
    """
    )

    test_values = (42, 2147483647, 9223372036854775807)
    conn.exec_drop("INSERT INTO test_int_types VALUES (?, ?, ?)", test_values)

    result = conn.query_first("SELECT * FROM test_int_types")
    assert result
    assert tuple(result[i] for i in range(len(result))) == test_values

    conn.query_drop("DROP TABLE test_int_types")
    conn.close()


def test_float_type(backend):
    """Test float type handling."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_float_types")
    conn.query_drop(
        """
        CREATE TABLE test_float_types (
            float_val FLOAT,
            double_val DOUBLE
        )
    """
    )

    test_values1 = (3.14159, 2.718281828459045)
    conn.exec_drop("INSERT INTO test_float_types VALUES (?, ?)", test_values1)

    test_values2 = (3, 2)
    conn.exec_drop("INSERT INTO test_float_types VALUES (?, ?)", test_values2)

    result = conn.query_first("SELECT * FROM test_float_types WHERE float_val > 3")
    assert result
    float_val, double_val = result[0], result[1]
    assert isinstance(float_val, float) and isinstance(double_val, float)
    assert abs(float_val - test_values1[0]) < 0.001
    assert abs(double_val - test_values1[1]) < 0.000001

    result = conn.query_first("SELECT * FROM test_float_types WHERE float_val < 3.14")
    assert result
    float_val, double_val = result[0], result[1]
    assert isinstance(float_val, float) and isinstance(double_val, float)
    assert abs(float_val - test_values2[0]) < 0.001
    assert abs(double_val - test_values2[1]) < 0.000001

    conn.query_drop("DROP TABLE test_float_types")
    conn.close()


def test_str_type(backend):
    """Test str type handling."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_str_types")
    conn.query_drop(
        """
        CREATE TABLE test_str_types (
            varchar_val VARCHAR(255),
            text_val TEXT,
            blob_val BLOB,
            int_val INT,
            float_val FLOAT
        )
    """
    )

    test_values = (
        b"Hello, World!",
        "This is a longer text string with special chars: åäö",
        "This string goes to blob",
        "1234",
        "567.89",
    )
    conn.exec_drop("INSERT INTO test_str_types VALUES (?, ?, ?, ?, ?)", test_values)

    result = conn.query_first("SELECT * FROM test_str_types")
    assert result
    # result already accessible via indexing
    assert result[0] == test_values[0].decode()
    assert result[1] == test_values[1]
    assert result[2] == test_values[2].encode()
    assert result[3] == int(test_values[3])
    assert result[4] == float(test_values[4])

    conn.query_drop("DROP TABLE test_str_types")
    conn.close()


def test_bytearray_type(backend):
    """Test bytearray type handling."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_bytearray_types")
    conn.query_drop(
        """
        CREATE TABLE test_bytearray_types (
            binary_val VARBINARY(255),
            blob_val BLOB
        )
    """
    )

    test_data = bytearray(b"Hello\x00\x01\x02\x03World")
    conn.exec_drop(
        "INSERT INTO test_bytearray_types VALUES (?, ?)", (test_data, test_data)
    )

    result = conn.query_first("SELECT * FROM test_bytearray_types")
    assert result
    binary_val, blob_val = result[0], result[1]
    # MySQL returns bytes, not bytearray
    assert binary_val == bytes(test_data)
    assert blob_val == bytes(test_data)

    conn.query_drop("DROP TABLE test_bytearray_types")
    conn.close()


def test_tuple_type(backend):
    """Test tuple type handling (as parameter binding)."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_tuple_types")
    conn.query_drop(
        """
        CREATE TABLE test_tuple_types (
            val1 INT,
            val2 VARCHAR(255),
            val3 FLOAT
        )
    """
    )

    test_tuple = (42, "hello", 3.14)
    conn.exec_drop("INSERT INTO test_tuple_types VALUES (?, ?, ?)", test_tuple)

    val1, val2, val3 = conn.query_first("SELECT * FROM test_tuple_types")
    assert val1 == 42
    assert val2 == "hello"
    assert isinstance(val3, float) and abs(val3 - 3.14) < 0.001

    conn.query_drop("DROP TABLE test_tuple_types")
    conn.close()


def test_list_type(backend):
    """Test list type handling (as parameter binding)."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_list_types")
    conn.query_drop(
        """
        CREATE TABLE test_list_types (
            val1 INT,
            val2 VARCHAR(255),
            val3 FLOAT
        )
    """
    )

    test_list = [100, "world", 2.718]
    conn.exec_drop("INSERT INTO test_list_types VALUES (?, ?, ?)", test_list)

    val1, val2, val3 = conn.query_first("SELECT * FROM test_list_types")
    assert val1 == 100
    assert val2 == "world"
    assert isinstance(val3, float) and abs(val3 - 2.718) < 0.001

    conn.query_drop("DROP TABLE test_list_types")
    conn.close()


def test_set_type(backend):
    """Test set type handling (converted to list for parameter binding)."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_set_types")
    conn.query_drop(
        """
        CREATE TABLE test_set_types (
            val1 INT,
            val2 VARCHAR(255)
        )
    """
    )

    # Convert set to list for consistent ordering
    test_set = {123, "test"}
    test_params = list(test_set)
    # Ensure consistent ordering for testing
    if isinstance(test_params[0], str):
        test_params = [test_params[1], test_params[0]]

    conn.exec_drop("INSERT INTO test_set_types VALUES (?, ?)", test_params)

    result = conn.query_first("SELECT * FROM test_set_types")
    assert result
    val1, val2 = result[0], result[1]
    assert val1 == 123
    assert val2 == "test"

    conn.query_drop("DROP TABLE test_set_types")
    conn.close()


def test_frozenset_type(backend):
    """Test frozenset type handling (converted to list for parameter binding)."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_frozenset_types")
    conn.query_drop(
        """
        CREATE TABLE test_frozenset_types (
            val1 INT,
            val2 VARCHAR(255)
        )
    """
    )

    # Convert frozenset to list for consistent ordering
    test_frozenset = frozenset({456, "frozen"})
    test_params = list(test_frozenset)
    # Ensure consistent ordering for testing
    if isinstance(test_params[0], str):
        test_params = [test_params[1], test_params[0]]

    conn.exec_drop("INSERT INTO test_frozenset_types VALUES (?, ?)", test_params)

    result = conn.query_first("SELECT * FROM test_frozenset_types")
    assert result
    val1, val2 = result[0], result[1]
    assert val1 == 456
    assert val2 == "frozen"

    conn.query_drop("DROP TABLE test_frozenset_types")
    conn.close()


def test_dict_type(backend):
    """Test basic data type handling."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_dict_types")
    conn.query_drop(
        """
        CREATE TABLE test_dict_types (
            name VARCHAR(255),
            age INT,
            score FLOAT
        )
    """
    )

    conn.exec_drop(
        "INSERT INTO test_dict_types (name, age, score) VALUES (?, ?, ?)",
        ("Alice", 30, 95.5),
    )

    name, age, score = conn.query_first("SELECT * FROM test_dict_types")
    assert name == "Alice"
    assert age == 30
    assert isinstance(score, float) and abs(score - 95.5) < 0.001

    conn.query_drop("DROP TABLE test_dict_types")
    conn.close()


def test_datetime_types(backend):
    """Test datetime.* types handling."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_datetime_types")
    conn.query_drop(
        """
        CREATE TABLE test_datetime_types (
            date_val DATE,
            time_val TIME,
            datetime_val DATETIME
        )
    """
    )

    test_date = date(2023, 12, 25)
    test_time = dt_time(15, 30, 45)
    test_datetime = datetime(2023, 12, 25, 15, 30, 45)

    conn.exec_drop(
        "INSERT INTO test_datetime_types VALUES (?, ?, ?)",
        (test_date, test_time, test_datetime),
    )

    date_val, time_val, datetime_val = conn.query_first(
        "SELECT * FROM test_datetime_types"
    )
    # Verify the types and values
    assert isinstance(date_val, date)
    assert date_val == test_date

    # time might be returned as timedelta or time, check value
    assert isinstance(time_val, timedelta)
    # TODO
    # assert time_val.hour == 15
    # assert time_val.minute == 30
    # assert time_val.second == 45

    assert isinstance(datetime_val, datetime)
    assert datetime_val == test_datetime

    conn.query_drop("DROP TABLE test_datetime_types")
    conn.close()


def test_struct_time_type(backend):
    """Test time.struct_time type handling."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_struct_time_types")
    conn.query_drop(
        """
        CREATE TABLE test_struct_time_types (
            timestamp_val TIMESTAMP
        )
    """
    )

    # Create a struct_time and convert to datetime for insertion
    test_struct_time = time.struct_time((2023, 12, 25, 15, 30, 45, 0, 359, 0))
    test_datetime = datetime(*test_struct_time[:6])

    conn.exec_drop("INSERT INTO test_struct_time_types VALUES (?)", (test_datetime,))

    result = conn.query_first("SELECT * FROM test_struct_time_types")
    assert result
    timestamp_val = result[0]

    assert isinstance(timestamp_val, datetime)
    assert timestamp_val.year == 2023
    assert timestamp_val.month == 12
    assert timestamp_val.day == 25
    assert timestamp_val.hour == 15
    assert timestamp_val.minute == 30
    assert timestamp_val.second == 45

    conn.query_drop("DROP TABLE test_struct_time_types")
    conn.close()


def test_decimal_type(backend):
    """Test decimal.Decimal type handling."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_decimal_types")
    conn.query_drop(
        """
        CREATE TABLE test_decimal_types (
            decimal_val DECIMAL(10,2),
            numeric_val NUMERIC(15,4)
        )
    """
    )

    test_decimal = Decimal("123.45")
    test_numeric = Decimal("12345.6789")

    conn.exec_drop(
        "INSERT INTO test_decimal_types VALUES (?, ?)", (test_decimal, test_numeric)
    )

    result = conn.query_first("SELECT * FROM test_decimal_types")
    assert result
    decimal_val, numeric_val = result[0], result[1]

    assert isinstance(decimal_val, Decimal)
    assert isinstance(numeric_val, Decimal)
    assert decimal_val == Decimal("123.45")
    assert numeric_val == Decimal("12345.6789")

    conn.query_drop("DROP TABLE test_decimal_types")
    conn.close()


def test_combined_data_types(backend):
    """Test a combination of different data types in a single query."""
    conn = Conn(get_test_db_url(), backend=backend)

    conn.query_drop("DROP TABLE IF EXISTS test_combined_types")
    conn.query_drop(
        """
        CREATE TABLE test_combined_types (
            null_val INT,
            int_val INT,
            float_val FLOAT,
            str_val VARCHAR(255),
            binary_val VARBINARY(255),
            date_val DATE,
            datetime_val DATETIME,
            decimal_val DECIMAL(10,2)
        )
    """
    )

    test_values = (
        None,
        42,
        3.14,
        "test string",
        bytearray(b"binary\x00data"),
        date(2023, 1, 1),
        datetime(2023, 1, 1, 12, 0, 0),
        Decimal("99.99"),
    )

    conn.exec_drop(
        "INSERT INTO test_combined_types VALUES (?, ?, ?, ?, ?, ?, ?, ?)", test_values
    )

    result = conn.query_first("SELECT * FROM test_combined_types")
    assert result
    values = tuple(result[i] for i in range(len(result)))

    assert values[0] is None
    assert values[1] == 42
    assert isinstance(values[2], float) and abs(values[2] - 3.14) < 0.001
    assert values[3] == "test string"
    assert values[4] == b"binary\x00data"  # MySQL returns bytes
    assert values[5] == date(2023, 1, 1)
    assert values[6] == datetime(2023, 1, 1, 12, 0, 0)
    assert values[7] == Decimal("99.99")

    conn.query_drop("DROP TABLE test_combined_types")
    conn.close()
