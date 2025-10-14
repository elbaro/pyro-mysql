import os

import pyro_mysql


def get_test_db_url() -> str:
    """Get the test database URL from environment or default."""
    return os.environ.get("TEST_DATABASE_URL", "mysql://test:1234@localhost:3306/test")


def test_float_rounding():
    # Connect to MySQL using sync API
    conn = pyro_mysql.SyncConn(get_test_db_url())

    try:
        # Drop existing table if any
        conn.exec_drop("DROP TABLE IF EXISTS test_float_rounding")

        # Create a test table with FLOAT and DOUBLE columns with specific decimal places
        conn.exec_drop(
            """
            CREATE TABLE test_float_rounding (
                id INT PRIMARY KEY,
                float_val FLOAT(10, 3),
                double_val DOUBLE(15, 3),
                float_no_spec FLOAT,
                double_no_spec DOUBLE
            )
        """
        )

        # Insert test data
        conn.exec_drop(
            """
            INSERT INTO test_float_rounding VALUES
            (1, 46.58300018310547, 46.58300018310547, 46.58300018310547, 46.58300018310547)
        """
        )

        # Also test with 46.183
        conn.exec_drop(
            """
            INSERT INTO test_float_rounding VALUES
            (2, 46.183, 46.183, 46.183, 46.183)
        """
        )

        # Query the data back
        rows = conn.exec("SELECT * FROM test_float_rounding WHERE id = 1")
        row = rows[0].to_tuple()

        # Query second row with 46.183
        rows2 = conn.exec("SELECT * FROM test_float_rounding WHERE id = 2")
        row2 = rows2[0].to_tuple()

        # Verify rounding worked for first row
        assert row[1] == 46.583, f"Expected 46.583 for float_val, got {row[1]}"
        assert row[2] == 46.583, f"Expected 46.583 for double_val, got {row[2]}"

        # Check second row
        assert row2[1] == 46.183, f"Expected 46.183 for float_val, got {row2[1]}"
        assert row2[2] == 46.183, f"Expected 46.183 for double_val, got {row2[2]}"

        # Clean up
        conn.exec_drop("DROP TABLE IF EXISTS test_float_rounding")

    finally:
        conn.close()
