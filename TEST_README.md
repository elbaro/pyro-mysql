# Test Suite for pyro-mysql

This test suite provides comprehensive coverage for the pyro-mysql MySQL driver, based on test cases from established Python MySQL drivers including aiomysql, PyMySQL, asyncmy, and mysqlclient.

## Test Structure

### Async Tests (`tests/test_async_*.py`)

1. **Connection Tests** (`test_async_connection.py`)
   - Basic connection establishment
   - Connection with specific database
   - Connection timeout handling  
   - Connection ping and reset
   - Server information retrieval
   - Charset configuration
   - Autocommit behavior
   - SSL/TLS connections
   - Init command execution
   - Large data transfer
   - Error scenarios (wrong credentials, invalid host)

2. **Query Tests** (`test_async_query.py`)
   - Basic SELECT queries
   - Parameterized queries with positional and named parameters
   - Query result iteration and mapping
   - Batch query execution
   - NULL value handling
   - Multi-statement queries
   - Last insert ID and affected rows

3. **Transaction Tests** (`test_async_transaction.py`)
   - Basic transaction commit/rollback
   - Transaction isolation levels
   - Nested transactions with savepoints
   - Transaction error handling
   - Concurrent transaction behavior
   - Read-only transactions
   - Consistent snapshot transactions
   - Auto-rollback on drop

4. **Data Type Tests** (`test_async_data_types.py`)
   - Integer types (TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT)
   - Floating point types (FLOAT, DOUBLE, DECIMAL)
   - String types (CHAR, VARCHAR, TEXT, BINARY, VARBINARY, BLOB)
   - Date/time types (DATE, TIME, DATETIME, TIMESTAMP)
   - Boolean type
   - NULL value handling

5. **Pool Tests** (`test_async_pool.py`)
   - Basic connection pooling
   - Pool constraints and configuration
   - Concurrent connection usage
   - Connection reuse and reset
   - Maximum connections handling
   - Transactions with pools

6. **Error Handling Tests** (`test_async_errors.py`)
   - Connection timeout errors
   - Invalid credentials
   - Database not found
   - SQL syntax errors
   - Table not found
   - Duplicate key constraints
   - Data too long errors
   - Connection lost scenarios
   - Foreign key constraint errors
   - Pool connection errors

### Sync Tests (`tests/test_sync_*.py`)

1. **Sync Connection Tests** (`test_sync_connection.py`)
   - Equivalent tests to async connection tests but using synchronous API

2. **Sync Query Tests** (`test_sync_query.py`)
   - Equivalent tests to async query tests but using synchronous API

## Running Tests

### Prerequisites

1. **MySQL Server**: Ensure MySQL server is running and accessible
2. **Test Database**: Create a test database (default: `test`)
3. **Environment Variables**: Set connection parameters
4. **Python Dependencies**: Install pytest and required packages

```bash
export TEST_DATABASE_URL="mysql://root:password@localhost:3306/test"
pip install pytest pytest-asyncio
```

### Running All Tests

```bash
pytest tests/
```

### Running Specific Test Categories

```bash
# Async tests only
pytest tests/test_async_*.py

# Sync tests only  
pytest tests/test_sync_*.py

# Connection tests only
pytest tests/ -k "connection"

# Query tests only
pytest tests/ -k "query"

# Transaction tests only
pytest tests/ -k "transaction"

# Data type tests only
pytest tests/ -k "data_types"

# Pool tests only
pytest tests/ -k "pool"

# Error tests only
pytest tests/ -k "error"
```

### Running Individual Tests

```bash
# Run a specific test
pytest tests/test_async_connection.py::test_basic_connection

# Run with verbose output
pytest tests/ -v

# Run with output capture disabled
pytest tests/ -s

# Run with multiple workers (requires pytest-xdist)
pytest tests/ -n 4
```

## Test Configuration

### Database Setup

The test suite requires a MySQL database for testing. By default, it expects:
- Host: localhost
- Port: 3306
- Username: root
- Password: password  
- Database: test

You can override these settings with the `TEST_DATABASE_URL` environment variable:

```bash
export TEST_DATABASE_URL="mysql://user:pass@host:port/database"
```

### Test Data

Tests automatically create and clean up their own test tables. The common test table structure is:

```sql
CREATE TABLE test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    age INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
```

## Test Coverage

The test suite covers:

- ✅ Connection management and configuration
- ✅ Query execution (SELECT, INSERT, UPDATE, DELETE)
- ✅ Prepared statements with parameters
- ✅ Transaction handling and isolation levels  
- ✅ All MySQL data types
- ✅ Connection pooling
- ✅ Error handling and recovery
- ✅ SSL/TLS connections
- ✅ Character set handling
- ✅ Large data transfers
- ✅ Concurrent operations
- ✅ Both async and sync APIs

## Test Data Sources

This test suite is based on test cases from established Python MySQL drivers:

1. **aiomysql** - Async MySQL driver test patterns
2. **PyMySQL** - Pure Python MySQL driver tests  
3. **asyncmy** - Fast async MySQL driver tests
4. **mysqlclient** - C-based MySQL driver tests

The tests ensure compatibility and correctness across different usage patterns and edge cases found in these mature drivers.

## Troubleshooting

### Common Issues

1. **Connection Failed**: Verify MySQL server is running and credentials are correct
2. **Permission Denied**: Ensure test user has necessary privileges
3. **Database Not Found**: Create the test database or update the connection URL
4. **Timeout Errors**: Check network connectivity and server responsiveness

### Debug Output

Enable debug logging for more verbose test output:

```bash
pytest tests/ -s --log-cli-level=DEBUG
```

### Pytest Configuration

Create a `pytest.ini` file in the project root for additional configuration:

```ini
[tool:pytest]
asyncio_mode = auto
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
markers =
    slow: marks tests as slow (deselect with '-m "not slow"')
    integration: marks tests as integration tests
```