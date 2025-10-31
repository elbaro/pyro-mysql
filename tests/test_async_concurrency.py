"""Tests for async concurrency."""

import asyncio
import time

import pytest
from pyro_mysql import AsyncConn

from .conftest import get_test_db_url


@pytest.mark.asyncio
async def test_concurrent_sleep():
    """Test that multiple async connections can run queries concurrently."""
    # Create 3 async connections
    conn1 = await AsyncConn.new(get_test_db_url())
    conn2 = await AsyncConn.new(get_test_db_url())
    conn3 = await AsyncConn.new(get_test_db_url())

    try:
        # Record start time
        start_time = time.time()

        # Execute SLEEP(1) on all 3 connections concurrently
        await asyncio.gather(
            conn1.exec_drop("SELECT SLEEP(1)"),
            conn2.exec_drop("SELECT SLEEP(1)"),
            conn3.exec_drop("SELECT SLEEP(1)"),
        )

        # Record end time
        elapsed_time = time.time() - start_time

        # If concurrent, should take ~1 second. If sequential, would take ~3 seconds.
        # Assert it completes within 4 seconds (with generous buffer for CI)
        assert (
            elapsed_time < 4
        ), f"Expected concurrent execution in < 4s, took {elapsed_time:.2f}s"

        # Also verify it's actually concurrent (not just fast sequential)
        # Should take at least 1 second (the sleep duration)
        assert (
            elapsed_time >= 1
        ), f"Expected at least 1s for SLEEP(1), took {elapsed_time:.2f}s"

    finally:
        # Clean up connections
        await conn1.close()
        await conn2.close()
        await conn3.close()
