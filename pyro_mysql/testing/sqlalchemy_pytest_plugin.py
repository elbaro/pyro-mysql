"""Pytest plugin to load PyroMySQLRequirements for SQLAlchemy testing."""

import os
import sys


def pytest_sessionstart(session):
    """Load PyroMySQLRequirements after session starts to override any defaults."""
    try:
        from sqlalchemy.testing.plugin.plugin_base import _setup_requirements

        _setup_requirements("pyro_mysql.testing.requirements:PyroMySQLRequirements")
        print("✓ sqlalchemy_pytest_plugin: Loaded PyroMySQLRequirements")
    except Exception as e:
        print(f"✗ sqlalchemy_pytest_plugin: Failed to load PyroMySQLRequirements: {e}")
