"""
Custom SQLAlchemy testing requirements for pyro_mysql.

This module provides a custom Requirements class that properly handles
pyro_mysql's behavior in SQLAlchemy's test suite by adding it to the
appropriate exclusion lists for MySQL drivers.

Usage:
    When running SQLAlchemy tests with pyro_mysql, use:

    pytest --requirements pyro_mysql.testing.requirements:PyroMySQLRequirements \
           --dburi=mariadb+pyro_mysql://user:pass@host/db \
           test/dialect/test_suite.py

    Or add this to setup.cfg:

    [sqla_testing]
    requirement_cls = pyro_mysql.testing.requirements:PyroMySQLRequirements
"""

from sqlalchemy.testing import exclusions

# Import the base requirements class with fallback
try:
    from sqlalchemy.testing.requirements import DefaultRequirements
    _BaseRequirements = DefaultRequirements
except ImportError:
    # Fallback to SuiteRequirements if DefaultRequirements not available
    from sqlalchemy.testing.requirements import SuiteRequirements
    _BaseRequirements = SuiteRequirements


class PyroMySQLRequirements(_BaseRequirements):
    """
    Custom requirements class for pyro_mysql testing.

    This class inherits all requirements from SQLAlchemy's DefaultRequirements
    but overrides the implicit bound requirements to include pyro_mysql in
    the exclusion lists alongside other MySQL drivers.
    """

    @property
    def date_implicit_bound(self):
        """
        Target dialect when given a date object will bind it such
        that the database server knows the object is a date, and not
        a plain string.

        Skips this test for pyro_mysql and other MySQL drivers that don't support this.
        """
        return exclusions.skip_if([
            "+mysqldb",
            "+pymysql",
            "+asyncmy",
            "+mysqlconnector",
            "+cymysql",
            "+aiomysql",
            "+pyro_mysql",  # Added for pyro_mysql
        ])

    @property
    def time_implicit_bound(self):
        """
        Target dialect when given a time object will bind it such
        that the database server knows the object is a time, and not
        a plain string.

        Skips this test for pyro_mysql and other MySQL drivers that don't support this.
        """
        return exclusions.skip_if([
            "+mysqldb",
            "+pymysql",
            "+asyncmy",
            "+mysqlconnector",
            "+mariadbconnector",
            "+cymysql",
            "+aiomysql",
            "+pyro_mysql",  # Added for pyro_mysql
        ])

    @property
    def datetime_implicit_bound(self):
        """
        Target dialect when given a datetime object will bind it such
        that the database server knows the object is a date, and not
        a plain string.

        Skips this test for pyro_mysql and other MySQL drivers that don't support this.
        """
        return exclusions.skip_if([
            "+mysqldb",
            "+pymysql",
            "+asyncmy",
            "+mysqlconnector",
            "+cymysql",
            "+aiomysql",
            "+pymssql",
            "+pyro_mysql",  # Added for pyro_mysql
        ])