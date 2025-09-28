import pyro_mysql
import pytest


def test_url():
    with pytest.raises(pyro_mysql.error.UrlError):
        conn = pyro_mysql.sync.Conn("seaweed")
