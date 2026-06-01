import sys
from pathlib import Path

import psycopg2

BACKEND_PATH = Path(__file__).resolve().parents[2] / "dashboard" / "backend"
if str(BACKEND_PATH) not in sys.path:
    sys.path.insert(0, str(BACKEND_PATH))

from app.core import database  # noqa: E402


def test_get_connection_falls_back_when_pool_is_exhausted(monkeypatch):
    class FakePool:
        def getconn(self):
            raise psycopg2.pool.PoolError("pool exhausted")

        def putconn(self, connection):
            raise AssertionError("Fallback connections must not be returned to the pool")

    class FakeConnection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    settings = type(
        "Settings",
        (),
        {
            "postgres_host": "localhost",
            "postgres_port": 5432,
            "postgres_db": "capstone_db",
            "postgres_user": "capstone",
            "postgres_password": "123",
        },
    )()
    fallback_connection = FakeConnection()

    monkeypatch.setattr(database, "get_pg_pool", lambda: FakePool())
    monkeypatch.setattr(database, "get_settings", lambda: settings)
    monkeypatch.setattr(database.psycopg2, "connect", lambda **kwargs: fallback_connection)

    with database.get_connection() as connection:
        assert connection is fallback_connection

    assert fallback_connection.closed is True
