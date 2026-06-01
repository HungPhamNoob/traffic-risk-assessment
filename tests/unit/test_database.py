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

        def putconn(self, connection, close=False):
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


def test_fetch_one_retries_with_fresh_connection_after_operational_error(monkeypatch):
    class FakeCursor:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            self.connection.execute_attempts += 1
            if self.connection.fail_on_execute:
                raise psycopg2.OperationalError("server closed the connection unexpectedly")

        def fetchone(self):
            return {"exists": True}

    class FakeConnection:
        def __init__(self, fail_on_execute=False):
            self.fail_on_execute = fail_on_execute
            self.execute_attempts = 0
            self.closed = False

        def cursor(self, cursor_factory=None):
            return FakeCursor(self)

        def close(self):
            self.closed = True

    class FakePool:
        def __init__(self):
            self.connections = [
                FakeConnection(fail_on_execute=True),
                FakeConnection(fail_on_execute=False),
            ]
            self.put_calls = []

        def getconn(self):
            return self.connections.pop(0)

        def putconn(self, connection, close=False):
            self.put_calls.append((connection, close))

    pool = FakePool()
    monkeypatch.setattr(database, "get_pg_pool", lambda: pool)

    row = database.fetch_one("SELECT 1")

    assert row == {"exists": True}
    assert len(pool.put_calls) == 2
    failed_connection, failed_close = pool.put_calls[0]
    recovered_connection, recovered_close = pool.put_calls[1]
    assert failed_connection.closed is True
    assert failed_close is True
    assert recovered_connection.closed is False
    assert recovered_close is False
