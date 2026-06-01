"""PostgreSQL helpers used by API services."""

from collections.abc import Iterable
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.errors
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

from app.core.config import get_settings


PG_POOL: SimpleConnectionPool | None = None


def get_pg_pool() -> SimpleConnectionPool:
    """Create the shared PostgreSQL connection pool lazily."""
    global PG_POOL
    if PG_POOL is None:
        settings = get_settings()
        PG_POOL = SimpleConnectionPool(
            minconn=max(1, settings.postgres_pool_min_conn),
            maxconn=max(1, settings.postgres_pool_max_conn),
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )
    return PG_POOL


@contextmanager
def get_connection():
    """Borrow a PostgreSQL connection from the shared pool."""
    pool = get_pg_pool()
    borrowed_from_pool = True
    try:
        connection = pool.getconn()
    except psycopg2.pool.PoolError:
        settings = get_settings()
        # Fall back to a direct connection instead of returning HTTP 500 when
        # a burst of dashboard requests temporarily exhausts the shared pool.
        connection = psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )
        borrowed_from_pool = False
    try:
        yield connection
    finally:
        if borrowed_from_pool:
            try:
                pool.putconn(connection)
            except Exception:
                connection.close()
        else:
            connection.close()


def fetch_one(
    query: Any, params: Iterable[Any] | dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """Execute a read query and return one row as a dictionary."""
    with get_connection() as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            try:
                cursor.execute(query, params)
            except psycopg2.errors.UndefinedTable:
                return None
            row = cursor.fetchone()
            return dict(row) if row else None


def fetch_all(
    query: Any, params: Iterable[Any] | dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Execute a read query and return all rows as dictionaries."""
    with get_connection() as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            try:
                cursor.execute(query, params)
            except psycopg2.errors.UndefinedTable:
                return []
            return [dict(row) for row in cursor.fetchall()]
