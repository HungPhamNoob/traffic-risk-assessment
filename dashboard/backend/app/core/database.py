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
RECOVERABLE_READ_ERRORS = (
    psycopg2.InterfaceError,
    psycopg2.OperationalError,
    psycopg2.pool.PoolError,
)


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


def reset_pg_pool() -> None:
    """Drop the shared pool so the next read can rebuild fresh connections."""
    global PG_POOL
    if PG_POOL is not None:
        try:
            PG_POOL.closeall()
        except Exception:
            pass
    PG_POOL = None


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
                pool.putconn(connection, close=bool(getattr(connection, "closed", False)))
            except Exception:
                connection.close()
        else:
            if not bool(getattr(connection, "closed", False)):
                connection.close()


def _run_read_query(
    query: Any,
    params: Iterable[Any] | dict[str, Any] | None,
    *,
    fetch_many: bool,
) -> dict[str, Any] | list[dict[str, Any]] | None:
    for attempt in range(2):
        try:
            with get_connection() as connection:
                with connection.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    try:
                        cursor.execute(query, params)
                    except psycopg2.errors.UndefinedTable:
                        return [] if fetch_many else None
                    except RECOVERABLE_READ_ERRORS:
                        # Drop dead pooled connections so the next attempt borrows
                        # a fresh backend instead of surfacing transient HTTP 500s.
                        try:
                            connection.close()
                        except Exception:
                            pass
                        reset_pg_pool()
                        if attempt == 0:
                            continue
                        return [] if fetch_many else None
                    if fetch_many:
                        return [dict(row) for row in cursor.fetchall()]
                    row = cursor.fetchone()
                    return dict(row) if row else None
        except RECOVERABLE_READ_ERRORS:
            reset_pg_pool()
            if attempt == 0:
                continue
            return [] if fetch_many else None
    return [] if fetch_many else None


def fetch_one(
    query: Any, params: Iterable[Any] | dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """Execute a read query and return one row as a dictionary."""
    result = _run_read_query(query, params, fetch_many=False)
    return result if isinstance(result, dict) or result is None else None


def fetch_all(
    query: Any, params: Iterable[Any] | dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Execute a read query and return all rows as dictionaries."""
    result = _run_read_query(query, params, fetch_many=True)
    return result if isinstance(result, list) else []
