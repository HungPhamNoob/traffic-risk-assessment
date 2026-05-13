"""PostgreSQL helpers used by API services."""

from collections.abc import Iterable
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.errors
import psycopg2.extras

from app.core.config import get_settings


@contextmanager
def get_connection():
    """Open a PostgreSQL connection and close it after the caller finishes."""
    settings = get_settings()
    connection = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )
    try:
        yield connection
    finally:
        connection.close()


def fetch_one(
    query: str, params: Iterable[Any] | dict[str, Any] | None = None
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
    query: str, params: Iterable[Any] | dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Execute a read query and return all rows as dictionaries."""
    with get_connection() as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            try:
                cursor.execute(query, params)
            except psycopg2.errors.UndefinedTable:
                return []
            return [dict(row) for row in cursor.fetchall()]
