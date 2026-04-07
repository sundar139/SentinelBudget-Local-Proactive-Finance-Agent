from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from psycopg import Connection

from sentinelbudget.config import Settings
from sentinelbudget.db.engine import create_connection


@contextmanager
def get_db_connection(settings: Settings | None = None) -> Iterator[Connection]:
    """Yield a PostgreSQL connection and ensure it is closed."""

    conn = create_connection(settings)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(settings: Settings | None = None) -> Iterator[Connection]:
    """Yield a transactional connection that commits or rolls back safely."""

    with get_db_connection(settings) as conn:
        with conn.transaction():
            yield conn
