from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from psycopg import Connection
from sentinelbudget.config import get_settings
from sentinelbudget.db.engine import create_connection
from sentinelbudget.db.init_db import run_migrations


@pytest.fixture(scope="session")
def integration_enabled() -> None:
    if os.getenv("SENTINEL_INTEGRATION_DB") != "1":
        pytest.skip("Set SENTINEL_INTEGRATION_DB=1 to run integration tests")


@pytest.fixture(scope="session")
def migrated_database(integration_enabled: None) -> None:
    get_settings.cache_clear()
    run_migrations("head")


@pytest.fixture()
def db_conn(migrated_database: None) -> Iterator[Connection]:
    settings = get_settings()
    conn = create_connection(settings)
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
        yield conn
    finally:
        conn.rollback()
        conn.close()
