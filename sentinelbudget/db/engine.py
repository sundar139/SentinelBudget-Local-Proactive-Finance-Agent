from __future__ import annotations

from typing import cast
from urllib.parse import quote_plus

import psycopg
from psycopg import Connection

from sentinelbudget.config import Settings, get_settings


def build_postgres_dsn(settings: Settings | None = None) -> str:
    """Build a SQLAlchemy-compatible PostgreSQL DSN from validated settings."""

    cfg = settings or get_settings()
    user = quote_plus(cfg.postgres_user)
    password = quote_plus(cfg.postgres_password.get_secret_value())
    host = cfg.postgres_host
    port = cfg.postgres_port
    database = quote_plus(cfg.postgres_db)
    sslmode = quote_plus(cfg.postgres_sslmode)
    connect_timeout = cfg.postgres_connect_timeout

    return (
        f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
        f"?sslmode={sslmode}&connect_timeout={connect_timeout}"
    )


def create_connection(settings: Settings | None = None) -> Connection:
    """Create a PostgreSQL connection from validated settings."""

    cfg = settings or get_settings()
    return psycopg.connect(
        host=cfg.postgres_host,
        port=cfg.postgres_port,
        dbname=cfg.postgres_db,
        user=cfg.postgres_user,
        password=cfg.postgres_password.get_secret_value(),
        sslmode=cfg.postgres_sslmode,
        connect_timeout=cfg.postgres_connect_timeout,
    )


def verify_db_connectivity(settings: Settings | None = None) -> tuple[bool, str]:
    """Check whether a database connection can be established."""

    cfg = settings or get_settings()

    try:
        with create_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return True, "Database connectivity check passed."
    except Exception as exc:  # pragma: no cover
        return False, f"Database connectivity check failed: {exc}"


def verify_pgvector_readiness(settings: Settings | None = None) -> tuple[bool, str]:
    """Check whether pgvector is enabled or available in PostgreSQL."""

    cfg = settings or get_settings()
    extension_name = cfg.pgvector_extension_name

    try:
        with create_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = %s);",
                    (extension_name,),
                )
                enabled_row = cur.fetchone()
                extension_enabled = (
                    bool(cast(tuple[bool], enabled_row)[0])
                    if enabled_row
                    else False
                )

                if extension_enabled:
                    return True, "pgvector extension is enabled in the target database."

                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = %s);",
                    (extension_name,),
                )
                available_row = cur.fetchone()
                extension_available = (
                    bool(cast(tuple[bool], available_row)[0])
                    if available_row
                    else False
                )

                if extension_available:
                    return (
                        False,
                        "pgvector extension is installed on this PostgreSQL server but not enabled "
                        "for the target database. Run: CREATE EXTENSION vector;",
                    )

                return (
                    False,
                    "pgvector extension is not installed on this PostgreSQL server. "
                    "Install pgvector package and restart PostgreSQL.",
                )
    except Exception as exc:  # pragma: no cover
        return False, f"pgvector readiness check failed: {exc}"
