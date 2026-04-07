from __future__ import annotations

import argparse
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config

from sentinelbudget.config import get_settings
from sentinelbudget.db.engine import build_postgres_dsn
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.db.schema import bootstrap_default_categories
from sentinelbudget.logging import setup_logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI_PATH = PROJECT_ROOT / "alembic.ini"
MIGRATIONS_PATH = PROJECT_ROOT / "migrations"


def _build_alembic_config() -> Config:
    cfg = Config(str(ALEMBIC_INI_PATH))
    cfg.set_main_option("script_location", str(MIGRATIONS_PATH))
    cfg.set_main_option("sqlalchemy.url", build_postgres_dsn(get_settings()))
    return cfg


def run_migrations(revision: str = "head") -> None:
    """Run Alembic migrations to the requested revision."""

    command.upgrade(_build_alembic_config(), revision)


def initialize_database(seed_categories: bool) -> int:
    """Initialize schema and optionally seed baseline categories."""

    settings = get_settings()
    logger = setup_logging(settings.log_level)
    logger.info(
        "Database initialization started",
        extra={
            "command": "db-init",
            "seed_categories": seed_categories,
        },
    )

    run_migrations("head")
    logger.info(
        "Migrations applied successfully",
        extra={"command": "db-init", "revision": "head"},
    )

    inserted = 0
    if seed_categories:
        with transaction(settings) as conn:
            inserted = bootstrap_default_categories(conn)
        logger.info(
            "Default categories bootstrapped",
            extra={"command": "db-init", "inserted": inserted},
        )

    logger.info(
        "Database initialization completed",
        extra={
            "command": "db-init",
            "seed_categories": seed_categories,
            "categories_inserted": inserted,
        },
    )

    return inserted


def migrate_main() -> None:
    parser = argparse.ArgumentParser(description="Run SentinelBudget database migrations")
    parser.add_argument(
        "--revision",
        default="head",
        help="Alembic revision target (default: head)",
    )
    args = parser.parse_args()

    settings = get_settings()
    logger = setup_logging(settings.log_level)

    logger.info(
        "Database migration command started",
        extra={"command": "db-migrate", "revision": args.revision},
    )

    try:
        run_migrations(args.revision)
        logger.info(
            "Database migration command completed",
            extra={"command": "db-migrate", "revision": args.revision},
        )
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Database migration failed",
            extra={
                "command": "db-migrate",
                "revision": args.revision,
                "detail": str(exc),
            },
        )
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize SentinelBudget database")
    parser.add_argument(
        "--no-seed-categories",
        action="store_true",
        help="Run migrations but skip default category bootstrap",
    )
    args = parser.parse_args()

    settings = get_settings()
    logger = setup_logging(settings.log_level)

    try:
        initialize_database(seed_categories=not args.no_seed_categories)
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Database initialization failed",
            extra={
                "command": "db-init",
                "seed_categories": not args.no_seed_categories,
                "detail": str(exc),
            },
        )
        sys.exit(1)
