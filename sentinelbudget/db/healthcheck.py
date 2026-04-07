from __future__ import annotations

import sys

from pydantic import ValidationError

from sentinelbudget.config import get_settings
from sentinelbudget.db.engine import verify_db_connectivity, verify_pgvector_readiness
from sentinelbudget.logging import setup_logging


def run_healthcheck() -> int:
    """Run startup preflight checks and return process exit code."""

    logger = setup_logging()
    command_context = {"command": "healthcheck"}

    try:
        settings = get_settings()
    except ValidationError as exc:
        logger.error(
            "Configuration validation failed",
            extra={"errors": exc.errors(), **command_context},
        )
        return 1

    logger.info(
        "Configuration validation passed",
        extra={"environment": settings.sentinel_env, **command_context},
    )

    db_ok, db_message = verify_db_connectivity(settings)
    if not db_ok:
        logger.error(
            "Database connectivity check failed",
            extra={"detail": db_message, **command_context},
        )
        return 1
    logger.info(
        "Database connectivity check passed",
        extra={"detail": db_message, **command_context},
    )

    vector_ok, vector_message = verify_pgvector_readiness(settings)
    if not vector_ok:
        logger.error(
            "pgvector readiness check failed",
            extra={"detail": vector_message, **command_context},
        )
        return 1
    logger.info(
        "pgvector readiness check passed",
        extra={"detail": vector_message, **command_context},
    )

    logger.info("All healthchecks passed", extra=command_context)
    return 0


def main() -> None:
    sys.exit(run_healthcheck())


if __name__ == "__main__":
    main()
