from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from importlib.util import find_spec
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pydantic import ValidationError

from sentinelbudget.config import Settings, get_settings
from sentinelbudget.db.engine import verify_db_connectivity, verify_pgvector_readiness
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.logging import setup_logging

_REQUIRED_TABLES: tuple[str, ...] = (
    "users",
    "accounts",
    "categories",
    "budgets",
    "goals",
    "user_preferences",
    "ledger",
    "semantic_memory",
    "conversation_history",
    "insights",
    "alembic_version",
)


@dataclass(frozen=True, slots=True)
class PreflightCheck:
    name: str
    status: str
    detail: str
    required: bool
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PreflightSummary:
    generated_at: datetime
    checks: list[PreflightCheck]
    hard_failures: int
    warnings: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "hard_failures": self.hard_failures,
            "warnings": self.warnings,
            "checks": [asdict(item) for item in self.checks],
        }


def _check_config() -> tuple[Settings | None, PreflightCheck]:
    get_settings.cache_clear()
    try:
        settings = get_settings()
    except ValidationError as exc:
        first_error = exc.errors()[0] if exc.errors() else {"msg": "unknown validation error"}
        return None, PreflightCheck(
            name="config_validity",
            status="fail",
            detail=f"Configuration validation failed: {first_error}",
            required=True,
            meta={"error_count": len(exc.errors())},
        )

    return settings, PreflightCheck(
        name="config_validity",
        status="pass",
        detail="Configuration validation passed.",
        required=True,
        meta={"environment": settings.sentinel_env},
    )


def _check_db_connectivity(settings: Settings) -> PreflightCheck:
    ok, message = verify_db_connectivity(settings)
    return PreflightCheck(
        name="db_connectivity",
        status="pass" if ok else "fail",
        detail=message,
        required=True,
    )


def _check_pgvector(settings: Settings) -> PreflightCheck:
    ok, message = verify_pgvector_readiness(settings)
    return PreflightCheck(
        name="pgvector_readiness",
        status="pass" if ok else "fail",
        detail=message,
        required=True,
    )


def _check_schema_and_migrations(settings: Settings) -> PreflightCheck:
    missing_tables: list[str] = []
    alembic_version: str | None = None

    try:
        with transaction(settings) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = ANY(%s);
                    """,
                    (list(_REQUIRED_TABLES),),
                )
                present_rows = cur.fetchall()

                present_names = {str(row[0]) for row in present_rows}
                missing_tables = [name for name in _REQUIRED_TABLES if name not in present_names]

                if "alembic_version" in present_names:
                    cur.execute("SELECT version_num FROM alembic_version LIMIT 1;")
                    version_row = cur.fetchone()
                    if version_row is not None and version_row[0] is not None:
                        alembic_version = str(version_row[0])
    except Exception as exc:
        return PreflightCheck(
            name="schema_migrations",
            status="fail",
            detail=f"Schema check failed: {exc}",
            required=True,
        )

    if missing_tables:
        return PreflightCheck(
            name="schema_migrations",
            status="fail",
            detail="Required tables are missing.",
            required=True,
            meta={"missing_tables": missing_tables},
        )

    if alembic_version is None:
        return PreflightCheck(
            name="schema_migrations",
            status="fail",
            detail="alembic_version table is present but contains no version row.",
            required=True,
        )

    return PreflightCheck(
        name="schema_migrations",
        status="pass",
        detail="Required tables and migration version are present.",
        required=True,
        meta={"alembic_version": alembic_version},
    )


def _fetch_ollama_model_names(settings: Settings) -> tuple[set[str] | None, str]:
    url = str(settings.ollama_base_url).rstrip("/") + "/api/tags"
    request = Request(url=url, method="GET")

    try:
        with urlopen(request, timeout=settings.ollama_chat_timeout_seconds) as response:
            body_text = response.read().decode("utf-8")
    except HTTPError as exc:
        return None, f"HTTP {exc.code} {exc.reason}"
    except URLError as exc:
        return None, str(exc.reason)
    except Exception as exc:  # pragma: no cover
        return None, str(exc)

    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        return None, "Ollama /api/tags response was not valid JSON"

    models_obj = payload.get("models")
    if not isinstance(models_obj, list):
        return None, "Ollama /api/tags response missing models list"

    names: set[str] = set()
    for item in models_obj:
        if not isinstance(item, dict):
            continue
        for key in ("name", "model"):
            value = item.get(key)
            if isinstance(value, str) and value.strip() != "":
                names.add(value.strip())

    return names, "ok"


def _canonical_model_name(name: str) -> str:
    cleaned = name.strip()
    if cleaned.endswith(":latest"):
        return cleaned[: -len(":latest")]
    return cleaned


def _model_available(configured_model: str, available_models: set[str]) -> bool:
    normalized_available = {_canonical_model_name(item) for item in available_models}
    return _canonical_model_name(configured_model) in normalized_available


def _check_ollama(settings: Settings) -> list[PreflightCheck]:
    names, detail = _fetch_ollama_model_names(settings)

    if names is None:
        return [
            PreflightCheck(
                name="ollama_reachability",
                status="warn",
                detail=f"Unable to reach Ollama: {detail}",
                required=False,
            ),
            PreflightCheck(
                name="ollama_chat_model",
                status="warn",
                detail="Skipped because Ollama was unreachable.",
                required=False,
            ),
            PreflightCheck(
                name="ollama_embedding_model",
                status="warn",
                detail="Skipped because Ollama was unreachable.",
                required=False,
            ),
        ]

    checks = [
        PreflightCheck(
            name="ollama_reachability",
            status="pass",
            detail="Ollama is reachable.",
            required=False,
            meta={"available_model_count": len(names)},
        )
    ]

    chat_model = settings.ollama_chat_model
    embedding_model = settings.memory_embedding_model

    chat_available = _model_available(chat_model, names)
    embedding_available = _model_available(embedding_model, names)

    checks.append(
        PreflightCheck(
            name="ollama_chat_model",
            status="pass" if chat_available else "warn",
            detail=(
                f"Configured chat model '{chat_model}' is available."
                if chat_available
                else f"Configured chat model '{chat_model}' not found in Ollama tags."
            ),
            required=False,
        )
    )
    checks.append(
        PreflightCheck(
            name="ollama_embedding_model",
            status="pass" if embedding_available else "warn",
            detail=(
                f"Configured embedding model '{embedding_model}' is available."
                if embedding_available
                else f"Configured embedding model '{embedding_model}' not found in Ollama tags."
            ),
            required=False,
        )
    )

    return checks


def _check_streamlit_import() -> PreflightCheck:
    available = find_spec("streamlit") is not None
    return PreflightCheck(
        name="streamlit_import",
        status="pass" if available else "warn",
        detail=(
            "Streamlit import is available."
            if available
            else "Streamlit is not importable in this environment."
        ),
        required=False,
    )


def _check_demo_seed_data(settings: Settings) -> PreflightCheck:
    try:
        with transaction(settings) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM ledger;")
                row = cur.fetchone()
    except Exception as exc:
        return PreflightCheck(
            name="demo_seed_data",
            status="warn",
            detail=f"Unable to inspect demo seed state: {exc}",
            required=False,
        )

    total_rows = int(row[0]) if row is not None else 0
    if total_rows < 1:
        return PreflightCheck(
            name="demo_seed_data",
            status="warn",
            detail="No ledger rows detected. Run demo bootstrap to seed local data.",
            required=False,
        )

    return PreflightCheck(
        name="demo_seed_data",
        status="pass",
        detail="Ledger contains seeded transaction data.",
        required=False,
        meta={"ledger_rows": total_rows},
    )


def run_preflight() -> tuple[PreflightSummary, int]:
    logger = setup_logging()
    checks: list[PreflightCheck] = []

    settings, config_check = _check_config()
    checks.append(config_check)

    if settings is None:
        summary = _build_summary(checks)
        _log_summary(logger, summary)
        return summary, 1

    logger = setup_logging(settings.log_level)

    checks.append(_check_db_connectivity(settings))
    checks.append(_check_pgvector(settings))
    checks.append(_check_schema_and_migrations(settings))
    checks.extend(_check_ollama(settings))
    checks.append(_check_streamlit_import())
    checks.append(_check_demo_seed_data(settings))

    summary = _build_summary(checks)
    _log_summary(logger, summary)

    exit_code = 1 if summary.hard_failures > 0 else 0
    return summary, exit_code


def _build_summary(checks: list[PreflightCheck]) -> PreflightSummary:
    hard_failures = len([item for item in checks if item.required and item.status == "fail"])
    warnings = len([item for item in checks if item.status == "warn"])
    return PreflightSummary(
        generated_at=datetime.now(UTC),
        checks=checks,
        hard_failures=hard_failures,
        warnings=warnings,
    )


def _log_summary(logger: Any, summary: PreflightSummary) -> None:
    for item in summary.checks:
        payload = {
            "command": "preflight",
            "check": item.name,
            "required": item.required,
            "detail": item.detail,
            **item.meta,
        }
        if item.status == "pass":
            logger.info("Preflight check passed", extra=payload)
        elif item.status == "warn":
            logger.warning("Preflight check warning", extra=payload)
        else:
            logger.error("Preflight check failed", extra=payload)

    logger.info(
        "Preflight summary",
        extra={
            "command": "preflight",
            "hard_failures": summary.hard_failures,
            "warnings": summary.warnings,
            "check_count": len(summary.checks),
        },
    )


def _build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description="SentinelBudget production-style preflight")


def main() -> None:
    parser = _build_parser()
    parser.parse_args()

    summary, exit_code = run_preflight()
    print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))

    if exit_code != 0:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
