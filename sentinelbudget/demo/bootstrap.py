from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from psycopg import Connection

from sentinelbudget.config import Settings, get_settings
from sentinelbudget.db.init_db import run_migrations
from sentinelbudget.db.repositories.accounts import Account, AccountRepository
from sentinelbudget.db.repositories.goals import GoalRepository
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.db.repositories.users import User, UserRepository
from sentinelbudget.db.schema import bootstrap_default_categories
from sentinelbudget.ingest.models import IngestSummary
from sentinelbudget.ingest.service import ingest_synthetic_transactions
from sentinelbudget.logging import setup_logging
from sentinelbudget.memory.models import MemorySyncSummary
from sentinelbudget.memory.service import SemanticMemoryService
from sentinelbudget.review.service import ProactiveReviewService, build_review_service

ReviewModeOption = Literal["daily", "weekly", "none"]

_DEMO_GOAL_SEEDS: tuple[dict[str, Any], ...] = (
    {
        "title": "Build Emergency Fund",
        "description": "Set aside cash to cover 3 months of essential expenses.",
        "target_amount": Decimal("5000.00"),
        "target_date": date(2026, 12, 31),
        "status": "active",
    },
    {
        "title": "Reduce Dining-Out Spending",
        "description": "Bring dining-out costs down with a monthly spending cap.",
        "target_amount": Decimal("300.00"),
        "target_date": date(2026, 10, 31),
        "status": "active",
    },
    {
        "title": "Save for Travel",
        "description": "Build a dedicated travel fund for a future trip.",
        "target_amount": Decimal("1800.00"),
        "target_date": date(2027, 3, 31),
        "status": "active",
    },
)


@dataclass(frozen=True, slots=True)
class DemoBootstrapResult:
    user_id: UUID
    account_id: UUID
    user_created: bool
    account_created: bool
    categories_inserted: int
    ingest_summary: dict[str, Any]
    synced_goals: dict[str, Any] | None
    review_created_count: int | None
    review_skipped_count: int | None
    review_mode: str | None
    warnings: list[str] = field(default_factory=list)

    @property
    def next_commands(self) -> list[str]:
        return [
            "uv run sentinelbudget-preflight",
            "uv run streamlit run ui/app.py",
            (
                "uv run sentinelbudget-chat ask "
                f"--user-id {self.user_id} --session-id <SESSION_UUID> "
                '--message "Am I overspending this month?"'
            ),
            (
                "uv run sentinelbudget-review list-unread-insights "
                f"--user-id {self.user_id} --limit 20"
            ),
        ]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["user_id"] = str(self.user_id)
        payload["account_id"] = str(self.account_id)
        payload["next_commands"] = self.next_commands
        return payload


def _build_memory_service(settings: Settings) -> SemanticMemoryService:
    from sentinelbudget.memory.embeddings import OllamaEmbeddingProvider

    provider = OllamaEmbeddingProvider(
        base_url=str(settings.ollama_base_url),
        model=settings.memory_embedding_model,
        dimension=settings.memory_embedding_dim,
        timeout_seconds=settings.memory_embedding_timeout_seconds,
    )
    return SemanticMemoryService(provider, embedding_dimension=settings.memory_embedding_dim)


def _ensure_user(conn: Connection, user_id: UUID, user_email: str) -> tuple[User, bool, list[str]]:
    warnings: list[str] = []
    existing = UserRepository.get_by_id(conn, user_id)
    if existing is None:
        created = UserRepository.create(conn, email=user_email, user_id=user_id)
        return created, True, warnings

    if existing.email != user_email:
        warnings.append(
            "Provided --user-email differs from existing user email; existing record was kept."
        )

    return existing, False, warnings


def _ensure_account(
    conn: Connection,
    account_id: UUID,
    user_id: UUID,
    institution: str,
    account_name: str,
    account_type: str,
    starting_balance: Decimal,
) -> tuple[Account, bool]:
    existing = AccountRepository.get_by_id(conn, account_id)
    if existing is None:
        created = AccountRepository.create(
            conn,
            user_id=user_id,
            institution=institution,
            name=account_name,
            account_type=account_type,
            current_balance=starting_balance,
            account_id=account_id,
        )
        return created, True

    if existing.user_id != user_id:
        raise ValueError("Provided account_id already belongs to a different user")

    return existing, False


def _sync_goals(
    conn: Connection,
    settings: Settings,
    user_id: UUID,
    sync_goals: bool,
) -> MemorySyncSummary | None:
    if not sync_goals:
        return None

    memory_service = _build_memory_service(settings)
    return memory_service.sync_goals(conn, user_id=user_id)


def _normalize_goal_title(value: str) -> str:
    return " ".join(value.split()).strip().casefold()


def _seed_demo_goals(conn: Connection, user_id: UUID) -> int:
    existing_goals = GoalRepository.list_by_user(conn, user_id=user_id, limit=500)
    existing_titles = {_normalize_goal_title(item.title) for item in existing_goals}

    inserted = 0
    for goal_seed in _DEMO_GOAL_SEEDS:
        title = str(goal_seed["title"])
        normalized_title = _normalize_goal_title(title)
        if normalized_title in existing_titles:
            continue

        GoalRepository.create(
            conn,
            user_id=user_id,
            title=title,
            description=str(goal_seed["description"]),
            target_amount=goal_seed["target_amount"],
            target_date=goal_seed["target_date"],
            status=str(goal_seed["status"]),
        )
        existing_titles.add(normalized_title)
        inserted += 1

    return inserted


def _run_review(
    conn: Connection,
    settings: Settings,
    user_id: UUID,
    review_mode: ReviewModeOption,
) -> tuple[int | None, int | None, str | None]:
    if review_mode == "none":
        return None, None, None

    review_service: ProactiveReviewService = build_review_service(settings)
    outcome = review_service.run_review(
        conn,
        user_id=user_id,
        mode=review_mode,
        persist=True,
    )
    return len(outcome.created_insight_ids), len(outcome.skipped_fingerprints), review_mode


def _require_non_empty(name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    return normalized


def _validate_email(email: str) -> str:
    normalized = email.strip()
    local_part, sep, domain = normalized.partition("@")
    if not normalized or not sep or not local_part or not domain or "." not in domain:
        raise ValueError("user_email must be a valid email address")
    return normalized


def _validate_bootstrap_inputs(
    user_email: str,
    institution: str,
    account_name: str,
    account_type: str,
    days: int,
    source_dataset: str,
    output_csv: Path | None,
) -> tuple[str, str, str, str, str]:
    if days < 1:
        raise ValueError("days must be positive")

    normalized_user_email = _validate_email(user_email)
    normalized_institution = _require_non_empty("institution", institution)
    normalized_account_name = _require_non_empty("account_name", account_name)
    normalized_account_type = _require_non_empty("account_type", account_type)
    normalized_source_dataset = _require_non_empty("source_dataset", source_dataset)

    if output_csv is not None and output_csv.exists() and output_csv.is_dir():
        raise ValueError("output_csv must point to a file path, not a directory")

    return (
        normalized_user_email,
        normalized_institution,
        normalized_account_name,
        normalized_account_type,
        normalized_source_dataset,
    )


def bootstrap_demo_data(
    conn: Connection,
    settings: Settings,
    user_id: UUID,
    account_id: UUID,
    user_email: str,
    institution: str,
    account_name: str,
    account_type: str,
    starting_balance: Decimal,
    days: int,
    seed: int,
    start_date: date,
    source_dataset: str,
    output_csv: Path | None,
    sync_goals: bool,
    review_mode: ReviewModeOption,
) -> DemoBootstrapResult:
    (
        normalized_user_email,
        normalized_institution,
        normalized_account_name,
        normalized_account_type,
        normalized_source_dataset,
    ) = _validate_bootstrap_inputs(
        user_email=user_email,
        institution=institution,
        account_name=account_name,
        account_type=account_type,
        days=days,
        source_dataset=source_dataset,
        output_csv=output_csv,
    )

    categories_inserted = bootstrap_default_categories(conn)

    user, user_created, warnings = _ensure_user(
        conn,
        user_id=user_id,
        user_email=normalized_user_email,
    )
    account, account_created = _ensure_account(
        conn,
        account_id=account_id,
        user_id=user.user_id,
        institution=normalized_institution,
        account_name=normalized_account_name,
        account_type=normalized_account_type,
        starting_balance=starting_balance,
    )

    try:
        ingest_summary: IngestSummary = ingest_synthetic_transactions(
            conn,
            account_id=account.account_id,
            days=days,
            seed=seed,
            start_date=start_date,
            source_dataset=normalized_source_dataset,
            output_csv=output_csv,
        )
    except Exception as exc:
        raise RuntimeError(
            "Synthetic ingest failed during demo bootstrap "
            f"(account_id={account.account_id}, source_dataset={normalized_source_dataset}, "
            f"days={days}, seed={seed}): {exc}"
        ) from exc

    _seed_demo_goals(conn, user.user_id)

    goal_sync_summary = _sync_goals(
        conn,
        settings=settings,
        user_id=user.user_id,
        sync_goals=sync_goals,
    )

    review_created_count, review_skipped_count, resolved_review_mode = _run_review(
        conn,
        settings=settings,
        user_id=user.user_id,
        review_mode=review_mode,
    )

    return DemoBootstrapResult(
        user_id=user.user_id,
        account_id=account.account_id,
        user_created=user_created,
        account_created=account_created,
        categories_inserted=categories_inserted,
        ingest_summary=ingest_summary.to_dict(),
        synced_goals=(
            {
                "kind": goal_sync_summary.kind,
                "processed": goal_sync_summary.processed,
                "inserted": goal_sync_summary.inserted,
                "updated": goal_sync_summary.updated,
                "skipped": goal_sync_summary.skipped,
            }
            if goal_sync_summary is not None
            else None
        ),
        review_created_count=review_created_count,
        review_skipped_count=review_skipped_count,
        review_mode=resolved_review_mode,
        warnings=warnings,
    )


def run_demo_bootstrap(
    settings: Settings,
    user_id: UUID,
    account_id: UUID,
    user_email: str,
    institution: str,
    account_name: str,
    account_type: str,
    starting_balance: Decimal,
    days: int,
    seed: int,
    start_date: date,
    source_dataset: str,
    output_csv: Path | None,
    sync_goals: bool,
    review_mode: ReviewModeOption,
) -> DemoBootstrapResult:
    run_migrations("head")

    with transaction(settings) as conn:
        return bootstrap_demo_data(
            conn=conn,
            settings=settings,
            user_id=user_id,
            account_id=account_id,
            user_email=user_email,
            institution=institution,
            account_name=account_name,
            account_type=account_type,
            starting_balance=starting_balance,
            days=days,
            seed=seed,
            start_date=start_date,
            source_dataset=source_dataset,
            output_csv=output_csv,
            sync_goals=sync_goals,
            review_mode=review_mode,
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bootstrap deterministic demo data for SentinelBudget"
    )
    parser.add_argument("--user-id", required=True, type=UUID)
    parser.add_argument("--account-id", required=True, type=UUID)
    parser.add_argument("--user-email", default=None)
    parser.add_argument("--institution", default="Sentinel Demo Bank")
    parser.add_argument("--account-name", default="Primary Checking")
    parser.add_argument("--account-type", default="checking")
    parser.add_argument("--starting-balance", type=Decimal, default=Decimal("2500.00"))
    parser.add_argument("--days", type=int, default=120)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 1, 1))
    parser.add_argument("--source-dataset", default="synthetic-demo")
    parser.add_argument("--output-csv", type=Path, default=None)
    parser.add_argument("--sync-goals", action="store_true")
    parser.add_argument(
        "--review-mode",
        choices=["none", "daily", "weekly"],
        default="none",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    settings = get_settings()
    logger = setup_logging(settings.log_level)

    user_email = args.user_email or f"demo-{args.user_id}@example.com"

    logger.info(
        "Demo bootstrap started",
        extra={
            "command": "demo-bootstrap",
            "user_id": str(args.user_id),
            "account_id": str(args.account_id),
            "seed": args.seed,
            "days": args.days,
            "sync_goals": bool(args.sync_goals),
            "review_mode": args.review_mode,
        },
    )

    try:
        result = run_demo_bootstrap(
            settings=settings,
            user_id=args.user_id,
            account_id=args.account_id,
            user_email=user_email,
            institution=args.institution,
            account_name=args.account_name,
            account_type=args.account_type,
            starting_balance=args.starting_balance,
            days=args.days,
            seed=args.seed,
            start_date=args.start_date,
            source_dataset=args.source_dataset,
            output_csv=args.output_csv,
            sync_goals=bool(args.sync_goals),
            review_mode=args.review_mode,
        )
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Demo bootstrap failed",
            extra={
                "command": "demo-bootstrap",
                "user_id": str(args.user_id),
                "account_id": str(args.account_id),
                "detail": str(exc),
            },
        )
        sys.exit(1)

    logger.info(
        "Demo bootstrap completed",
        extra={
            "command": "demo-bootstrap",
            "user_id": str(result.user_id),
            "account_id": str(result.account_id),
            "inserted_rows": result.ingest_summary.get("inserted_rows", 0),
            "duplicate_rows": result.ingest_summary.get("duplicate_rows", 0),
            "review_mode": result.review_mode,
        },
    )

    print(json.dumps(result.to_dict(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
