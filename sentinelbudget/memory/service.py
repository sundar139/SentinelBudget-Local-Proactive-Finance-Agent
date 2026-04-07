from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from uuid import UUID

from psycopg import Connection

from sentinelbudget.config import Settings, get_settings
from sentinelbudget.db.repositories.goals import GoalRepository
from sentinelbudget.db.repositories.preferences import UserPreferenceRepository
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.logging import setup_logging
from sentinelbudget.memory.embeddings import EmbeddingProvider, OllamaEmbeddingProvider
from sentinelbudget.memory.models import (
    MemoryKind,
    MemorySyncSummary,
    SemanticMemoryItem,
    SemanticMemoryMatch,
)
from sentinelbudget.memory.repository import SemanticMemoryRepository

_ALLOWED_KINDS: tuple[MemoryKind, ...] = ("goal", "preference", "note")


class SemanticMemoryService:
    """Service for pgvector-backed semantic memory workflows."""

    def __init__(self, embedding_provider: EmbeddingProvider, embedding_dimension: int) -> None:
        if embedding_dimension < 1:
            raise ValueError("embedding_dimension must be positive")

        self._embedding_provider = embedding_provider
        self._embedding_dimension = embedding_dimension

    def _validate_ready(self, conn: Connection) -> None:
        if self._embedding_provider.dimension != self._embedding_dimension:
            raise ValueError(
                "Embedding provider dimension mismatch: "
                "provider="
                f"{self._embedding_provider.dimension}, "
                f"configured={self._embedding_dimension}"
            )

        SemanticMemoryRepository.ensure_embedding_dimension(conn, self._embedding_dimension)

    def store_memory(
        self,
        conn: Connection,
        user_id: UUID,
        kind: MemoryKind,
        text: str,
        metadata: dict[str, Any] | None = None,
    ) -> SemanticMemoryItem:
        _validate_kind(kind)
        _validate_non_empty_text(text=text, field_name="text")
        self._validate_ready(conn)

        payload = metadata or {}
        embedding = self._embedding_provider.embed_text(text)

        return SemanticMemoryRepository.create(
            conn,
            user_id=user_id,
            kind=kind,
            text=text,
            embedding=embedding,
            metadata=payload,
        )

    def list_memory(
        self,
        conn: Connection,
        user_id: UUID,
        kind: MemoryKind | None = None,
        limit: int = 20,
    ) -> list[SemanticMemoryItem]:
        if kind is not None:
            _validate_kind(kind)
        if limit < 1:
            raise ValueError("limit must be positive")

        self._validate_ready(conn)
        return SemanticMemoryRepository.list_recent(conn, user_id=user_id, kind=kind, limit=limit)

    def query_memory(
        self,
        conn: Connection,
        user_id: UUID,
        query_text: str,
        kind: MemoryKind | None = None,
        top_k: int = 5,
    ) -> list[SemanticMemoryMatch]:
        if kind is not None:
            _validate_kind(kind)
        if top_k < 1:
            raise ValueError("top_k must be positive")
        _validate_non_empty_text(text=query_text, field_name="query_text")

        self._validate_ready(conn)
        query_embedding = self._embedding_provider.embed_text(query_text)

        return SemanticMemoryRepository.search(
            conn,
            user_id=user_id,
            query_embedding=query_embedding,
            kind=kind,
            top_k=top_k,
        )

    def sync_goals(self, conn: Connection, user_id: UUID) -> MemorySyncSummary:
        self._validate_ready(conn)
        goals = GoalRepository.list_by_user(conn, user_id, limit=500)

        inserted = 0
        updated = 0
        skipped = 0

        for goal in goals:
            text = _goal_to_text(goal)
            metadata = {
                "source": "goals_table",
                "source_id": str(goal.goal_id),
                "title": goal.title,
                "status": goal.status,
                "target_amount": (
                    f"{goal.target_amount:.2f}" if goal.target_amount is not None else None
                ),
                "target_date": (
                    goal.target_date.isoformat() if goal.target_date is not None else None
                ),
            }

            upsert_outcome = self._upsert_by_source(
                conn,
                user_id=user_id,
                kind="goal",
                source="goals_table",
                source_id=str(goal.goal_id),
                text=text,
                metadata=metadata,
            )

            if upsert_outcome == "inserted":
                inserted += 1
            elif upsert_outcome == "updated":
                updated += 1
            else:
                skipped += 1

        return MemorySyncSummary(
            kind="goal",
            processed=len(goals),
            inserted=inserted,
            updated=updated,
            skipped=skipped,
        )

    def sync_preferences(
        self,
        conn: Connection,
        user_id: UUID,
        preference_keys: set[str] | None = None,
    ) -> MemorySyncSummary:
        self._validate_ready(conn)
        preferences = UserPreferenceRepository.list_by_user(conn, user_id)

        if preference_keys:
            preferences = [item for item in preferences if item.preference_key in preference_keys]

        inserted = 0
        updated = 0
        skipped = 0

        for preference in preferences:
            text = _preference_to_text(preference.preference_key, preference.preference_value)
            metadata = {
                "source": "user_preferences",
                "source_id": str(preference.preference_id),
                "preference_key": preference.preference_key,
            }

            outcome = self._upsert_by_source(
                conn,
                user_id=user_id,
                kind="preference",
                source="user_preferences",
                source_id=str(preference.preference_id),
                text=text,
                metadata=metadata,
            )

            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated":
                updated += 1
            else:
                skipped += 1

        return MemorySyncSummary(
            kind="preference",
            processed=len(preferences),
            inserted=inserted,
            updated=updated,
            skipped=skipped,
        )

    def _upsert_by_source(
        self,
        conn: Connection,
        user_id: UUID,
        kind: MemoryKind,
        source: str,
        source_id: str,
        text: str,
        metadata: dict[str, Any],
    ) -> str:
        existing = SemanticMemoryRepository.get_by_source(
            conn,
            user_id=user_id,
            kind=kind,
            source=source,
            source_id=source_id,
        )

        if existing is None:
            embedding = self._embedding_provider.embed_text(text)
            SemanticMemoryRepository.create(
                conn,
                user_id=user_id,
                kind=kind,
                text=text,
                embedding=embedding,
                metadata=metadata,
            )
            return "inserted"

        if (
            existing.text == text
            and _stable_metadata(existing.metadata) == _stable_metadata(metadata)
        ):
            return "skipped"

        embedding = self._embedding_provider.embed_text(text)
        SemanticMemoryRepository.update(
            conn,
            memory_id=existing.id,
            text=text,
            embedding=embedding,
            metadata=metadata,
        )
        return "updated"


def _validate_kind(kind: str) -> None:
    if kind not in _ALLOWED_KINDS:
        raise ValueError(f"kind must be one of: {', '.join(_ALLOWED_KINDS)}")


def _validate_non_empty_text(text: str, field_name: str) -> None:
    if text.strip() == "":
        raise ValueError(f"{field_name} cannot be empty")


def _goal_to_text(goal: Any) -> str:
    target_amount = f"{goal.target_amount:.2f}" if goal.target_amount is not None else "unspecified"
    target_date = goal.target_date.isoformat() if goal.target_date is not None else "unspecified"
    description = goal.description.strip() if goal.description else "No description"

    return (
        f"Goal '{goal.title}'. Status: {goal.status}. Target amount: {target_amount}. "
        f"Target date: {target_date}. Description: {description}."
    )


def _preference_to_text(preference_key: str, preference_value: dict[str, Any]) -> str:
    return (
        f"Preference '{preference_key}': "
        + json.dumps(preference_value, sort_keys=True, separators=(",", ":"))
    )


def _stable_metadata(metadata: dict[str, Any]) -> str:
    return json.dumps(metadata, sort_keys=True, separators=(",", ":"), default=str)


def _build_embedding_provider(settings: Settings) -> SemanticMemoryService:
    provider = OllamaEmbeddingProvider(
        base_url=str(settings.ollama_base_url),
        model=settings.memory_embedding_model,
        dimension=settings.memory_embedding_dim,
        timeout_seconds=settings.memory_embedding_timeout_seconds,
    )
    return SemanticMemoryService(provider, embedding_dimension=settings.memory_embedding_dim)


def _build_parser(default_top_k: int) -> argparse.ArgumentParser:
    if default_top_k < 1:
        raise ValueError("default_top_k must be positive")

    parser = argparse.ArgumentParser(description="SentinelBudget semantic memory commands")
    subparsers = parser.add_subparsers(dest="command", required=True)

    store_parser = subparsers.add_parser("store-memory", help="Store one semantic memory item")
    store_parser.add_argument("--user-id", required=True, type=UUID)
    store_parser.add_argument("--kind", required=True, choices=list(_ALLOWED_KINDS))
    store_parser.add_argument("--text", required=True)
    store_parser.add_argument("--metadata-json", default="{}")

    goals_parser = subparsers.add_parser("sync-goals", help="Sync goals table into semantic memory")
    goals_parser.add_argument("--user-id", required=True, type=UUID)

    prefs_parser = subparsers.add_parser(
        "sync-preferences",
        help="Sync user_preferences into semantic memory",
    )
    prefs_parser.add_argument("--user-id", required=True, type=UUID)
    prefs_parser.add_argument("--preference-key", action="append", default=[])

    query_parser = subparsers.add_parser("query-memory", help="Query semantic memory by similarity")
    query_parser.add_argument("--user-id", required=True, type=UUID)
    query_parser.add_argument("--query", required=True)
    query_parser.add_argument("--kind", choices=list(_ALLOWED_KINDS), default=None)
    query_parser.add_argument("--top-k", type=int, default=default_top_k)

    list_parser = subparsers.add_parser("list-memory", help="List recent semantic memory items")
    list_parser.add_argument("--user-id", required=True, type=UUID)
    list_parser.add_argument("--kind", choices=list(_ALLOWED_KINDS), default=None)
    list_parser.add_argument("--limit", type=int, default=20)

    return parser


def main() -> None:
    if any(flag in {"-h", "--help"} for flag in sys.argv[1:]):
        parser = _build_parser(default_top_k=5)
        parser.parse_args()
        return

    settings = get_settings()
    logger = setup_logging(settings.log_level)
    parser = _build_parser(default_top_k=settings.memory_default_top_k)
    args = parser.parse_args()

    service = _build_embedding_provider(settings)

    user_id_obj = getattr(args, "user_id", None)
    user_id_text = str(user_id_obj) if user_id_obj is not None else "n/a"
    logger.info(
        "Memory command started",
        extra={
            "command": "memory",
            "subcommand": args.command,
            "user_id": user_id_text,
        },
    )

    try:
        with transaction(settings) as conn:
            if args.command == "store-memory":
                metadata_obj = json.loads(args.metadata_json)
                if not isinstance(metadata_obj, dict):
                    raise ValueError("--metadata-json must decode to an object")

                item = service.store_memory(
                    conn,
                    user_id=args.user_id,
                    kind=args.kind,
                    text=args.text,
                    metadata=metadata_obj,
                )
                print(json.dumps({"stored": _item_to_dict(item)}, indent=2, sort_keys=True))

            elif args.command == "sync-goals":
                summary = service.sync_goals(conn, user_id=args.user_id)
                print(
                    json.dumps(
                        {
                            "kind": summary.kind,
                            "processed": summary.processed,
                            "inserted": summary.inserted,
                            "updated": summary.updated,
                            "skipped": summary.skipped,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                )

            elif args.command == "sync-preferences":
                keys = set(args.preference_key) if args.preference_key else None
                summary = service.sync_preferences(conn, user_id=args.user_id, preference_keys=keys)
                print(
                    json.dumps(
                        {
                            "kind": summary.kind,
                            "processed": summary.processed,
                            "inserted": summary.inserted,
                            "updated": summary.updated,
                            "skipped": summary.skipped,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                )

            elif args.command == "query-memory":
                matches = service.query_memory(
                    conn,
                    user_id=args.user_id,
                    query_text=args.query,
                    kind=args.kind,
                    top_k=args.top_k,
                )
                print(
                    json.dumps(
                        {"matches": [_match_to_dict(item) for item in matches]},
                        indent=2,
                        sort_keys=True,
                    )
                )

            else:
                items = service.list_memory(
                    conn,
                    user_id=args.user_id,
                    kind=args.kind,
                    limit=args.limit,
                )
                print(
                    json.dumps(
                        {"items": [_item_to_dict(item) for item in items]},
                        indent=2,
                        sort_keys=True,
                    )
                )
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Memory command failed",
            extra={
                "command": "memory",
                "subcommand": args.command,
                "user_id": user_id_text,
                "detail": str(exc),
            },
        )
        raise SystemExit(1) from exc

    logger.info(
        "Memory command completed",
        extra={
            "command": "memory",
            "subcommand": args.command,
            "user_id": user_id_text,
        },
    )


def _item_to_dict(item: SemanticMemoryItem) -> dict[str, Any]:
    return {
        "id": item.id,
        "user_id": str(item.user_id),
        "kind": item.kind,
        "text": item.text,
        "metadata": item.metadata,
        "created_at": item.created_at.isoformat(),
    }


def _match_to_dict(item: SemanticMemoryMatch) -> dict[str, Any]:
    return {
        "id": item.id,
        "user_id": str(item.user_id),
        "kind": item.kind,
        "text": item.text,
        "metadata": item.metadata,
        "created_at": item.created_at.isoformat(),
        "score": f"{item.score:.4f}",
    }


if __name__ == "__main__":
    main()
