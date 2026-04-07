from __future__ import annotations

import argparse
import json
from typing import Any
from uuid import UUID

from sentinelbudget.agent.history import PostgresConversationHistoryStore
from sentinelbudget.agent.models import ConversationEntry, GroundedAnswer
from sentinelbudget.agent.orchestrator import ConversationOrchestrator
from sentinelbudget.agent.provider import ChatModelProvider, OllamaChatModelProvider
from sentinelbudget.agent.tools import AgentToolRegistry
from sentinelbudget.config import Settings, get_settings
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.logging import setup_logging
from sentinelbudget.memory.embeddings import OllamaEmbeddingProvider
from sentinelbudget.memory.service import SemanticMemoryService


class SentinelBudgetChatService:
    """High-level service for local grounded chat orchestration."""

    def __init__(
        self,
        settings: Settings,
        provider: ChatModelProvider,
        memory_service: SemanticMemoryService,
    ) -> None:
        self._settings = settings
        self._provider = provider
        self._history_store = PostgresConversationHistoryStore()
        self._tool_registry = AgentToolRegistry(memory_service=memory_service)
        self._orchestrator = ConversationOrchestrator(
            provider=self._provider,
            history_store=self._history_store,
            tool_registry=self._tool_registry,
            max_tool_hops=self._settings.agent_max_tool_hops,
            history_limit=self._settings.agent_history_limit,
        )

    def ask(self, user_id: UUID, session_id: UUID, message: str) -> GroundedAnswer:
        with transaction(self._settings) as conn:
            return self._orchestrator.run_turn(
                conn,
                user_id=user_id,
                session_id=session_id,
                user_message=message,
            )

    def inspect_session(
        self,
        user_id: UUID,
        session_id: UUID,
        limit: int,
    ) -> list[ConversationEntry]:
        with transaction(self._settings) as conn:
            return self._history_store.list_recent(
                conn,
                session_id=session_id,
                user_id=user_id,
                limit=limit,
            )


def build_chat_service(
    settings: Settings | None = None,
    provider: ChatModelProvider | None = None,
    memory_service: SemanticMemoryService | None = None,
) -> SentinelBudgetChatService:
    cfg = settings or get_settings()

    resolved_provider = provider or OllamaChatModelProvider(
        base_url=str(cfg.ollama_base_url),
        model=cfg.ollama_chat_model,
        timeout_seconds=cfg.ollama_chat_timeout_seconds,
        temperature=cfg.ollama_chat_temperature,
    )

    resolved_memory_service = memory_service or _build_memory_service(cfg)

    return SentinelBudgetChatService(
        settings=cfg,
        provider=resolved_provider,
        memory_service=resolved_memory_service,
    )


def _build_memory_service(settings: Settings) -> SemanticMemoryService:
    provider = OllamaEmbeddingProvider(
        base_url=str(settings.ollama_base_url),
        model=settings.memory_embedding_model,
        dimension=settings.memory_embedding_dim,
        timeout_seconds=settings.memory_embedding_timeout_seconds,
    )
    return SemanticMemoryService(provider, embedding_dimension=settings.memory_embedding_dim)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SentinelBudget local grounded chat")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ask_parser = subparsers.add_parser("ask", help="One-shot grounded question answering")
    ask_parser.add_argument("--user-id", required=True, type=UUID)
    ask_parser.add_argument("--session-id", required=True, type=UUID)
    ask_parser.add_argument("--message", required=True)

    chat_parser = subparsers.add_parser("chat", help="Interactive terminal chat")
    chat_parser.add_argument("--user-id", required=True, type=UUID)
    chat_parser.add_argument("--session-id", required=True, type=UUID)

    inspect_parser = subparsers.add_parser(
        "inspect-session",
        help="Inspect persisted conversation history",
    )
    inspect_parser.add_argument("--user-id", required=True, type=UUID)
    inspect_parser.add_argument("--session-id", required=True, type=UUID)
    inspect_parser.add_argument("--limit", type=int, default=50)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    settings = get_settings()
    logger = setup_logging(settings.log_level)

    service = build_chat_service()

    user_id_text = str(getattr(args, "user_id", "n/a"))
    session_id_text = str(getattr(args, "session_id", "n/a"))
    logger.info(
        "Chat command started",
        extra={
            "command": "chat",
            "subcommand": args.command,
            "user_id": user_id_text,
            "session_id": session_id_text,
        },
    )

    try:
        if args.command == "ask":
            answer = service.ask(
                user_id=args.user_id,
                session_id=args.session_id,
                message=args.message,
            )
            print(json.dumps(answer.to_dict(), indent=2, sort_keys=True))
            logger.info(
                "Chat ask completed",
                extra={
                    "command": "chat",
                    "subcommand": "ask",
                    "user_id": user_id_text,
                    "session_id": session_id_text,
                    "tools_used": len(answer.tools_used),
                    "warnings": len(answer.warnings),
                },
            )
            return

        if args.command == "inspect-session":
            records = service.inspect_session(
                user_id=args.user_id,
                session_id=args.session_id,
                limit=args.limit,
            )
            payload = {"messages": [_conversation_to_dict(item) for item in records]}
            print(json.dumps(payload, indent=2, sort_keys=True))
            logger.info(
                "Chat inspect-session completed",
                extra={
                    "command": "chat",
                    "subcommand": "inspect-session",
                    "user_id": user_id_text,
                    "session_id": session_id_text,
                    "message_count": len(records),
                },
            )
            return

        print("Interactive chat mode. Type 'exit' or 'quit' to stop.")
        while True:
            try:
                line = input("you> ").strip()
            except EOFError:
                break

            if line == "":
                continue
            if line.lower() in {"exit", "quit"}:
                break

            answer = service.ask(
                user_id=args.user_id,
                session_id=args.session_id,
                message=line,
            )
            print(f"assistant> {answer.answer_text}")
            if answer.warnings:
                print("warnings>")
                for item in answer.warnings:
                    print(f"- {item}")

        logger.info(
            "Interactive chat session completed",
            extra={
                "command": "chat",
                "subcommand": "chat",
                "user_id": user_id_text,
                "session_id": session_id_text,
            },
        )
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Chat command failed",
            extra={
                "command": "chat",
                "subcommand": args.command,
                "user_id": user_id_text,
                "session_id": session_id_text,
                "detail": str(exc),
            },
        )
        raise SystemExit(1) from exc


def _conversation_to_dict(entry: ConversationEntry) -> dict[str, Any]:
    return {
        "id": entry.id,
        "session_id": str(entry.session_id),
        "user_id": str(entry.user_id),
        "role": entry.role,
        "content": entry.content,
        "metadata": entry.metadata,
        "created_at": entry.created_at.isoformat(),
    }


if __name__ == "__main__":
    main()
