from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from psycopg import Connection

from sentinelbudget.agent.history import ConversationHistoryStore
from sentinelbudget.agent.models import (
    ChatMessage,
    EvidenceBlock,
    GroundedAnswer,
    ToolExecutionRecord,
)
from sentinelbudget.agent.prompts import build_system_prompt
from sentinelbudget.agent.provider import (
    ChatModelProvider,
    ChatProviderError,
    ChatProviderUnavailableError,
)
from sentinelbudget.agent.tools import (
    AgentToolRegistry,
    ToolExecutionError,
    ToolValidationError,
    UnsupportedToolError,
)


@dataclass(slots=True)
class ConversationOrchestrator:
    provider: ChatModelProvider
    history_store: ConversationHistoryStore
    tool_registry: AgentToolRegistry
    max_tool_hops: int = 4
    history_limit: int = 40

    def __post_init__(self) -> None:
        if self.max_tool_hops < 1:
            raise ValueError("max_tool_hops must be positive")
        if self.history_limit < 1:
            raise ValueError("history_limit must be positive")

    def run_turn(
        self,
        conn: Connection,
        user_id: UUID,
        session_id: UUID,
        user_message: str,
    ) -> GroundedAnswer:
        if user_message.strip() == "":
            raise ValueError("user_message cannot be empty")

        self.history_store.append_message(
            conn,
            session_id=session_id,
            user_id=user_id,
            role="user",
            content=user_message,
            metadata={"source": "phase6_cli"},
        )

        conversation = self.history_store.list_recent(
            conn,
            session_id=session_id,
            user_id=user_id,
            limit=self.history_limit,
        )
        tool_definitions = self.tool_registry.list_tool_definitions()

        prompt_messages: list[ChatMessage] = [
            ChatMessage(role="system", content=build_system_prompt(tool_definitions)),
            *[
                ChatMessage(
                    role=item.role,
                    content=item.content,
                    name=(
                        item.metadata.get("tool_name")
                        if isinstance(item.metadata.get("tool_name"), str)
                        else None
                    ),
                )
                for item in conversation
            ],
        ]

        warnings: list[str] = []
        tool_records: list[ToolExecutionRecord] = []

        for hop in range(self.max_tool_hops):
            try:
                model_result = self.provider.chat(prompt_messages, tool_definitions)
            except ChatProviderUnavailableError as exc:
                return self._safe_fail_answer(
                    conn,
                    user_id=user_id,
                    session_id=session_id,
                    warning=f"Local model unavailable: {exc}",
                    tool_records=tool_records,
                )
            except ChatProviderError as exc:
                return self._safe_fail_answer(
                    conn,
                    user_id=user_id,
                    session_id=session_id,
                    warning=f"Model response error: {exc}",
                    tool_records=tool_records,
                )

            if model_result.tool_call is None:
                return self._build_final_answer(
                    conn,
                    user_id=user_id,
                    session_id=session_id,
                    model_text=model_result.content,
                    tool_records=tool_records,
                    warnings=warnings,
                    original_user_message=user_message,
                )

            tool_call = model_result.tool_call
            assistant_tool_call = {
                "type": "tool_call",
                "name": tool_call.name,
                "arguments": tool_call.arguments,
                "call_id": tool_call.call_id,
                "hop": hop,
            }
            assistant_tool_call_text = json.dumps(assistant_tool_call, sort_keys=True)

            self.history_store.append_message(
                conn,
                session_id=session_id,
                user_id=user_id,
                role="assistant",
                content=assistant_tool_call_text,
                metadata={
                    "kind": "tool_call",
                    "tool_name": tool_call.name,
                    "hop": hop,
                },
            )
            prompt_messages.append(
                ChatMessage(role="assistant", content=assistant_tool_call_text)
            )

            try:
                record = self.tool_registry.execute_tool(
                    conn,
                    user_id=user_id,
                    tool_name=tool_call.name,
                    arguments=tool_call.arguments,
                )
                tool_records.append(record)

                tool_content_payload = {
                    "ok": True,
                    "tool_name": record.tool_name,
                    "output": record.output,
                }
            except (UnsupportedToolError, ToolValidationError, ToolExecutionError) as exc:
                warnings.append(str(exc))
                tool_content_payload = {
                    "ok": False,
                    "tool_name": tool_call.name,
                    "error": str(exc),
                }

            tool_content_text = json.dumps(tool_content_payload, sort_keys=True)
            self.history_store.append_message(
                conn,
                session_id=session_id,
                user_id=user_id,
                role="tool",
                content=tool_content_text,
                metadata={
                    "tool_name": tool_call.name,
                    "hop": hop,
                    "ok": bool(tool_content_payload["ok"]),
                },
            )
            prompt_messages.append(
                ChatMessage(
                    role="tool",
                    name=tool_call.name,
                    tool_call_id=tool_call.call_id,
                    content=tool_content_text,
                )
            )

        warnings.append("Maximum tool-call hops reached before final answer")
        return self._safe_fail_answer(
            conn,
            user_id=user_id,
            session_id=session_id,
            warning=warnings[-1],
            tool_records=tool_records,
        )

    def _build_final_answer(
        self,
        conn: Connection,
        user_id: UUID,
        session_id: UUID,
        model_text: str | None,
        tool_records: list[ToolExecutionRecord],
        warnings: list[str],
        original_user_message: str,
    ) -> GroundedAnswer:
        parsed = _parse_model_answer_payload(model_text)
        warnings = [*warnings, *parsed["warnings"]]

        answer_text = parsed["answer_text"]
        if parsed["is_structured"] is False or parsed["has_answer_text"] is False:
            answer_text = _build_unstructured_fallback_text(tool_records)

        if not tool_records and _likely_data_question(original_user_message):
            answer_text = (
                "I could not produce a grounded finance answer because no verified tool output "
                "was available for this question."
            )
            warnings.append("No grounded tools were used for a data-specific question")

        citations = _resolve_citations(parsed["citations"], tool_records)
        tools_used = _ordered_unique([item.tool_name for item in tool_records])

        grounded_answer = GroundedAnswer(
            answer_text=answer_text,
            citations=citations,
            tools_used=tools_used,
            warnings=warnings,
            structured_payload=parsed["structured_payload"],
            session_id=session_id,
            user_id=user_id,
            created_at=datetime.now(UTC),
            meta={"tool_hops": len(tool_records)},
        )

        self.history_store.append_message(
            conn,
            session_id=session_id,
            user_id=user_id,
            role="assistant",
            content=grounded_answer.answer_text,
            metadata={
                "warnings": grounded_answer.warnings,
                "tools_used": grounded_answer.tools_used,
                "citations": [
                    {"tool_name": item.tool_name, "evidence": item.evidence}
                    for item in grounded_answer.citations
                ],
            },
        )

        return grounded_answer

    def _safe_fail_answer(
        self,
        conn: Connection,
        user_id: UUID,
        session_id: UUID,
        warning: str,
        tool_records: list[ToolExecutionRecord],
    ) -> GroundedAnswer:
        citations = _resolve_citations([], tool_records)
        answer = GroundedAnswer(
            answer_text=(
                "I could not complete the request safely because the local model or tooling "
                "was unavailable."
            ),
            citations=citations,
            tools_used=_ordered_unique([item.tool_name for item in tool_records]),
            warnings=[warning],
            structured_payload=None,
            session_id=session_id,
            user_id=user_id,
            created_at=datetime.now(UTC),
            meta={"tool_hops": len(tool_records)},
        )

        self.history_store.append_message(
            conn,
            session_id=session_id,
            user_id=user_id,
            role="assistant",
            content=answer.answer_text,
            metadata={"warnings": answer.warnings, "tools_used": answer.tools_used},
        )

        return answer


def _parse_model_answer_payload(content: str | None) -> dict[str, Any]:
    if content is None or content.strip() == "":
        return {
            "answer_text": "I could not generate a grounded response.",
            "citations": [],
            "warnings": ["Model returned empty response content"],
            "structured_payload": None,
            "is_structured": False,
        }

    parsed_warnings: list[str] = []

    try:
        payload_obj = json.loads(content)
    except json.JSONDecodeError:
        return {
            "answer_text": "I could not parse a structured model response.",
            "citations": [],
            "warnings": ["Model response was not JSON; using deterministic grounded fallback"],
            "structured_payload": None,
            "is_structured": False,
        }

    if not isinstance(payload_obj, dict):
        return {
            "answer_text": "I could not parse a structured model response.",
            "citations": [],
            "warnings": [
                "Model JSON response was not an object; using deterministic grounded fallback"
            ],
            "structured_payload": None,
            "is_structured": False,
        }

    answer_obj = payload_obj.get("answer_text")
    answer_text = answer_obj.strip() if isinstance(answer_obj, str) else ""
    has_answer_text = answer_text != ""
    if answer_text == "":
        answer_text = "I could not generate a grounded response."
        parsed_warnings.append("answer_text missing in model JSON response")

    citations_obj = payload_obj.get("citations", [])
    parsed_citations: list[EvidenceBlock] = []
    if isinstance(citations_obj, list):
        for entry in citations_obj:
            if not isinstance(entry, dict):
                continue
            tool_name = entry.get("tool_name")
            evidence = entry.get("evidence")
            if isinstance(tool_name, str) and tool_name.strip() != "" and isinstance(evidence, str):
                payload = entry.get("payload")
                parsed_citations.append(
                    EvidenceBlock(
                        tool_name=tool_name,
                        evidence=evidence,
                        payload=payload if isinstance(payload, dict) else None,
                    )
                )

    warnings_obj = payload_obj.get("warnings", [])
    if isinstance(warnings_obj, list):
        for item in warnings_obj:
            if isinstance(item, str) and item.strip() != "":
                parsed_warnings.append(item)

    structured_payload_obj = payload_obj.get("structured_payload")
    structured_payload = (
        structured_payload_obj if isinstance(structured_payload_obj, dict) else None
    )

    return {
        "answer_text": answer_text,
        "citations": parsed_citations,
        "warnings": parsed_warnings,
        "structured_payload": structured_payload,
        "is_structured": True,
        "has_answer_text": has_answer_text,
    }


def _build_unstructured_fallback_text(tool_records: list[ToolExecutionRecord]) -> str:
    if not tool_records:
        return "I could not produce a grounded response from the available model output."

    tools = ", ".join(_ordered_unique([item.tool_name for item in tool_records]))
    return (
        "I received an unstructured model response, so I am returning only grounded tool "
        f"evidence from: {tools}."
    )


def _resolve_citations(
    model_citations: list[EvidenceBlock],
    tool_records: list[ToolExecutionRecord],
) -> list[EvidenceBlock]:
    if model_citations:
        used_tool_names = {item.tool_name for item in tool_records}
        filtered = [item for item in model_citations if item.tool_name in used_tool_names]
        if filtered:
            return filtered

    return [
        EvidenceBlock(
            tool_name=item.tool_name,
            evidence=_summarize_tool_output(item),
            payload=item.output,
        )
        for item in tool_records
    ]


def _summarize_tool_output(record: ToolExecutionRecord) -> str:
    if record.tool_name == "get_kpi_summary":
        kpi_obj = record.output.get("kpis")
        if isinstance(kpi_obj, dict):
            net = kpi_obj.get("net_cashflow")
            income = kpi_obj.get("total_income")
            expenses = kpi_obj.get("total_expenses")
            return (
                "KPI summary used with total_income="
                f"{income}, total_expenses={expenses}, net_cashflow={net}."
            )

    count_obj = record.output.get("count")
    if isinstance(count_obj, int):
        return f"Tool returned count={count_obj}."

    keys = sorted(record.output.keys())
    preview = ", ".join(keys[:4])
    return f"Tool output fields: {preview}."


def _ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _likely_data_question(message: str) -> bool:
    normalized = message.lower()
    keywords = (
        "spend",
        "spent",
        "overspend",
        "balance",
        "goal",
        "anomaly",
        "recurring",
        "income",
        "expense",
        "budget",
        "cashflow",
    )
    return any(token in normalized for token in keywords)
