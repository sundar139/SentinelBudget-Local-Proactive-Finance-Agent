from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID

from psycopg import Connection

from sentinelbudget.agent.models import ChatToolDefinition, ToolExecutionRecord
from sentinelbudget.analytics.service import run_analytics
from sentinelbudget.db.repositories.goals import Goal, GoalRepository
from sentinelbudget.memory.context import assemble_context_bundle
from sentinelbudget.memory.service import SemanticMemoryService

ToolHandler = Callable[[Connection, UUID, dict[str, Any]], dict[str, Any]]
ToolArgumentParser = Callable[[dict[str, Any]], dict[str, Any]]
AnalyticsRunner = Callable[..., Any]
GoalLister = Callable[[Connection, UUID, int], list[Goal]]

_ALLOWED_WINDOWS = {"last_7_days", "last_30_days", "month_to_date", "custom"}
_ALLOWED_MEMORY_KINDS: set[str] = {"goal", "preference", "note"}
_TOOL_ALIASES: dict[str, str] = {
    "get_category_summary": "get_category_spend",
    "get_spending_by_category": "get_category_spend",
}


class ToolRegistryError(RuntimeError):
    """Base error for tool registry issues."""


class UnsupportedToolError(ToolRegistryError):
    """Raised when model requests a tool that is not registered."""


class ToolValidationError(ToolRegistryError):
    """Raised when tool arguments fail validation."""


class ToolExecutionError(ToolRegistryError):
    """Raised when a grounded tool execution fails."""


@dataclass(frozen=True, slots=True)
class RegisteredTool:
    definition: ChatToolDefinition
    parse_args: ToolArgumentParser
    execute: ToolHandler


@dataclass(slots=True)
class AgentToolRegistry:
    memory_service: SemanticMemoryService
    analytics_runner: AnalyticsRunner = run_analytics
    goal_lister: GoalLister = GoalRepository.list_by_user
    _tools: dict[str, RegisteredTool] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._tools: dict[str, RegisteredTool] = {
            "get_kpi_summary": RegisteredTool(
                definition=ChatToolDefinition(
                    name="get_kpi_summary",
                    description=(
                        "Return deterministic KPI summary for a user and time window."
                    ),
                    input_schema={
                        "type": "object",
                        "properties": _common_analytics_properties(),
                        "required": [],
                        "additionalProperties": False,
                    },
                ),
                parse_args=_parse_analytics_args,
                execute=self._execute_get_kpi_summary,
            ),
            "get_category_spend": RegisteredTool(
                definition=ChatToolDefinition(
                    name="get_category_spend",
                    description=(
                        "Return deterministic spending by category for a user and window."
                    ),
                    input_schema={
                        "type": "object",
                        "properties": _common_analytics_properties(),
                        "required": [],
                        "additionalProperties": False,
                    },
                ),
                parse_args=_parse_analytics_args,
                execute=self._execute_get_category_spend,
            ),
            "get_recurring_candidates": RegisteredTool(
                definition=ChatToolDefinition(
                    name="get_recurring_candidates",
                    description=(
                        "Return deterministic recurring transaction candidates "
                        "for a user and window."
                    ),
                    input_schema={
                        "type": "object",
                        "properties": _common_analytics_properties(),
                        "required": [],
                        "additionalProperties": False,
                    },
                ),
                parse_args=_parse_analytics_args,
                execute=self._execute_get_recurring,
            ),
            "get_anomalies": RegisteredTool(
                definition=ChatToolDefinition(
                    name="get_anomalies",
                    description="Return deterministic anomaly events for a user and window.",
                    input_schema={
                        "type": "object",
                        "properties": _common_analytics_properties(),
                        "required": [],
                        "additionalProperties": False,
                    },
                ),
                parse_args=_parse_analytics_args,
                execute=self._execute_get_anomalies,
            ),
            "search_semantic_memory": RegisteredTool(
                definition=ChatToolDefinition(
                    name="search_semantic_memory",
                    description=(
                        "Search semantic memory for the user and return ranked grounded matches."
                    ),
                    input_schema={
                        "type": "object",
                        "properties": {
                            "query_text": {"type": "string"},
                            "kind": {
                                "type": ["string", "null"],
                                "enum": ["goal", "preference", "note", None],
                            },
                            "top_k": {"type": "integer", "minimum": 1, "maximum": 50},
                        },
                        "required": ["query_text"],
                        "additionalProperties": False,
                    },
                ),
                parse_args=_parse_semantic_search_args,
                execute=self._execute_search_semantic_memory,
            ),
            "assemble_context_bundle": RegisteredTool(
                definition=ChatToolDefinition(
                    name="assemble_context_bundle",
                    description=(
                        "Assemble a deterministic context bundle with memory, goals, "
                        "and KPI summary."
                    ),
                    input_schema={
                        "type": "object",
                        "properties": {
                            "query_text": {"type": "string"},
                            "top_k": {"type": "integer", "minimum": 1, "maximum": 50},
                            "kind_filter": {
                                "type": ["string", "null"],
                                "enum": ["goal", "preference", "note", None],
                            },
                            **_common_analytics_properties(),
                        },
                        "required": ["query_text"],
                        "additionalProperties": False,
                    },
                ),
                parse_args=_parse_context_bundle_args,
                execute=self._execute_assemble_context_bundle,
            ),
            "list_recent_goals": RegisteredTool(
                definition=ChatToolDefinition(
                    name="list_recent_goals",
                    description="List recent goals for the user in deterministic order.",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                        },
                        "required": [],
                        "additionalProperties": False,
                    },
                ),
                parse_args=_parse_recent_goals_args,
                execute=self._execute_list_recent_goals,
            ),
        }

    def list_tool_definitions(self) -> list[ChatToolDefinition]:
        return [
            self._tools["get_kpi_summary"].definition,
            self._tools["get_category_spend"].definition,
            self._tools["get_recurring_candidates"].definition,
            self._tools["get_anomalies"].definition,
            self._tools["search_semantic_memory"].definition,
            self._tools["assemble_context_bundle"].definition,
            self._tools["list_recent_goals"].definition,
        ]

    def execute_tool(
        self,
        conn: Connection,
        user_id: UUID,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> ToolExecutionRecord:
        resolved_tool_name = _TOOL_ALIASES.get(tool_name, tool_name)
        tool = self._tools.get(resolved_tool_name)
        if tool is None:
            raise UnsupportedToolError(f"Unsupported tool requested: {tool_name}")
        if not isinstance(arguments, dict):
            raise ToolValidationError("tool arguments must be an object")

        try:
            parsed_args = tool.parse_args(arguments)
        except ValueError as exc:
            raise ToolValidationError(f"Invalid arguments for {tool_name}: {exc}") from exc

        try:
            output = tool.execute(conn, user_id, parsed_args)
        except Exception as exc:
            raise ToolExecutionError(f"Tool {resolved_tool_name} execution failed: {exc}") from exc

        return ToolExecutionRecord(
            tool_name=resolved_tool_name,
            arguments=parsed_args,
            output=output,
        )

    def _execute_get_kpi_summary(
        self,
        conn: Connection,
        user_id: UUID,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        result = self.analytics_runner(conn=conn, user_id=user_id, **arguments)
        payload = result.to_dict()
        return {
            "time_window": payload["time_window"],
            "kpis": payload["kpis"],
            "meta": payload["meta"],
        }

    def _execute_get_category_spend(
        self,
        conn: Connection,
        user_id: UUID,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        result = self.analytics_runner(conn=conn, user_id=user_id, **arguments)
        payload = result.to_dict()
        kpis = payload["kpis"]
        return {
            "time_window": payload["time_window"],
            "spending_by_category": kpis["spending_by_category"],
            "top_spending_categories": kpis["top_spending_categories"],
            "meta": payload["meta"],
        }

    def _execute_get_recurring(
        self,
        conn: Connection,
        user_id: UUID,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        result = self.analytics_runner(conn=conn, user_id=user_id, **arguments)
        payload = result.to_dict()
        return {
            "time_window": payload["time_window"],
            "recurring_candidates": payload["recurring_candidates"],
            "meta": payload["meta"],
        }

    def _execute_get_anomalies(
        self,
        conn: Connection,
        user_id: UUID,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        result = self.analytics_runner(conn=conn, user_id=user_id, **arguments)
        payload = result.to_dict()
        return {
            "time_window": payload["time_window"],
            "anomaly_events": payload["anomaly_events"],
            "meta": payload["meta"],
        }

    def _execute_search_semantic_memory(
        self,
        conn: Connection,
        user_id: UUID,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        matches = self.memory_service.query_memory(
            conn,
            user_id=user_id,
            query_text=arguments["query_text"],
            kind=arguments.get("kind"),
            top_k=arguments.get("top_k", 5),
        )

        serialized_matches = [
            {
                "id": item.id,
                "kind": item.kind,
                "text": item.text,
                "score": f"{item.score:.4f}",
                "metadata": _serialize(item.metadata),
                "created_at": item.created_at.isoformat(),
            }
            for item in matches
        ]

        return {
            "matches": serialized_matches,
            "count": len(serialized_matches),
        }

    def _execute_assemble_context_bundle(
        self,
        conn: Connection,
        user_id: UUID,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        analytics_args = {
            key: arguments[key]
            for key in ("window", "custom_start", "custom_end", "account_filter_ids")
            if key in arguments
        }
        analytics_result = self.analytics_runner(conn=conn, user_id=user_id, **analytics_args)
        analytics_payload = analytics_result.to_dict()
        kpis = analytics_payload["kpis"]

        analytics_summary = {
            "time_window": analytics_payload["time_window"]["label"],
            "total_income": kpis["total_income"],
            "total_expenses": kpis["total_expenses"],
            "net_cashflow": kpis["net_cashflow"],
        }

        bundle = assemble_context_bundle(
            conn,
            memory_service=self.memory_service,
            user_id=user_id,
            query_text=arguments["query_text"],
            top_k=arguments.get("top_k", 5),
            kind_filter=arguments.get("kind_filter"),
            analytics_summary=analytics_summary,
        )
        return bundle.to_dict()

    def _execute_list_recent_goals(
        self,
        conn: Connection,
        user_id: UUID,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        goals = self.goal_lister(conn, user_id, arguments.get("limit", 10))

        return {
            "goals": [
                {
                    "goal_id": str(item.goal_id),
                    "title": item.title,
                    "description": item.description,
                    "status": item.status,
                    "target_amount": _format_decimal(item.target_amount),
                    "target_date": item.target_date.isoformat() if item.target_date else None,
                    "created_at": item.created_at.isoformat(),
                }
                for item in goals
            ],
            "count": len(goals),
        }


def _common_analytics_properties() -> dict[str, Any]:
    return {
        "window": {
            "type": "string",
            "enum": ["last_7_days", "last_30_days", "month_to_date", "custom"],
        },
        "custom_start": {"type": ["string", "null"], "format": "date"},
        "custom_end": {"type": ["string", "null"], "format": "date"},
        "account_ids": {
            "type": "array",
            "items": {"type": "string", "format": "uuid"},
        },
    }


def _parse_analytics_args(arguments: dict[str, Any]) -> dict[str, Any]:
    window_obj = arguments.get("window", "last_30_days")
    if not isinstance(window_obj, str) or window_obj not in _ALLOWED_WINDOWS:
        raise ValueError("window must be one of last_7_days, last_30_days, month_to_date, custom")

    custom_start = _parse_optional_date(arguments.get("custom_start"), field_name="custom_start")
    custom_end = _parse_optional_date(arguments.get("custom_end"), field_name="custom_end")

    if window_obj == "custom":
        if custom_start is None or custom_end is None:
            raise ValueError("custom_start and custom_end are required when window=custom")
    else:
        if custom_start is not None or custom_end is not None:
            raise ValueError("custom_start/custom_end can only be used when window=custom")

    account_filter_ids = _parse_optional_uuid_list(
        arguments.get("account_ids"),
        field_name="account_ids",
    )

    parsed: dict[str, Any] = {
        "window": window_obj,
        "account_filter_ids": account_filter_ids,
    }
    if custom_start is not None:
        parsed["custom_start"] = custom_start
    if custom_end is not None:
        parsed["custom_end"] = custom_end

    return parsed


def _parse_semantic_search_args(arguments: dict[str, Any]) -> dict[str, Any]:
    query_obj = arguments.get("query_text")
    if not isinstance(query_obj, str) or query_obj.strip() == "":
        raise ValueError("query_text is required and must be non-empty")

    kind_obj = arguments.get("kind")
    if kind_obj is not None:
        if not isinstance(kind_obj, str) or kind_obj not in _ALLOWED_MEMORY_KINDS:
            raise ValueError("kind must be one of goal, preference, note")

    top_k = _parse_positive_int(arguments.get("top_k", 5), field_name="top_k", max_value=50)

    parsed: dict[str, Any] = {
        "query_text": query_obj,
        "top_k": top_k,
    }
    if kind_obj is not None:
        parsed["kind"] = kind_obj

    return parsed


def _parse_context_bundle_args(arguments: dict[str, Any]) -> dict[str, Any]:
    base = _parse_analytics_args(arguments)

    query_obj = arguments.get("query_text")
    if not isinstance(query_obj, str) or query_obj.strip() == "":
        raise ValueError("query_text is required and must be non-empty")

    top_k = _parse_positive_int(arguments.get("top_k", 5), field_name="top_k", max_value=50)

    kind_filter_obj = arguments.get("kind_filter")
    if kind_filter_obj is not None:
        if not isinstance(kind_filter_obj, str) or kind_filter_obj not in _ALLOWED_MEMORY_KINDS:
            raise ValueError("kind_filter must be one of goal, preference, note")

    base["query_text"] = query_obj
    base["top_k"] = top_k
    if kind_filter_obj is not None:
        base["kind_filter"] = kind_filter_obj

    return base


def _parse_recent_goals_args(arguments: dict[str, Any]) -> dict[str, Any]:
    limit = _parse_positive_int(arguments.get("limit", 10), field_name="limit", max_value=50)
    return {"limit": limit}


def _parse_optional_date(value: Any, field_name: str) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO date string")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date string") from exc


def _parse_optional_uuid_list(value: Any, field_name: str) -> list[UUID]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be an array of UUID strings")

    out: list[UUID] = []
    for raw in value:
        if not isinstance(raw, str):
            raise ValueError(f"{field_name} must contain only UUID strings")
        cleaned = raw.strip()
        if cleaned == "":
            continue
        try:
            out.append(UUID(cleaned))
        except ValueError:
            # Model-generated placeholders (for example "user_account_id") should not
            # fail deterministic tool execution. Invalid entries are ignored.
            continue

    return out


def _parse_positive_int(value: Any, field_name: str, max_value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    parsed_value = int(value)
    if parsed_value < 1:
        raise ValueError(f"{field_name} must be positive")
    if parsed_value > max_value:
        raise ValueError(f"{field_name} must be <= {max_value}")
    return parsed_value


def _serialize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return f"{value:.4f}"
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    return value


def _format_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.2f}"
