from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sentinelbudget.memory.service import (
    _build_parser,
    _goal_to_text,
    _preference_to_text,
    _stable_metadata,
    _validate_kind,
    _validate_non_empty_text,
)
from sentinelbudget.memory.service import (
    main as memory_main,
)


def test_goal_to_text_is_deterministic() -> None:
    goal = SimpleNamespace(
        goal_id=uuid4(),
        title="Emergency Fund",
        status="active",
        target_amount=Decimal("5000.00"),
        target_date=date(2026, 12, 31),
        description="Build a six-month safety net",
    )

    text_a = _goal_to_text(goal)
    text_b = _goal_to_text(goal)

    assert text_a == text_b
    assert "Emergency Fund" in text_a
    assert "5000.00" in text_a


def test_preference_text_is_stable() -> None:
    pref = _preference_to_text("notifications", {"email": True, "threshold": 100})
    assert pref == "Preference 'notifications': {\"email\":true,\"threshold\":100}"


def test_stable_metadata_orders_keys() -> None:
    first = _stable_metadata({"b": 2, "a": 1})
    second = _stable_metadata({"a": 1, "b": 2})
    assert first == second


def test_validate_kind_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="kind must be one of"):
        _validate_kind("unknown")


def test_validate_non_empty_text_rejects_blank() -> None:
    with pytest.raises(ValueError, match="query_text cannot be empty"):
        _validate_non_empty_text("   ", field_name="query_text")


def test_parser_uses_configured_top_k_default() -> None:
    parser = _build_parser(default_top_k=7)
    args = parser.parse_args(
        [
            "query-memory",
            "--user-id",
            "123e4567-e89b-12d3-a456-426614174000",
            "--query",
            "emergency fund status",
        ]
    )

    assert args.top_k == 7


def test_memory_help_does_not_require_settings(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sentinelbudget-memory", "--help"])

    def _fail_if_called():
        raise AssertionError("get_settings should not be called for --help")

    monkeypatch.setattr("sentinelbudget.memory.service.get_settings", _fail_if_called)

    with pytest.raises(SystemExit) as exc_info:
        memory_main()

    assert exc_info.value.code == 0
