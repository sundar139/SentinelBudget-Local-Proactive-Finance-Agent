from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from ui.formatters import (
    format_date,
    format_datetime,
    format_money,
    format_percent,
    format_window_label,
    pretty_json,
    severity_label,
    severity_rank,
)


def test_format_money_handles_signs_and_none() -> None:
    assert format_money(Decimal("1234.5")) == "USD 1,234.50"
    assert format_money(Decimal("-5.5")) == "-USD 5.50"
    assert format_money(None) == "n/a"


def test_format_percent_and_date_helpers() -> None:
    assert format_percent(Decimal("0.251")) == "25.10%"
    assert format_percent(None) == "n/a"

    ts = datetime(2026, 4, 6, 12, 0, tzinfo=UTC)
    assert format_date(ts) == "2026-04-06"
    assert "2026-04-06" in format_datetime(ts)


def test_severity_helpers_are_stable() -> None:
    assert severity_rank("high") < severity_rank("medium") < severity_rank("low")
    assert severity_label("medium") == "MEDIUM"
    assert severity_label("unknown") == "UNKNOWN"


def test_pretty_json_and_window_label() -> None:
    rendered = pretty_json({"b": 2, "a": 1})
    assert '"a": 1' in rendered
    assert '"b": 2' in rendered
    assert format_window_label("last_30_days") == "Last 30 Days"
    assert format_window_label("custom_value") == "custom_value"
