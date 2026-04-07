from __future__ import annotations

import json
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any


def _to_decimal(value: Decimal | float | int | str | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float, str)):
        return Decimal(str(value))
    return None


def format_money(
    value: Decimal | float | int | str | None,
    currency: str = "USD",
) -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "n/a"

    rounded = parsed.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    sign = "-" if rounded < 0 else ""
    return f"{sign}{currency} {abs(rounded):,.2f}"


def format_percent(ratio_value: Decimal | float | int | str | None, digits: int = 2) -> str:
    parsed = _to_decimal(ratio_value)
    if parsed is None:
        return "n/a"

    percent = parsed * Decimal("100")
    quant = Decimal("1").scaleb(-digits)
    rounded = percent.quantize(quant, rounding=ROUND_HALF_UP)
    return f"{rounded:.{digits}f}%"


def format_date(value: date | datetime | None) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, datetime):
        value = value.date()
    return value.isoformat()


def format_datetime(value: datetime | None) -> str:
    if value is None:
        return "n/a"
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def severity_rank(severity: str) -> int:
    normalized = severity.strip().lower()
    order = {"high": 0, "medium": 1, "low": 2}
    return order.get(normalized, 3)


def severity_label(severity: str) -> str:
    normalized = severity.strip().lower()
    if normalized in {"high", "medium", "low"}:
        return normalized.upper()
    return "UNKNOWN"


def pretty_json(payload: dict[str, Any] | list[Any] | None) -> str:
    if payload is None:
        return "{}"
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def format_window_label(window_key: str) -> str:
    options = {
        "last_7_days": "Last 7 Days",
        "last_30_days": "Last 30 Days",
        "month_to_date": "Month to Date",
        "custom": "Custom Range",
    }
    return options.get(window_key, window_key)
