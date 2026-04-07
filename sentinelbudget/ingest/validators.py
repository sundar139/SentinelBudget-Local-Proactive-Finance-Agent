from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from sentinelbudget.ingest.models import NormalizedTransactionRecord

_DECIMAL_RE = re.compile(r"^-?\d+(\.\d+)?$")


def parse_decimal(value: str) -> Decimal:
    text = value.strip()
    if not text:
        raise ValueError("Amount is empty")

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    text = text.replace("$", "").replace(",", "")

    if text.startswith("+"):
        text = text[1:]

    if not _DECIMAL_RE.match(text):
        raise ValueError(f"Invalid decimal amount: {value}")

    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:  # pragma: no cover
        raise ValueError(f"Invalid decimal amount: {value}") from exc

    if negative:
        parsed = -parsed

    return parsed.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def parse_timestamp(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError("Timestamp is empty")

    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    )

    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            continue

    try:
        parsed_iso = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {value}") from exc

    if parsed_iso.tzinfo is None:
        return parsed_iso.replace(tzinfo=UTC)

    return parsed_iso.astimezone(UTC)


def normalize_currency(value: str | None) -> str:
    if value is None or not value.strip():
        return "USD"

    currency = value.strip().upper()
    if len(currency) != 3 or not currency.isalpha():
        raise ValueError(f"Currency must be a 3-letter code, got: {value}")
    return currency


def validate_normalized_record(record: NormalizedTransactionRecord) -> None:
    if record.ts.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware")

    if record.currency != record.currency.upper() or len(record.currency) != 3:
        raise ValueError("Currency must be uppercase 3-letter code")

    if record.trans_type not in {"debit", "credit"}:
        raise ValueError("trans_type must be either debit or credit")

    if record.direction not in {"inflow", "outflow"}:
        raise ValueError("direction must be either inflow or outflow")

    if record.source_row_hash.strip() == "":
        raise ValueError("source_row_hash cannot be empty")

    if record.trans_type == "debit" and record.amount > Decimal("0.00"):
        raise ValueError("debit transactions must have non-positive amounts")

    if record.trans_type == "credit" and record.amount < Decimal("0.00"):
        raise ValueError("credit transactions must have non-negative amounts")

    if record.direction == "inflow" and record.amount < Decimal("0.00"):
        raise ValueError("inflow transactions must have non-negative amounts")

    if record.direction == "outflow" and record.amount > Decimal("0.00"):
        raise ValueError("outflow transactions must have non-positive amounts")

    if record.amount == Decimal("0.00"):
        raise ValueError("Amount cannot be zero")
