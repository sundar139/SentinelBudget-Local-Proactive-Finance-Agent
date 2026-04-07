from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sentinelbudget.ingest.models import NormalizedTransactionRecord
from sentinelbudget.ingest.validators import (
    normalize_currency,
    parse_decimal,
    validate_normalized_record,
)


def _valid_record() -> NormalizedTransactionRecord:
    return NormalizedTransactionRecord(
        account_id=uuid4(),
        ts=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        amount=Decimal("-10.00"),
        currency="USD",
        trans_type="debit",
        description="Test",
        merchant="Merchant",
        source_dataset="unit",
        source_row_hash="abc123",
        raw_category="Groceries",
        normalized_category_name="Groceries",
        is_recurring_candidate=False,
        direction="outflow",
        raw_payload={"date": "2026-01-01"},
        metadata={"source": "test"},
    )


def test_parse_decimal_supports_parentheses() -> None:
    assert parse_decimal("(123.45)") == Decimal("-123.45")


def test_normalize_currency_rejects_non_alpha() -> None:
    with pytest.raises(ValueError, match="3-letter code"):
        normalize_currency("US1")


def test_validate_normalized_record_rejects_sign_mismatch() -> None:
    record = _valid_record()
    broken = NormalizedTransactionRecord(
        account_id=record.account_id,
        ts=record.ts,
        amount=Decimal("10.00"),
        currency=record.currency,
        trans_type=record.trans_type,
        description=record.description,
        merchant=record.merchant,
        source_dataset=record.source_dataset,
        source_row_hash=record.source_row_hash,
        raw_category=record.raw_category,
        normalized_category_name=record.normalized_category_name,
        is_recurring_candidate=record.is_recurring_candidate,
        direction=record.direction,
        raw_payload=record.raw_payload,
        metadata=record.metadata,
    )

    with pytest.raises(ValueError, match="debit transactions"):
        validate_normalized_record(broken)
