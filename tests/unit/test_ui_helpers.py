from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sentinelbudget.db.repositories.ledger import LedgerEntryWithContext
from ui.helpers import (
    build_monthly_cashflow_points,
    build_transaction_records,
    category_chart_rows,
    count_anomalies_by_severity,
    filter_transaction_records,
    has_valid_custom_date_range,
    records_to_csv,
    transaction_records_to_rows,
)


def _row(
    trans_key: int,
    ts: datetime,
    amount: Decimal,
    account_name: str = "Checking",
    category_name: str | None = "Food",
    description: str | None = "Merchant",
) -> LedgerEntryWithContext:
    return LedgerEntryWithContext(
        trans_key=trans_key,
        account_id=uuid4(),
        account_name=account_name,
        institution="Demo Bank",
        category_id=1,
        category_name=category_name,
        ts=ts,
        amount=amount,
        currency="USD",
        trans_type="debit" if amount < 0 else "credit",
        description=description,
        metadata={},
    )


def test_build_monthly_cashflow_points_empty_and_grouped() -> None:
    assert build_monthly_cashflow_points([]) == []

    rows = [
        _row(1, datetime(2026, 1, 5, tzinfo=UTC), Decimal("100.00")),
        _row(2, datetime(2026, 1, 10, tzinfo=UTC), Decimal("-20.00")),
        _row(3, datetime(2026, 2, 2, tzinfo=UTC), Decimal("-10.00")),
    ]

    points = build_monthly_cashflow_points(rows)
    assert len(points) == 2
    assert points[0].month == "2026-01"
    assert points[0].income == Decimal("100.00")
    assert points[0].expenses == Decimal("20.00")
    assert points[0].net == Decimal("80.00")


def test_transaction_filter_and_csv_helpers() -> None:
    rows = [
        _row(1, datetime(2026, 4, 1, tzinfo=UTC), Decimal("-15.00"), category_name="Food"),
        _row(2, datetime(2026, 4, 2, tzinfo=UTC), Decimal("50.00"), category_name="Income"),
    ]

    records = build_transaction_records(rows, anomaly_trans_keys={1})
    filtered = filter_transaction_records(
        records,
        account_names={"Checking"},
        categories={"Food"},
        directions={"outflow"},
        search_text="merch",
        anomalies_only=True,
    )
    assert len(filtered) == 1
    assert filtered[0].trans_key == 1

    table_rows = transaction_records_to_rows(filtered)
    csv_payload = records_to_csv(table_rows)
    assert "trans_key" in csv_payload
    assert "1" in csv_payload


def test_category_and_anomaly_helpers() -> None:
    categories = category_chart_rows(
        [
            {"category_name": "Rent", "total_spend": "1200.00"},
            {"category_name": "Food", "total_spend": "300.00"},
        ]
    )
    assert categories[0]["category"] == "Rent"

    anomaly_counts = count_anomalies_by_severity(
        [
            {"severity": "high"},
            {"severity": "high"},
            {"severity": "medium"},
            {"severity": "unexpected"},
        ]
    )
    assert anomaly_counts["high"] == 2
    assert anomaly_counts["medium"] == 1
    assert anomaly_counts["unknown"] == 1


def test_custom_date_range_validation_helper() -> None:
    assert has_valid_custom_date_range(None, None) is True
    assert has_valid_custom_date_range(datetime(2026, 4, 1).date(), None) is True
    assert has_valid_custom_date_range(None, datetime(2026, 4, 2).date()) is True
    assert (
        has_valid_custom_date_range(
            datetime(2026, 4, 1).date(),
            datetime(2026, 4, 2).date(),
        )
        is True
    )
    assert (
        has_valid_custom_date_range(
            datetime(2026, 4, 3).date(),
            datetime(2026, 4, 2).date(),
        )
        is False
    )


def test_records_to_csv_empty_payload_returns_empty_string() -> None:
    assert records_to_csv([]) == ""
