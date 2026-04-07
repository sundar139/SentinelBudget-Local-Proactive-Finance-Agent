from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sentinelbudget.analytics.models import AnalyticsLedgerRow
from sentinelbudget.analytics.recurring import detect_recurring_candidates


def _make_row(
    trans_key: int,
    ts: datetime,
    amount: str,
    description: str,
    account_id: UUID,
) -> AnalyticsLedgerRow:
    return AnalyticsLedgerRow(
        trans_key=trans_key,
        account_id=account_id,
        account_name="Checking",
        institution="Test Bank",
        category_id=2,
        category_name="Rent",
        ts=ts,
        amount=Decimal(amount),
        currency="USD",
        trans_type="debit" if Decimal(amount) < 0 else "credit",
        description=description,
        metadata={},
    )


def test_detect_recurring_monthly_rent() -> None:
    account_id = uuid4()
    rows = [
        _make_row(
            1,
            datetime(2026, 1, 2, 8, 0, tzinfo=UTC),
            "-1600.00",
            "Monthly Rent",
            account_id,
        ),
        _make_row(
            2,
            datetime(2026, 2, 2, 8, 0, tzinfo=UTC),
            "-1600.00",
            "Monthly Rent",
            account_id,
        ),
        _make_row(
            3,
            datetime(2026, 3, 2, 8, 0, tzinfo=UTC),
            "-1600.00",
            "Monthly Rent",
            account_id,
        ),
        _make_row(
            4,
            datetime(2026, 4, 2, 8, 0, tzinfo=UTC),
            "-1600.00",
            "Monthly Rent",
            account_id,
        ),
    ]

    candidates = detect_recurring_candidates(rows)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.normalized_label == "monthly rent"
    assert candidate.estimated_cadence == "monthly"
    assert candidate.expected_next_date.isoformat() == "2026-05-02"
    assert candidate.median_amount == Decimal("1600.00")


def test_recurring_detection_is_conservative_on_irregular_rows() -> None:
    account_id = uuid4()
    rows = [
        _make_row(1, datetime(2026, 1, 2, 8, 0, tzinfo=UTC), "-20.00", "Coffee", account_id),
        _make_row(2, datetime(2026, 1, 4, 8, 0, tzinfo=UTC), "-35.00", "Coffee", account_id),
        _make_row(3, datetime(2026, 1, 11, 8, 0, tzinfo=UTC), "-11.00", "Coffee", account_id),
        _make_row(4, datetime(2026, 2, 9, 8, 0, tzinfo=UTC), "-64.00", "Coffee", account_id),
    ]

    candidates = detect_recurring_candidates(rows)

    assert candidates == []
