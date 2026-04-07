from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sentinelbudget.analytics.anomalies import detect_anomalies
from sentinelbudget.analytics.models import AnalyticsLedgerRow


def _row(
    trans_key: int,
    ts: datetime,
    amount: str,
    category_id: int,
    category_name: str,
    description: str,
) -> AnalyticsLedgerRow:
    return AnalyticsLedgerRow(
        trans_key=trans_key,
        account_id=uuid4(),
        account_name="Checking",
        institution="Test Bank",
        category_id=category_id,
        category_name=category_name,
        ts=ts,
        amount=Decimal(amount),
        currency="USD",
        trans_type="debit" if Decimal(amount) < 0 else "credit",
        description=description,
        metadata={"merchant": description},
    )


def test_detect_anomalies_with_spikes() -> None:
    baseline = [
        _row(1, datetime(2026, 1, 1, 12, 0, tzinfo=UTC), "-30.00", 3, "Dining Out", "Cafe A"),
        _row(2, datetime(2026, 1, 3, 12, 0, tzinfo=UTC), "-28.00", 3, "Dining Out", "Cafe A"),
        _row(3, datetime(2026, 1, 5, 12, 0, tzinfo=UTC), "-31.00", 3, "Dining Out", "Cafe A"),
        _row(4, datetime(2026, 1, 7, 12, 0, tzinfo=UTC), "-29.00", 3, "Dining Out", "Cafe A"),
        _row(5, datetime(2026, 1, 9, 12, 0, tzinfo=UTC), "-32.00", 3, "Dining Out", "Cafe A"),
        _row(6, datetime(2026, 1, 11, 12, 0, tzinfo=UTC), "-27.00", 3, "Dining Out", "Cafe A"),
        _row(7, datetime(2026, 1, 13, 12, 0, tzinfo=UTC), "-34.00", 3, "Dining Out", "Cafe A"),
    ]

    window_rows = [
        _row(99, datetime(2026, 2, 1, 12, 0, tzinfo=UTC), "-410.00", 3, "Dining Out", "Cafe A"),
        _row(
            100,
            datetime(2026, 2, 1, 14, 0, tzinfo=UTC),
            "-620.00",
            5,
            "Miscellaneous",
            "New Merchant X",
        ),
    ]

    events = detect_anomalies(
        baseline_rows=baseline + window_rows,
        window_start=datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 2, 2, 0, 0, tzinfo=UTC),
    )

    kinds = {event.anomaly_kind for event in events}
    assert "category_spike" in kinds or "transaction_spike" in kinds
    assert "first_seen_large_merchant" in kinds
