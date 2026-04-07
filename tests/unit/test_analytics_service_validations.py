from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sentinelbudget.analytics.models import AnalyticsLedgerRow
from sentinelbudget.analytics.service import _validate_currency_consistency, run_analytics


def _ledger_row(currency: str) -> AnalyticsLedgerRow:
    return AnalyticsLedgerRow(
        trans_key=1,
        account_id=uuid4(),
        account_name="Checking",
        institution="Test Bank",
        category_id=None,
        category_name=None,
        ts=datetime(2026, 4, 1, tzinfo=UTC),
        amount=Decimal("-10.00"),
        currency=currency,
        trans_type="debit",
        description="Test",
        metadata={},
    )


def test_validate_currency_consistency_rejects_multiple_currencies() -> None:
    with pytest.raises(ValueError, match="single currency"):
        _validate_currency_consistency([_ledger_row("USD"), _ledger_row("EUR")])


def test_run_analytics_rejects_custom_dates_for_non_custom_window() -> None:
    with pytest.raises(ValueError, match="can only be used when window=custom"):
        run_analytics(
            conn=None,  # type: ignore[arg-type]
            user_id=uuid4(),
            window="last_30_days",
            custom_start=datetime(2026, 4, 1, tzinfo=UTC).date(),
            custom_end=datetime(2026, 4, 30, tzinfo=UTC).date(),
            reference_time=datetime(2026, 4, 30, tzinfo=UTC),
        )
