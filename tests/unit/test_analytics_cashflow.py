from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from sentinelbudget.analytics.cashflow import resolve_time_window


def test_resolve_last_7_days_window() -> None:
    reference = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)

    window = resolve_time_window("last_7_days", reference)

    assert window.label == "last_7_days"
    assert window.start_ts == datetime(2026, 4, 23, 12, 0, tzinfo=UTC)
    assert window.end_ts == reference


def test_resolve_custom_window() -> None:
    reference = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)

    window = resolve_time_window(
        "custom",
        reference,
        custom_start=date(2026, 4, 1),
        custom_end=date(2026, 4, 30),
    )

    assert window.start_ts.isoformat() == "2026-04-01T00:00:00+00:00"
    assert window.end_ts.date().isoformat() == "2026-04-30"


def test_resolve_custom_window_requires_dates() -> None:
    reference = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="custom_start and custom_end"):
        resolve_time_window("custom", reference)


def test_resolve_custom_window_rejects_invalid_date_order() -> None:
    reference = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="custom_start must be <= custom_end"):
        resolve_time_window(
            "custom",
            reference,
            custom_start=date(2026, 4, 30),
            custom_end=date(2026, 4, 1),
        )
