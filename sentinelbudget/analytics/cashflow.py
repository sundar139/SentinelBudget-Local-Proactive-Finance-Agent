from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal

from sentinelbudget.analytics.models import CategorySpendSummary, TimeWindow


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def resolve_time_window(
    window: str,
    reference_time: datetime,
    custom_start: date | None = None,
    custom_end: date | None = None,
) -> TimeWindow:
    if reference_time.tzinfo is None:
        raise ValueError("reference_time must be timezone-aware")

    end_ts = reference_time.astimezone(UTC)

    if window == "last_7_days":
        start_ts = end_ts - timedelta(days=7)
    elif window == "last_30_days":
        start_ts = end_ts - timedelta(days=30)
    elif window == "month_to_date":
        start_ts = datetime(end_ts.year, end_ts.month, 1, tzinfo=UTC)
    elif window == "custom":
        if custom_start is None or custom_end is None:
            raise ValueError("custom_start and custom_end are required for custom window")
        if custom_start > custom_end:
            raise ValueError("custom_start must be <= custom_end")

        start_ts = datetime.combine(custom_start, time.min, tzinfo=UTC)
        end_ts = datetime.combine(custom_end, time.max, tzinfo=UTC)
    else:
        raise ValueError(f"Unsupported window: {window}")

    return TimeWindow(label=window, start_ts=start_ts, end_ts=end_ts)


def category_spend_summaries(
    rows: list[tuple[int | None, str | None, Decimal]],
) -> list[CategorySpendSummary]:
    grouped: dict[tuple[int | None, str], tuple[Decimal, int]] = {}

    for category_id, category_name, amount in rows:
        if amount >= Decimal("0.00"):
            continue

        key = (category_id, category_name or "Uncategorized")
        total, count = grouped.get(key, (Decimal("0.00"), 0))
        grouped[key] = (total + abs(amount), count + 1)

    summaries = [
        CategorySpendSummary(
            category_id=category_id,
            category_name=category_name,
            total_spend=quantize_money(total),
            transaction_count=count,
        )
        for (category_id, category_name), (total, count) in grouped.items()
    ]

    summaries.sort(key=lambda item: (-item.total_spend, item.category_name.lower()))
    return summaries
