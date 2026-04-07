from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from statistics import mean, pstdev

from sentinelbudget.analytics.models import AnalyticsLedgerRow, AnomalyEvent


def detect_anomalies(
    baseline_rows: list[AnalyticsLedgerRow],
    window_start: datetime,
    window_end: datetime,
) -> list[AnomalyEvent]:
    rows = sorted(baseline_rows, key=lambda item: (item.ts, item.trans_key))
    baseline_only = [row for row in rows if row.ts < window_start]
    window_rows = [row for row in rows if window_start <= row.ts <= window_end]

    events: list[AnomalyEvent] = []
    events.extend(_category_spike_events(window_rows, baseline_only))
    events.extend(_single_transaction_spike_events(window_rows, baseline_only))
    events.extend(_daily_total_spike_events(window_rows, baseline_only))
    events.extend(_first_seen_large_merchant_events(window_rows, baseline_only))

    events.sort(
        key=lambda item: (
            item.ts.isoformat() if item.ts else "",
            float(item.score),
            item.anomaly_kind,
        ),
        reverse=True,
    )
    return events


def _category_spike_events(
    window_rows: list[AnalyticsLedgerRow],
    baseline_rows: list[AnalyticsLedgerRow],
) -> list[AnomalyEvent]:
    by_category: dict[int | None, list[Decimal]] = defaultdict(list)
    for row in baseline_rows:
        if row.amount < 0:
            by_category[row.category_id].append(abs(row.amount))

    events: list[AnomalyEvent] = []
    for row in window_rows:
        if row.amount >= 0:
            continue

        samples = by_category.get(row.category_id, [])
        if len(samples) < 5:
            continue

        baseline_mean = mean(samples)
        baseline_std = pstdev(samples)
        if baseline_std <= 0:
            continue

        current = abs(row.amount)
        z_score = (current - baseline_mean) / baseline_std
        if z_score < Decimal("2.5"):
            continue

        score = _score_from_z(z_score)
        events.append(
            AnomalyEvent(
                anomaly_kind="category_spike",
                severity=_severity(score),
                score=score,
                explanation=(
                    f"Transaction amount {current:.2f} exceeds category baseline "
                    f"(mean {baseline_mean:.2f}, z={z_score:.2f})."
                ),
                trans_key=row.trans_key,
                ts=row.ts,
                evidence={
                    "category_id": row.category_id,
                    "category_name": row.category_name,
                    "current_amount": f"{current:.2f}",
                    "baseline_mean": f"{baseline_mean:.2f}",
                    "baseline_std": f"{baseline_std:.2f}",
                    "z_score": f"{z_score.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)}",
                },
            )
        )

    return events


def _single_transaction_spike_events(
    window_rows: list[AnalyticsLedgerRow],
    baseline_rows: list[AnalyticsLedgerRow],
) -> list[AnomalyEvent]:
    by_category: dict[int | None, list[Decimal]] = defaultdict(list)
    for row in baseline_rows:
        if row.amount < 0:
            by_category[row.category_id].append(abs(row.amount))

    events: list[AnomalyEvent] = []
    for row in window_rows:
        if row.amount >= 0:
            continue

        samples = by_category.get(row.category_id, [])
        if len(samples) < 3:
            continue

        baseline_mean = mean(samples)
        current = abs(row.amount)
        if current < Decimal("2.2") * baseline_mean or current < Decimal("250.00"):
            continue

        ratio = current / baseline_mean if baseline_mean > Decimal("0.00") else Decimal("0.00")
        score = min(
            Decimal("0.99"),
            Decimal("0.45") + min(Decimal("0.5"), ratio / Decimal("10")),
        ).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )

        events.append(
            AnomalyEvent(
                anomaly_kind="transaction_spike",
                severity=_severity(score),
                score=score,
                explanation=(
                    f"Large transaction {current:.2f} is {ratio:.2f}x category mean "
                    f"{baseline_mean:.2f}."
                ),
                trans_key=row.trans_key,
                ts=row.ts,
                evidence={
                    "category_id": row.category_id,
                    "category_name": row.category_name,
                    "current_amount": f"{current:.2f}",
                    "baseline_mean": f"{baseline_mean:.2f}",
                    "ratio": f"{ratio.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)}",
                },
            )
        )

    return events


def _daily_total_spike_events(
    window_rows: list[AnalyticsLedgerRow],
    baseline_rows: list[AnalyticsLedgerRow],
) -> list[AnomalyEvent]:
    baseline_daily: dict[date, Decimal] = defaultdict(lambda: Decimal("0.00"))
    for row in baseline_rows:
        if row.amount < 0:
            baseline_daily[row.ts.date()] += abs(row.amount)

    baseline_values = list(baseline_daily.values())
    if len(baseline_values) < 7:
        return []

    daily_mean = mean(baseline_values)
    daily_std = pstdev(baseline_values)
    if daily_std <= 0:
        return []

    window_daily: dict[date, Decimal] = defaultdict(lambda: Decimal("0.00"))
    for row in window_rows:
        if row.amount < 0:
            window_daily[row.ts.date()] += abs(row.amount)

    events: list[AnomalyEvent] = []
    for day, total in sorted(window_daily.items()):
        z_score = (total - daily_mean) / daily_std
        if z_score < Decimal("2.0"):
            continue

        score = _score_from_z(z_score)
        ts = next((row.ts for row in window_rows if row.ts.date() == day), None)

        events.append(
            AnomalyEvent(
                anomaly_kind="daily_spend_spike",
                severity=_severity(score),
                score=score,
                explanation=(
                    f"Daily spend {total:.2f} exceeds trailing daily baseline "
                    f"(mean {daily_mean:.2f}, z={z_score:.2f})."
                ),
                trans_key=None,
                ts=ts,
                evidence={
                    "date": day.isoformat(),
                    "daily_total": f"{total:.2f}",
                    "baseline_mean": f"{daily_mean:.2f}",
                    "baseline_std": f"{daily_std:.2f}",
                    "z_score": f"{z_score.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)}",
                },
            )
        )

    return events


def _first_seen_large_merchant_events(
    window_rows: list[AnalyticsLedgerRow],
    baseline_rows: list[AnalyticsLedgerRow],
) -> list[AnomalyEvent]:
    seen_labels: set[str] = set()
    for row in baseline_rows:
        label = _merchant_label(row)
        if label:
            seen_labels.add(label)

    events: list[AnomalyEvent] = []
    for row in window_rows:
        if row.amount >= 0:
            continue

        label = _merchant_label(row)
        if not label:
            continue

        amount = abs(row.amount)
        if label in seen_labels or amount < Decimal("300.00"):
            seen_labels.add(label)
            continue

        score = Decimal("0.78")
        events.append(
            AnomalyEvent(
                anomaly_kind="first_seen_large_merchant",
                severity=_severity(score),
                score=score,
                explanation=(
                    f"Large first-seen spend at '{label}' for {amount:.2f} "
                    "compared to trailing history."
                ),
                trans_key=row.trans_key,
                ts=row.ts,
                evidence={
                    "label": label,
                    "amount": f"{amount:.2f}",
                    "category_name": row.category_name,
                },
            )
        )
        seen_labels.add(label)

    return events


def _merchant_label(row: AnalyticsLedgerRow) -> str:
    merchant = row.metadata.get("merchant") if isinstance(row.metadata, dict) else None
    if isinstance(merchant, str) and merchant.strip():
        return " ".join(merchant.strip().lower().split())

    if row.description:
        return " ".join(row.description.strip().lower().split())

    return ""


def _severity(score: Decimal) -> str:
    if score >= Decimal("0.85"):
        return "high"
    if score >= Decimal("0.60"):
        return "medium"
    return "low"


def _score_from_z(z_score: Decimal) -> Decimal:
    bounded = min(
        Decimal("0.99"),
        max(
            Decimal("0.40"),
            Decimal("0.40") + min(Decimal("0.55"), z_score / Decimal("8")),
        ),
    )
    return bounded.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
