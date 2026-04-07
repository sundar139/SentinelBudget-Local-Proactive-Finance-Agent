from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal
from statistics import median

from sentinelbudget.analytics.models import AnalyticsLedgerRow, RecurringCandidate


@dataclass(frozen=True, slots=True)
class _CadenceMatch:
    cadence: str
    target_days: int
    tolerance_days: int


_CADENCE_RULES: tuple[_CadenceMatch, ...] = (
    _CadenceMatch(cadence="weekly", target_days=7, tolerance_days=1),
    _CadenceMatch(cadence="biweekly", target_days=14, tolerance_days=2),
    _CadenceMatch(cadence="monthly", target_days=30, tolerance_days=4),
)


def detect_recurring_candidates(rows: list[AnalyticsLedgerRow]) -> list[RecurringCandidate]:
    grouped: dict[tuple[str, str, str], list[AnalyticsLedgerRow]] = {}
    for row in rows:
        label = _normalized_label(row)
        if not label:
            continue

        group_key = (str(row.account_id), row.trans_type, label)
        grouped.setdefault(group_key, []).append(row)

    candidates: list[RecurringCandidate] = []
    for (_, _, label), group_rows in grouped.items():
        if len(group_rows) < 3:
            continue

        group_rows.sort(key=lambda item: item.ts)
        intervals = [
            max(1, (group_rows[index].ts.date() - group_rows[index - 1].ts.date()).days)
            for index in range(1, len(group_rows))
        ]

        cadence_rule = _match_cadence(intervals)
        if cadence_rule is None:
            continue

        amounts = [abs(row.amount) for row in group_rows]
        median_amount = median(amounts).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )

        if median_amount == Decimal("0.00"):
            continue

        max_rel_dev = max(abs(amount - median_amount) / median_amount for amount in amounts)
        if max_rel_dev > Decimal("0.35"):
            continue

        cadence_dev = median(
            [
                Decimal(abs(interval - cadence_rule.target_days))
                / Decimal(cadence_rule.tolerance_days)
                for interval in intervals
            ]
        )
        cadence_score = max(Decimal("0.0"), Decimal("1.0") - cadence_dev)
        amount_score = max(Decimal("0.0"), Decimal("1.0") - max_rel_dev)
        occurrence_score = min(Decimal("1.0"), Decimal(len(group_rows)) / Decimal("6.0"))

        confidence = (
            Decimal("0.45") * cadence_score
            + Decimal("0.35") * amount_score
            + Decimal("0.20") * occurrence_score
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        if confidence < Decimal("0.60"):
            continue

        latest_date = group_rows[-1].ts.date()
        expected_next = latest_date + timedelta(days=cadence_rule.target_days)

        category_id = next(
            (row.category_id for row in group_rows if row.category_id is not None),
            None,
        )
        category_name = next((row.category_name for row in group_rows if row.category_name), None)

        sample_keys = [row.trans_key for row in group_rows[-5:]]

        candidates.append(
            RecurringCandidate(
                normalized_label=label,
                category_id=category_id,
                category_name=category_name,
                estimated_cadence=cadence_rule.cadence,
                expected_next_date=expected_next,
                median_amount=median_amount,
                confidence=confidence,
                sample_occurrences=sample_keys,
                explanation=(
                    f"Detected {len(group_rows)} occurrences with {cadence_rule.cadence} cadence "
                    f"and stable median amount {median_amount:.2f}."
                ),
            )
        )

    candidates.sort(key=lambda item: (-item.confidence, item.normalized_label))
    return candidates


def _normalized_label(row: AnalyticsLedgerRow) -> str:
    merchant = row.metadata.get("merchant") if isinstance(row.metadata, dict) else None
    if isinstance(merchant, str) and merchant.strip():
        return " ".join(merchant.strip().lower().split())

    if row.description:
        return " ".join(row.description.strip().lower().split())

    return ""


def _match_cadence(intervals: list[int]) -> _CadenceMatch | None:
    for rule in _CADENCE_RULES:
        if all(abs(interval - rule.target_days) <= rule.tolerance_days for interval in intervals):
            return rule

    return None
