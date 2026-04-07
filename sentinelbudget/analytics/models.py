from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast
from uuid import UUID


def _serialize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    return value


@dataclass(frozen=True, slots=True)
class TimeWindow:
    label: str
    start_ts: datetime
    end_ts: datetime


@dataclass(frozen=True, slots=True)
class AnalyticsLedgerRow:
    trans_key: int
    account_id: UUID
    account_name: str
    institution: str
    category_id: int | None
    category_name: str | None
    ts: datetime
    amount: Decimal
    currency: str
    trans_type: str
    description: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CategorySpendSummary:
    category_id: int | None
    category_name: str
    total_spend: Decimal
    transaction_count: int


@dataclass(frozen=True, slots=True)
class MonthOverMonthSpendComparison:
    current_period_spend: Decimal
    previous_period_spend: Decimal
    delta_amount: Decimal
    delta_percent: Decimal | None


@dataclass(frozen=True, slots=True)
class AccountBalanceSnapshot:
    account_id: UUID
    account_name: str
    institution: str
    current_balance: Decimal
    period_income: Decimal
    period_expenses: Decimal
    period_net_cashflow: Decimal
    projected_balance: Decimal


@dataclass(frozen=True, slots=True)
class KpiSummary:
    total_income: Decimal
    total_expenses: Decimal
    net_cashflow: Decimal
    average_daily_spend: Decimal
    savings_rate: Decimal | None
    spending_by_category: list[CategorySpendSummary]
    top_spending_categories: list[CategorySpendSummary]
    month_over_month_spend: MonthOverMonthSpendComparison | None
    account_balance_snapshots: list[AccountBalanceSnapshot]


@dataclass(frozen=True, slots=True)
class RecurringCandidate:
    normalized_label: str
    category_id: int | None
    category_name: str | None
    estimated_cadence: str
    expected_next_date: date
    median_amount: Decimal
    confidence: Decimal
    sample_occurrences: list[int]
    explanation: str


@dataclass(frozen=True, slots=True)
class AnomalyEvent:
    anomaly_kind: str
    severity: str
    score: Decimal
    explanation: str
    trans_key: int | None
    ts: datetime | None
    evidence: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AnalyticsRunResult:
    user_id: UUID
    generated_at: datetime
    time_window: TimeWindow
    account_filter_ids: list[UUID]
    kpis: KpiSummary
    recurring_candidates: list[RecurringCandidate]
    anomaly_events: list[AnomalyEvent]
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], _serialize(asdict(self)))
