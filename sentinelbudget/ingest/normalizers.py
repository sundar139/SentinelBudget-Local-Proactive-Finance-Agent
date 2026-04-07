from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

from sentinelbudget.ingest.dedup import compute_source_row_hash
from sentinelbudget.ingest.models import Direction, NormalizedTransactionRecord, RawInputRow
from sentinelbudget.ingest.validators import normalize_currency, parse_decimal, parse_timestamp

DEFAULT_CATEGORY_MAPPING: dict[str, str] = {
    "salary": "Salary",
    "payroll": "Salary",
    "rent": "Rent",
    "groceries": "Groceries",
    "grocery": "Groceries",
    "dining": "Dining Out",
    "restaurant": "Dining Out",
    "utilities": "Electric",
    "electric": "Electric",
    "water": "Water",
    "internet": "Internet",
    "phone": "Phone",
    "subscription": "Subscriptions",
    "subscriptions": "Subscriptions",
    "transfer": "Credit Card",
    "payment": "Credit Card",
    "insurance": "Health Insurance",
    "travel": "Travel",
}


def _normalize_lookup(value: str) -> str:
    return " ".join(value.strip().lower().split())


def normalize_category_name(raw_category: str | None, description: str | None = None) -> str | None:
    raw = (raw_category or "").strip()
    if raw:
        normalized_raw = _normalize_lookup(raw)
        if normalized_raw in DEFAULT_CATEGORY_MAPPING:
            return DEFAULT_CATEGORY_MAPPING[normalized_raw]

        for key, target in DEFAULT_CATEGORY_MAPPING.items():
            if key in normalized_raw:
                return target

    desc = (description or "").strip().lower()
    for key, target in DEFAULT_CATEGORY_MAPPING.items():
        if key in desc:
            return target

    return None


def normalize_finance_row(
    row: RawInputRow,
    account_id: UUID,
) -> NormalizedTransactionRecord:
    ts = parse_timestamp(row.payload.get("date", ""))
    description = row.payload.get("description") or None
    amount = parse_decimal(row.payload.get("amount", ""))
    currency = normalize_currency(row.payload.get("currency"))

    direction: Direction = "inflow" if amount > Decimal("0.00") else "outflow"
    trans_type = "credit" if amount > Decimal("0.00") else "debit"

    raw_category = row.payload.get("category") or None
    normalized_category_name = normalize_category_name(raw_category, description)

    metadata: dict[str, Any] = {
        "source": "finance",
        "institution": row.payload.get("institution") or None,
        "account_label": row.payload.get("account") or None,
        "raw_category": raw_category,
    }

    return NormalizedTransactionRecord(
        account_id=account_id,
        ts=ts,
        amount=amount,
        currency=currency,
        trans_type=trans_type,
        description=description,
        merchant=description,
        source_dataset=row.source_dataset,
        source_row_hash=compute_source_row_hash(row.payload),
        raw_category=raw_category,
        normalized_category_name=normalized_category_name,
        is_recurring_candidate=_is_recurring_candidate(description),
        direction=direction,
        raw_payload=row.payload,
        metadata=metadata,
    )


def normalize_retail_row(
    row: RawInputRow,
    account_id: UUID,
) -> NormalizedTransactionRecord:
    ts = parse_timestamp(row.payload.get("transaction_date", ""))
    merchant = row.payload.get("merchant") or None
    description = merchant or row.payload.get("notes") or "Retail purchase"

    total_amount = parse_decimal(row.payload.get("total_amount", ""))
    discount_text = row.payload.get("discount", "")
    discount = parse_decimal(discount_text) if discount_text.strip() else Decimal("0.00")

    net_amount = total_amount - discount
    amount = -abs(net_amount) if net_amount >= Decimal("0.00") else net_amount

    currency = normalize_currency(row.payload.get("currency"))
    trans_type = "credit" if amount > Decimal("0.00") else "debit"
    direction: Direction = "inflow" if amount > Decimal("0.00") else "outflow"

    raw_category = row.payload.get("category") or None
    normalized_category_name = normalize_category_name(raw_category, description)

    metadata: dict[str, Any] = {
        "source": "retail",
        "merchant": merchant,
        "total_amount": f"{total_amount:.2f}",
        "discount": f"{discount:.2f}",
        "raw_category": raw_category,
        "notes": row.payload.get("notes") or None,
    }

    return NormalizedTransactionRecord(
        account_id=account_id,
        ts=ts,
        amount=amount,
        currency=currency,
        trans_type=trans_type,
        description=description,
        merchant=merchant,
        source_dataset=row.source_dataset,
        source_row_hash=compute_source_row_hash(row.payload),
        raw_category=raw_category,
        normalized_category_name=normalized_category_name,
        is_recurring_candidate=_is_recurring_candidate(description),
        direction=direction,
        raw_payload=row.payload,
        metadata=metadata,
    )


def _is_recurring_candidate(description: str | None) -> bool:
    text = (description or "").lower()
    recurring_keywords = ("rent", "subscription", "utilities", "internet", "insurance", "mortgage")
    return any(keyword in text for keyword in recurring_keywords)
