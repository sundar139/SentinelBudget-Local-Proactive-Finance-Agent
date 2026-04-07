from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from sentinelbudget.ingest.dedup import compute_trans_key
from sentinelbudget.ingest.models import CanonicalTransaction


@dataclass(frozen=True, slots=True)
class SyntheticGenerationConfig:
    account_id: UUID
    days: int = 90
    seed: int = 42
    start_date: date = date(2026, 1, 1)
    source_dataset: str = "synthetic-demo"


def generate_synthetic_transactions(
    config: SyntheticGenerationConfig,
    category_name_to_id: dict[str, int] | None = None,
) -> list[CanonicalTransaction]:
    rng = random.Random(config.seed)
    records: list[CanonicalTransaction] = []
    category_ids = category_name_to_id or {}

    def add_transaction(
        event_ts: datetime,
        amount: Decimal,
        description: str,
        category_name: str | None,
        event_type: str,
        source_index: int,
    ) -> None:
        source_row_hash = f"synthetic:{config.seed}:{source_index}:{event_type}"
        trans_type = "credit" if amount > Decimal("0.00") else "debit"
        category_id = category_ids.get((category_name or "").lower())

        metadata: dict[str, Any] = {
            "source": "synthetic",
            "source_dataset": config.source_dataset,
            "event_type": event_type,
            "seed": config.seed,
            "normalized_category_name": category_name,
            "source_row_hash": source_row_hash,
        }

        trans_key = compute_trans_key(
            account_id=config.account_id,
            ts=event_ts,
            amount=amount,
            currency="USD",
            trans_type=trans_type,
            description=description,
            source_row_hash=source_row_hash,
        )

        records.append(
            CanonicalTransaction(
                trans_key=trans_key,
                account_id=config.account_id,
                category_id=category_id,
                ts=event_ts,
                amount=amount,
                currency="USD",
                trans_type=trans_type,
                description=description,
                metadata=metadata,
            )
        )

    source_index = 0
    for day_offset in range(config.days):
        current_date = config.start_date + timedelta(days=day_offset)
        event_ts = datetime.combine(current_date, datetime.min.time(), tzinfo=UTC)

        if current_date.day == 1:
            source_index += 1
            add_transaction(
                event_ts + timedelta(hours=9),
                Decimal("4500.00"),
                "Payroll Deposit",
                "Salary",
                "salary",
                source_index,
            )

        if current_date.day == 2:
            source_index += 1
            add_transaction(
                event_ts + timedelta(hours=8),
                Decimal("-1600.00"),
                "Monthly Rent",
                "Rent",
                "rent",
                source_index,
            )

        if current_date.day in {5, 20}:
            source_index += 1
            utility_amount = Decimal(str(rng.randint(80, 210))) + Decimal("0.37")
            add_transaction(
                event_ts + timedelta(hours=7),
                -utility_amount,
                "Utilities Bundle",
                "Electric",
                "utilities",
                source_index,
            )

        if current_date.day % 30 == 12:
            source_index += 1
            add_transaction(
                event_ts + timedelta(hours=6),
                Decimal("-18.99"),
                "Streaming Subscription",
                "Subscriptions",
                "subscription",
                source_index,
            )

        if current_date.weekday() in {1, 4}:
            source_index += 1
            groceries_amount = Decimal(str(rng.randint(45, 160))) + Decimal("0.11")
            add_transaction(
                event_ts + timedelta(hours=19),
                -groceries_amount,
                "Grocery Market",
                "Groceries",
                "groceries",
                source_index,
            )

        if current_date.weekday() in {0, 2, 4}:
            source_index += 1
            transport_amount = Decimal(str(rng.randint(6, 18))) + Decimal("0.25")
            add_transaction(
                event_ts + timedelta(hours=8, minutes=30),
                -transport_amount,
                "Public Transit",
                "Public Transit",
                "transport",
                source_index,
            )

        if current_date.weekday() in {3, 5}:
            source_index += 1
            dining_amount = Decimal(str(rng.randint(18, 75))) + Decimal("0.49")
            add_transaction(
                event_ts + timedelta(hours=20),
                -dining_amount,
                "Dining Out",
                "Dining Out",
                "dining",
                source_index,
            )

        if current_date.weekday() == 6:
            source_index += 1
            payment_amount = Decimal(str(rng.randint(120, 380))) + Decimal("0.00")
            add_transaction(
                event_ts + timedelta(hours=10),
                -payment_amount,
                "Card Payment",
                "Credit Card",
                "payment",
                source_index,
            )

        if current_date.day == 15:
            source_index += 1
            add_transaction(
                event_ts + timedelta(hours=11),
                Decimal("-300.00"),
                "Transfer to Savings",
                "Emergency Fund",
                "transfer",
                source_index,
            )

        if day_offset in {35, 70}:
            source_index += 1
            spike_amount = Decimal("-920.00") if day_offset == 35 else Decimal("-1240.00")
            spike_desc = "Unexpected Car Repair" if day_offset == 35 else "Urgent Medical Expense"
            add_transaction(
                event_ts + timedelta(hours=14),
                spike_amount,
                spike_desc,
                "Miscellaneous",
                "anomaly_spike",
                source_index,
            )

    records.sort(key=lambda item: (item.ts, item.trans_key))
    return records


def write_synthetic_finance_csv(records: list[CanonicalTransaction], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open(mode="w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=["date", "description", "amount", "category", "currency", "source_dataset"],
        )
        writer.writeheader()

        for record in records:
            writer.writerow(
                {
                    "date": record.ts.date().isoformat(),
                    "description": record.description or "",
                    "amount": f"{record.amount:.2f}",
                    "category": record.metadata.get("normalized_category_name") or "",
                    "currency": record.currency,
                    "source_dataset": record.metadata.get("source_dataset") or "",
                }
            )
