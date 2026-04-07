from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

DatasetType = Literal["finance", "retail", "synthetic"]
Direction = Literal["inflow", "outflow"]


@dataclass(frozen=True, slots=True)
class RawInputRow:
    row_number: int
    source_dataset: str
    payload: dict[str, str]


@dataclass(frozen=True, slots=True)
class NormalizedTransactionRecord:
    account_id: UUID
    ts: datetime
    amount: Decimal
    currency: str
    trans_type: str
    description: str | None
    merchant: str | None
    source_dataset: str
    source_row_hash: str
    raw_category: str | None
    normalized_category_name: str | None
    is_recurring_candidate: bool
    direction: Direction
    raw_payload: dict[str, str]
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CanonicalTransaction:
    trans_key: int
    account_id: UUID
    category_id: int | None
    ts: datetime
    amount: Decimal
    currency: str
    trans_type: str
    description: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class QuarantineItem:
    row_number: int
    source_dataset: str
    reason: str
    payload: dict[str, str]


@dataclass(frozen=True, slots=True)
class IngestSummary:
    dataset_type: DatasetType
    source_dataset: str
    total_rows: int
    inserted_rows: int
    duplicate_rows: int
    quarantined_rows: int
    normalized_rows: int
    catastrophic_failure: bool
    quarantined: list[QuarantineItem] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_type": self.dataset_type,
            "source_dataset": self.source_dataset,
            "total_rows": self.total_rows,
            "inserted_rows": self.inserted_rows,
            "duplicate_rows": self.duplicate_rows,
            "quarantined_rows": self.quarantined_rows,
            "normalized_rows": self.normalized_rows,
            "catastrophic_failure": self.catastrophic_failure,
            "quarantined": [
                {
                    "row_number": item.row_number,
                    "source_dataset": item.source_dataset,
                    "reason": item.reason,
                    "payload": item.payload,
                }
                for item in self.quarantined
            ],
        }
