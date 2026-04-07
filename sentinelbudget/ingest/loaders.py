from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from sentinelbudget.ingest.models import RawInputRow


@dataclass(frozen=True, slots=True)
class FinanceColumnMapping:
    date: str = "date"
    description: str = "description"
    amount: str = "amount"
    category: str = "category"
    institution: str = "institution"
    account: str = "account"
    currency: str = "currency"


def _strip_row(row: dict[str, str | None]) -> dict[str, str]:
    return {key: (value or "").strip() for key, value in row.items()}


def _read_csv(file_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with file_path.open(mode="r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        if reader.fieldnames is None:
            raise ValueError(f"CSV file has no header row: {file_path}")

        headers = [header.strip() for header in reader.fieldnames]
        return headers, [_strip_row(row) for row in reader]


def _validate_required_headers(
    headers: list[str],
    required_headers: set[str],
    dataset_label: str,
) -> None:
    missing = sorted(required_headers.difference(set(headers)))
    if missing:
        raise ValueError(
            f"Missing required {dataset_label} CSV columns: {', '.join(missing)}"
        )


def _validate_finance_mapping(mapping: FinanceColumnMapping) -> None:
    values = [
        mapping.date,
        mapping.description,
        mapping.amount,
        mapping.category,
        mapping.institution,
        mapping.account,
        mapping.currency,
    ]

    if any(not item.strip() for item in values):
        raise ValueError("Finance column mapping values must be non-empty")

    if len(set(values)) != len(values):
        raise ValueError("Finance column mapping values must be unique")


def load_finance_csv(
    file_path: Path,
    source_dataset: str,
    column_mapping: FinanceColumnMapping | None = None,
) -> list[RawInputRow]:
    mapping = column_mapping or FinanceColumnMapping()
    _validate_finance_mapping(mapping)

    headers, rows = _read_csv(file_path)
    _validate_required_headers(
        headers=headers,
        required_headers={
            mapping.date,
            mapping.description,
            mapping.amount,
            mapping.category,
            mapping.institution,
            mapping.account,
            mapping.currency,
        },
        dataset_label="finance",
    )

    normalized: list[RawInputRow] = []
    for index, row in enumerate(rows, start=2):
        payload = {
            "date": row.get(mapping.date, ""),
            "description": row.get(mapping.description, ""),
            "amount": row.get(mapping.amount, ""),
            "category": row.get(mapping.category, ""),
            "institution": row.get(mapping.institution, ""),
            "account": row.get(mapping.account, ""),
            "currency": row.get(mapping.currency, ""),
        }
        normalized.append(
            RawInputRow(
                row_number=index,
                source_dataset=source_dataset,
                payload=payload,
            )
        )

    return normalized


def load_retail_csv(file_path: Path, source_dataset: str) -> list[RawInputRow]:
    headers, rows = _read_csv(file_path)
    _validate_required_headers(
        headers=headers,
        required_headers={"transaction_date", "total_amount", "category", "discount", "notes"},
        dataset_label="retail",
    )

    if "merchant" not in headers and "product" not in headers:
        raise ValueError("Retail CSV must include either merchant or product column")

    normalized: list[RawInputRow] = []
    for index, row in enumerate(rows, start=2):
        payload = {
            "transaction_date": row.get("transaction_date", ""),
            "merchant": row.get("merchant", row.get("product", "")),
            "total_amount": row.get("total_amount", ""),
            "discount": row.get("discount", ""),
            "category": row.get("category", ""),
            "currency": row.get("currency", ""),
            "notes": row.get("notes", ""),
        }
        normalized.append(
            RawInputRow(
                row_number=index,
                source_dataset=source_dataset,
                payload=payload,
            )
        )

    return normalized
