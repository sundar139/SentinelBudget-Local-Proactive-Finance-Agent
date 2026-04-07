from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID

from psycopg import Connection

from sentinelbudget.config import get_settings
from sentinelbudget.db.repositories import CategoryRepository, LedgerRepository
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.ingest.dedup import compute_trans_key, dedup_fingerprint
from sentinelbudget.ingest.loaders import (
    FinanceColumnMapping,
    load_finance_csv,
    load_retail_csv,
)
from sentinelbudget.ingest.models import (
    CanonicalTransaction,
    DatasetType,
    IngestSummary,
    NormalizedTransactionRecord,
    QuarantineItem,
    RawInputRow,
)
from sentinelbudget.ingest.normalizers import normalize_finance_row, normalize_retail_row
from sentinelbudget.ingest.synthetic import (
    SyntheticGenerationConfig,
    generate_synthetic_transactions,
    write_synthetic_finance_csv,
)
from sentinelbudget.ingest.validators import validate_normalized_record
from sentinelbudget.logging import setup_logging


def _category_lookup(conn: Connection) -> dict[str, int]:
    return {
        category.name.strip().lower(): category.category_id
        for category in CategoryRepository.list_all(conn)
    }


def _normalize_rows(
    dataset_type: DatasetType,
    rows: list[RawInputRow],
    account_id: UUID,
    category_name_to_id: dict[str, int],
) -> tuple[list[CanonicalTransaction], list[QuarantineItem], int]:
    canonical_records: list[CanonicalTransaction] = []
    quarantined: list[QuarantineItem] = []
    duplicate_rows = 0

    seen_trans_keys: set[int] = set()
    seen_fingerprints: set[tuple[str, str, str, str]] = set()

    for row in rows:
        try:
            normalized = _normalize_row(dataset_type=dataset_type, row=row, account_id=account_id)
            validate_normalized_record(normalized)
            canonical = _to_canonical_transaction(
                normalized=normalized,
                category_name_to_id=category_name_to_id,
            )

            fingerprint = dedup_fingerprint(
                account_id=canonical.account_id,
                ts=canonical.ts,
                amount=canonical.amount,
                description=canonical.description,
            )
            if canonical.trans_key in seen_trans_keys or fingerprint in seen_fingerprints:
                duplicate_rows += 1
                continue

            seen_trans_keys.add(canonical.trans_key)
            seen_fingerprints.add(fingerprint)
            canonical_records.append(canonical)
        except ValueError as exc:
            quarantined.append(
                QuarantineItem(
                    row_number=row.row_number,
                    source_dataset=row.source_dataset,
                    reason=str(exc),
                    payload=row.payload,
                )
            )

    return canonical_records, quarantined, duplicate_rows


def normalize_rows_for_ingest(
    dataset_type: DatasetType,
    rows: list[RawInputRow],
    account_id: UUID,
    category_name_to_id: dict[str, int],
) -> tuple[list[CanonicalTransaction], list[QuarantineItem], int]:
    """Normalize and validate rows before database insertion."""

    return _normalize_rows(
        dataset_type=dataset_type,
        rows=rows,
        account_id=account_id,
        category_name_to_id=category_name_to_id,
    )


def _normalize_row(
    dataset_type: DatasetType,
    row: RawInputRow,
    account_id: UUID,
) -> NormalizedTransactionRecord:
    if dataset_type == "finance":
        return normalize_finance_row(row=row, account_id=account_id)
    if dataset_type == "retail":
        return normalize_retail_row(row=row, account_id=account_id)

    raise ValueError(f"Unsupported dataset type for CSV ingest: {dataset_type}")


def _to_canonical_transaction(
    normalized: NormalizedTransactionRecord,
    category_name_to_id: dict[str, int],
) -> CanonicalTransaction:
    category_id: int | None = None
    normalized_name = normalized.normalized_category_name
    if normalized_name:
        category_id = category_name_to_id.get(normalized_name.strip().lower())

    metadata: dict[str, Any] = dict(normalized.metadata)
    metadata.update(
        {
            "source_dataset": normalized.source_dataset,
            "source_row_hash": normalized.source_row_hash,
            "raw_category": normalized.raw_category,
            "normalized_category_name": normalized_name,
            "is_recurring_candidate": normalized.is_recurring_candidate,
            "direction": normalized.direction,
            "raw_payload": normalized.raw_payload,
        }
    )
    if normalized_name and category_id is None:
        metadata["unresolved_category_name"] = normalized_name

    trans_key = compute_trans_key(
        account_id=normalized.account_id,
        ts=normalized.ts,
        amount=normalized.amount,
        currency=normalized.currency,
        trans_type=normalized.trans_type,
        description=normalized.description,
        source_row_hash=normalized.source_row_hash,
    )

    return CanonicalTransaction(
        trans_key=trans_key,
        account_id=normalized.account_id,
        category_id=category_id,
        ts=normalized.ts,
        amount=normalized.amount,
        currency=normalized.currency,
        trans_type=normalized.trans_type,
        description=normalized.description,
        metadata=metadata,
    )


def _insert_canonical_records(
    conn: Connection,
    records: list[CanonicalTransaction],
) -> tuple[int, int]:
    inserted_rows = 0
    duplicate_rows = 0

    for record in records:
        if LedgerRepository.exists_by_natural_key(
            conn,
            account_id=record.account_id,
            ts=record.ts,
            amount=record.amount,
            description=record.description,
        ):
            duplicate_rows += 1
            continue

        inserted = LedgerRepository.insert_if_absent(
            conn,
            trans_key=record.trans_key,
            account_id=record.account_id,
            ts=record.ts,
            amount=record.amount,
            trans_type=record.trans_type,
            category_id=record.category_id,
            currency=record.currency,
            description=record.description,
            metadata=record.metadata,
        )
        if inserted:
            inserted_rows += 1
        else:
            duplicate_rows += 1

    return inserted_rows, duplicate_rows


def ingest_csv_file(
    conn: Connection,
    dataset_type: DatasetType,
    file_path: Path,
    account_id: UUID,
    source_dataset: str,
    finance_column_mapping: FinanceColumnMapping | None = None,
    max_quarantine_ratio: float = 0.5,
) -> IngestSummary:
    if max_quarantine_ratio < 0.0 or max_quarantine_ratio > 1.0:
        raise ValueError("max_quarantine_ratio must be between 0.0 and 1.0")

    if dataset_type == "finance":
        rows = load_finance_csv(
            file_path=file_path,
            source_dataset=source_dataset,
            column_mapping=finance_column_mapping,
        )
    elif dataset_type == "retail":
        rows = load_retail_csv(file_path=file_path, source_dataset=source_dataset)
    else:
        raise ValueError(f"Unsupported dataset type: {dataset_type}")

    category_name_to_id = _category_lookup(conn)
    canonical_records, quarantined, duplicate_rows_preinsert = _normalize_rows(
        dataset_type=dataset_type,
        rows=rows,
        account_id=account_id,
        category_name_to_id=category_name_to_id,
    )

    total_rows = len(rows)
    quarantined_rows = len(quarantined)
    catastrophic_failure = (
        total_rows > 0
        and (quarantined_rows / total_rows) > max_quarantine_ratio
        and total_rows >= 5
    )

    if catastrophic_failure:
        return IngestSummary(
            dataset_type=dataset_type,
            source_dataset=source_dataset,
            total_rows=total_rows,
            inserted_rows=0,
            duplicate_rows=duplicate_rows_preinsert,
            quarantined_rows=quarantined_rows,
            normalized_rows=len(canonical_records),
            catastrophic_failure=True,
            quarantined=quarantined,
        )

    inserted_rows, duplicate_rows_existing = _insert_canonical_records(conn, canonical_records)

    return IngestSummary(
        dataset_type=dataset_type,
        source_dataset=source_dataset,
        total_rows=total_rows,
        inserted_rows=inserted_rows,
        duplicate_rows=duplicate_rows_preinsert + duplicate_rows_existing,
        quarantined_rows=quarantined_rows,
        normalized_rows=len(canonical_records),
        catastrophic_failure=False,
        quarantined=quarantined,
    )


def ingest_synthetic_transactions(
    conn: Connection,
    account_id: UUID,
    days: int,
    seed: int,
    start_date: date,
    source_dataset: str,
    output_csv: Path | None = None,
) -> IngestSummary:
    category_name_to_id = _category_lookup(conn)
    config = SyntheticGenerationConfig(
        account_id=account_id,
        days=days,
        seed=seed,
        start_date=start_date,
        source_dataset=source_dataset,
    )
    records = generate_synthetic_transactions(
        config=config,
        category_name_to_id=category_name_to_id,
    )

    if output_csv is not None:
        write_synthetic_finance_csv(records, output_csv)

    inserted_rows, duplicate_rows = _insert_canonical_records(conn, records)

    return IngestSummary(
        dataset_type="synthetic",
        source_dataset=source_dataset,
        total_rows=len(records),
        inserted_rows=inserted_rows,
        duplicate_rows=duplicate_rows,
        quarantined_rows=0,
        normalized_rows=len(records),
        catastrophic_failure=False,
        quarantined=[],
    )


def write_quarantine_report(quarantined: list[QuarantineItem], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open(mode="w", encoding="utf-8") as file_obj:
        for item in quarantined:
            file_obj.write(
                json.dumps(
                    {
                        "row_number": item.row_number,
                        "source_dataset": item.source_dataset,
                        "reason": item.reason,
                        "payload": item.payload,
                    },
                    sort_keys=True,
                )
            )
            file_obj.write("\n")


def _parse_finance_mapping(json_text: str | None) -> FinanceColumnMapping | None:
    if json_text is None:
        return None

    mapping_obj = json.loads(json_text)
    if not isinstance(mapping_obj, dict):
        raise ValueError("--finance-column-mapping-json must decode to an object")

    return FinanceColumnMapping(
        date=str(mapping_obj.get("date", "date")),
        description=str(mapping_obj.get("description", "description")),
        amount=str(mapping_obj.get("amount", "amount")),
        category=str(mapping_obj.get("category", "category")),
        institution=str(mapping_obj.get("institution", "institution")),
        account=str(mapping_obj.get("account", "account")),
        currency=str(mapping_obj.get("currency", "currency")),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SentinelBudget transaction ingestion")
    subparsers = parser.add_subparsers(dest="command", required=True)

    csv_parser = subparsers.add_parser("csv", help="Ingest a CSV dataset")
    csv_parser.add_argument("--dataset-type", choices=["finance", "retail"], required=True)
    csv_parser.add_argument("--file", required=True, type=Path)
    csv_parser.add_argument("--account-id", required=True, type=UUID)
    csv_parser.add_argument("--source-dataset", default=None)
    csv_parser.add_argument("--finance-column-mapping-json", default=None)
    csv_parser.add_argument("--max-quarantine-ratio", type=float, default=0.5)
    csv_parser.add_argument("--quarantine-file", type=Path, default=None)

    synthetic_parser = subparsers.add_parser("synthetic", help="Generate and ingest synthetic data")
    synthetic_parser.add_argument("--account-id", required=True, type=UUID)
    synthetic_parser.add_argument("--days", type=int, default=90)
    synthetic_parser.add_argument("--seed", type=int, default=42)
    synthetic_parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 1, 1))
    synthetic_parser.add_argument("--source-dataset", default="synthetic-demo")
    synthetic_parser.add_argument("--output-csv", type=Path, default=None)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    settings = get_settings()
    logger = setup_logging(settings.log_level)

    logger.info(
        "Ingestion command started",
        extra={
            "command": "ingest",
            "subcommand": args.command,
            "account_id": str(args.account_id),
        },
    )

    try:
        with transaction(settings) as conn:
            if args.command == "csv":
                dataset_type = args.dataset_type
                source_dataset = args.source_dataset or Path(args.file).stem
                finance_mapping = _parse_finance_mapping(args.finance_column_mapping_json)

                summary = ingest_csv_file(
                    conn=conn,
                    dataset_type=dataset_type,
                    file_path=args.file,
                    account_id=args.account_id,
                    source_dataset=source_dataset,
                    finance_column_mapping=finance_mapping,
                    max_quarantine_ratio=args.max_quarantine_ratio,
                )

                if args.quarantine_file is not None and summary.quarantined:
                    write_quarantine_report(summary.quarantined, args.quarantine_file)

            else:
                summary = ingest_synthetic_transactions(
                    conn=conn,
                    account_id=args.account_id,
                    days=args.days,
                    seed=args.seed,
                    start_date=args.start_date,
                    source_dataset=args.source_dataset,
                    output_csv=args.output_csv,
                )

        logger.info(
            "Ingestion completed",
            extra={
                "command": "ingest",
                "subcommand": args.command,
                "account_id": str(args.account_id),
                "dataset_type": summary.dataset_type,
                "source_dataset": summary.source_dataset,
                "total_rows": summary.total_rows,
                "normalized_rows": summary.normalized_rows,
                "inserted_rows": summary.inserted_rows,
                "duplicate_rows": summary.duplicate_rows,
                "quarantined_rows": summary.quarantined_rows,
                "catastrophic_failure": summary.catastrophic_failure,
            },
        )

        if summary.catastrophic_failure:
            logger.error(
                "Ingestion failed due to quarantine ratio",
                extra={
                    "command": "ingest",
                    "subcommand": args.command,
                    "account_id": str(args.account_id),
                    "source_dataset": summary.source_dataset,
                    "quarantined_rows": summary.quarantined_rows,
                    "total_rows": summary.total_rows,
                },
            )
            sys.exit(1)
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Ingestion failed",
            extra={
                "command": "ingest",
                "subcommand": args.command,
                "account_id": str(args.account_id),
                "detail": str(exc),
            },
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
