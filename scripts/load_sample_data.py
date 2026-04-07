from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from uuid import UUID

from sentinelbudget.config import get_settings
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.ingest.service import ingest_csv_file, write_quarantine_report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load deterministic sample CSV data into SentinelBudget"
    )
    parser.add_argument("--dataset-type", choices=["finance", "retail"], default="finance")
    parser.add_argument(
        "--file",
        type=Path,
        default=Path("tests/fixtures/finance_sample.csv"),
    )
    parser.add_argument("--account-id", type=UUID, required=True)
    parser.add_argument("--source-dataset", default="sample-fixture")
    parser.add_argument("--max-quarantine-ratio", type=float, default=0.8)
    parser.add_argument("--quarantine-file", type=Path, default=Path("artifacts/quarantine.jsonl"))
    args = parser.parse_args()

    settings = get_settings()
    with transaction(settings) as conn:
        summary = ingest_csv_file(
            conn=conn,
            dataset_type=args.dataset_type,
            file_path=args.file,
            account_id=args.account_id,
            source_dataset=args.source_dataset,
            max_quarantine_ratio=args.max_quarantine_ratio,
        )

    if summary.quarantined:
        write_quarantine_report(summary.quarantined, args.quarantine_file)

    print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))

    if summary.catastrophic_failure:
        sys.exit(1)


if __name__ == "__main__":
    main()
