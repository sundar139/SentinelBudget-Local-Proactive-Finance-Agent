from __future__ import annotations

from pathlib import Path

import pytest
from sentinelbudget.ingest.loaders import (
    FinanceColumnMapping,
    load_finance_csv,
    load_retail_csv,
)

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_load_finance_csv_default_columns() -> None:
    file_path = FIXTURES_DIR / "finance_sample.csv"

    rows = load_finance_csv(file_path=file_path, source_dataset="finance-sample")

    assert len(rows) == 4
    assert rows[0].payload["date"] == "2026-01-01"
    assert rows[1].payload["description"] == "Monthly Rent"
    assert rows[2].payload["amount"] == "-85.42"


def test_load_finance_csv_custom_mapping() -> None:
    file_path = FIXTURES_DIR / "finance_custom_columns.csv"
    mapping = FinanceColumnMapping(
        date="posted_date",
        description="details",
        amount="amount_usd",
        category="raw_category",
        institution="institution_name",
        account="acct_label",
        currency="currency_code",
    )

    rows = load_finance_csv(
        file_path=file_path,
        source_dataset="finance-custom",
        column_mapping=mapping,
    )

    assert len(rows) == 2
    assert rows[0].payload["description"] == "Payroll Deposit"
    assert rows[1].payload["category"] == "Restaurant"


def test_load_retail_csv() -> None:
    file_path = FIXTURES_DIR / "retail_sample.csv"

    rows = load_retail_csv(file_path=file_path, source_dataset="retail-sample")

    assert len(rows) == 3
    assert rows[0].payload["merchant"] == "City Grocer"
    assert rows[0].payload["discount"] == "10.00"
    assert rows[2].payload["category"] == "Utilities"


def test_load_finance_csv_raises_on_missing_columns(tmp_path: Path) -> None:
    file_path = tmp_path / "finance_missing.csv"
    file_path.write_text(
        "date,description,amount,category,institution,account\n"
        "2026-01-01,Payroll,4500.00,Salary,Acme,Checking\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing required finance CSV columns"):
        load_finance_csv(file_path=file_path, source_dataset="missing-columns")


def test_load_retail_csv_raises_when_merchant_and_product_missing(tmp_path: Path) -> None:
    file_path = tmp_path / "retail_missing.csv"
    file_path.write_text(
        "transaction_date,total_amount,discount,category,currency,notes\n"
        "2026-01-01,10.00,0.00,Groceries,USD,No merchant\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must include either merchant or product"):
        load_retail_csv(file_path=file_path, source_dataset="missing-merchant")


def test_load_finance_csv_raises_on_duplicate_mapping_values() -> None:
    mapping = FinanceColumnMapping(
        date="posted_date",
        description="posted_date",
    )

    with pytest.raises(ValueError, match="mapping values must be unique"):
        load_finance_csv(
            file_path=FIXTURES_DIR / "finance_custom_columns.csv",
            source_dataset="duplicate-mapping",
            column_mapping=mapping,
        )
