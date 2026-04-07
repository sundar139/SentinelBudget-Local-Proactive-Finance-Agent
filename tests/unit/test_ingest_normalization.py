from __future__ import annotations

from uuid import uuid4

from sentinelbudget.ingest.models import RawInputRow
from sentinelbudget.ingest.normalizers import normalize_category_name
from sentinelbudget.ingest.service import normalize_rows_for_ingest


def test_category_normalization_maps_known_values() -> None:
    assert normalize_category_name("Restaurant") == "Dining Out"
    assert normalize_category_name("Utilities") == "Electric"


def test_category_normalization_returns_none_for_unknown_value() -> None:
    assert normalize_category_name("Unmapped Category") is None


def test_bad_rows_are_quarantined_during_normalization() -> None:
    account_id = uuid4()
    valid = RawInputRow(
        row_number=2,
        source_dataset="unit-finance",
        payload={
            "date": "2026-01-01",
            "description": "Payroll",
            "amount": "4500.00",
            "category": "Salary",
            "institution": "Acme",
            "account": "Checking",
            "currency": "USD",
        },
    )
    invalid = RawInputRow(
        row_number=3,
        source_dataset="unit-finance",
        payload={
            "date": "not-a-date",
            "description": "Broken",
            "amount": "oops",
            "category": "Groceries",
            "institution": "Acme",
            "account": "Checking",
            "currency": "USD",
        },
    )

    canonical, quarantined, duplicate_rows = normalize_rows_for_ingest(
        dataset_type="finance",
        rows=[valid, invalid],
        account_id=account_id,
        category_name_to_id={"salary": 1, "groceries": 2},
    )

    assert len(canonical) == 1
    assert canonical[0].category_id == 1
    assert len(quarantined) == 1
    assert quarantined[0].row_number == 3
    assert duplicate_rows == 0
