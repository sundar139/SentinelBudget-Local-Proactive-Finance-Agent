from __future__ import annotations

from datetime import date
from uuid import uuid4

from sentinelbudget.ingest.synthetic import (
    SyntheticGenerationConfig,
    generate_synthetic_transactions,
)


def test_synthetic_generation_is_deterministic() -> None:
    account_id = uuid4()
    config = SyntheticGenerationConfig(
        account_id=account_id,
        days=90,
        seed=123,
        start_date=date(2026, 1, 1),
        source_dataset="synthetic-unit",
    )

    first = generate_synthetic_transactions(config)
    second = generate_synthetic_transactions(config)

    assert len(first) == len(second)
    assert [item.trans_key for item in first] == [item.trans_key for item in second]
    assert [item.amount for item in first] == [item.amount for item in second]


def test_synthetic_generation_covers_expected_patterns() -> None:
    config = SyntheticGenerationConfig(
        account_id=uuid4(),
        days=90,
        seed=99,
        start_date=date(2026, 1, 1),
        source_dataset="synthetic-unit",
    )
    records = generate_synthetic_transactions(config)

    descriptions = [item.description or "" for item in records]
    event_types = {str(item.metadata.get("event_type")) for item in records}

    assert len(records) >= 90
    assert any("Payroll" in description for description in descriptions)
    assert any("Rent" in description for description in descriptions)
    assert any("Grocery" in description for description in descriptions)
    assert any("Utilities" in description for description in descriptions)
    assert any("Dining" in description for description in descriptions)
    assert any("Subscription" in description for description in descriptions)
    assert any("Payment" in description for description in descriptions)
    assert "anomaly_spike" in event_types
