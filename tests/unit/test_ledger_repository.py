from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from psycopg.types.json import Jsonb
from sentinelbudget.db.repositories.ledger import LedgerRepository


class _FakeCursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row
        self.last_sql: str | None = None
        self.last_params: tuple[object, ...] | None = None

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.last_sql = sql
        self.last_params = params

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_insert_if_absent_wraps_metadata_with_jsonb() -> None:
    cursor = _FakeCursor(row=(12345,))
    conn = _FakeConnection(cursor)

    inserted = LedgerRepository.insert_if_absent(
        conn=conn,  # type: ignore[arg-type]
        trans_key=12345,
        account_id=UUID("00000000-0000-0000-0000-000000000222"),
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        amount=Decimal("-10.00"),
        trans_type="debit",
        description="Synthetic record",
        metadata={"source": "synthetic"},
    )

    assert inserted is True
    assert cursor.last_params is not None
    metadata_param = cursor.last_params[-1]
    assert isinstance(metadata_param, Jsonb)
    assert metadata_param.obj == {"source": "synthetic"}


def test_insert_if_absent_uses_empty_json_for_none_metadata() -> None:
    cursor = _FakeCursor(row=(12345,))
    conn = _FakeConnection(cursor)

    inserted = LedgerRepository.insert_if_absent(
        conn=conn,  # type: ignore[arg-type]
        trans_key=12345,
        account_id=UUID("00000000-0000-0000-0000-000000000222"),
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        amount=Decimal("-10.00"),
        trans_type="debit",
        description="Synthetic record",
        metadata=None,
    )

    assert inserted is True
    assert cursor.last_params is not None
    metadata_param = cursor.last_params[-1]
    assert isinstance(metadata_param, Jsonb)
    assert metadata_param.obj == {}
