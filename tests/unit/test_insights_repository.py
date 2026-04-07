from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from psycopg.types.json import Jsonb
from sentinelbudget.db.repositories.insights import InsightRepository


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


def _insight_row(details: dict[str, object]) -> tuple[object, ...]:
    return (
        1,
        UUID("00000000-0000-0000-0000-000000000111"),
        "test_kind",
        "title",
        "body",
        "medium",
        details,
        "fp-1",
        datetime(2026, 4, 7, tzinfo=UTC),
        False,
    )


def test_create_wraps_details_with_jsonb() -> None:
    cursor = _FakeCursor(row=_insight_row({"source": "review"}))
    conn = _FakeConnection(cursor)

    created = InsightRepository.create(
        conn=conn,  # type: ignore[arg-type]
        user_id=UUID("00000000-0000-0000-0000-000000000111"),
        kind="test_kind",
        title="title",
        body="body",
        severity="medium",
        details={"source": "review"},
        fingerprint="fp-1",
    )

    assert created.id == 1
    assert cursor.last_params is not None
    details_param = cursor.last_params[5]
    assert isinstance(details_param, Jsonb)
    assert details_param.obj == {"source": "review"}


def test_create_uses_empty_json_object_when_details_is_none() -> None:
    cursor = _FakeCursor(row=_insight_row({}))
    conn = _FakeConnection(cursor)

    created = InsightRepository.create(
        conn=conn,  # type: ignore[arg-type]
        user_id=UUID("00000000-0000-0000-0000-000000000111"),
        kind="test_kind",
        title="title",
        body="body",
        severity="medium",
        details=None,
        fingerprint="fp-1",
    )

    assert created.id == 1
    assert cursor.last_params is not None
    details_param = cursor.last_params[5]
    assert isinstance(details_param, Jsonb)
    assert details_param.obj == {}
