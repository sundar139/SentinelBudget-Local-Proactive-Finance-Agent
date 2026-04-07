from __future__ import annotations

from uuid import UUID

from ui.state import parse_uuid_text


def test_parse_uuid_text_handles_empty_invalid_and_valid() -> None:
    assert parse_uuid_text("") is None
    assert parse_uuid_text("not-a-uuid") is None

    parsed = parse_uuid_text("00000000-0000-0000-0000-000000000001")
    assert parsed == UUID("00000000-0000-0000-0000-000000000001")
