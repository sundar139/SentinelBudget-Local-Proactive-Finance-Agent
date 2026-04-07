from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import ui.state as ui_state
from ui.state import parse_uuid_text


def test_parse_uuid_text_handles_empty_invalid_and_valid() -> None:
    assert parse_uuid_text("") is None
    assert parse_uuid_text("not-a-uuid") is None

    parsed = parse_uuid_text("00000000-0000-0000-0000-000000000001")
    assert parsed == UUID("00000000-0000-0000-0000-000000000001")


def test_sidebar_flash_is_one_shot(monkeypatch) -> None:
    fake_session_state: dict[str, object] = {}
    monkeypatch.setattr(ui_state, "st", SimpleNamespace(session_state=fake_session_state))

    ui_state.ensure_session_state_defaults()
    ui_state.set_sidebar_flash("Active user updated.")

    first = ui_state.pop_sidebar_flash()
    second = ui_state.pop_sidebar_flash()

    assert first == ("success", "Active user updated.")
    assert second is None
