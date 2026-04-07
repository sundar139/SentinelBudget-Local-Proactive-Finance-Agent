from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from sentinelbudget.agent.models import EvidenceBlock, GroundedAnswer
from sentinelbudget.agent.service import main as chat_main


class _FakeChatService:
    def __init__(self) -> None:
        self.calls: list[tuple[UUID, UUID, str]] = []

    def ask(self, user_id: UUID, session_id: UUID, message: str) -> GroundedAnswer:
        self.calls.append((user_id, session_id, message))
        return GroundedAnswer(
            answer_text="Grounded response",
            citations=[
                EvidenceBlock(
                    tool_name="get_category_spend",
                    evidence="Used deterministic category summary",
                    payload={"count": 2},
                )
            ],
            tools_used=["get_category_spend"],
            warnings=[],
            structured_payload={"window": "last_30_days"},
            session_id=session_id,
            user_id=user_id,
            created_at=datetime(2026, 4, 7, tzinfo=UTC),
            meta={"tool_hops": 1},
        )


def test_chat_cli_ask_path_outputs_grounded_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_service = _FakeChatService()

    monkeypatch.setattr(
        "sentinelbudget.agent.service.get_settings",
        lambda: type("_S", (), {"log_level": "INFO"})(),
    )

    def _fake_logger(*_args):
        return type(
            "_L",
            (),
            {
                "info": staticmethod(lambda *a, **k: None),
                "error": staticmethod(lambda *a, **k: None),
            },
        )()

    monkeypatch.setattr("sentinelbudget.agent.service.setup_logging", _fake_logger)
    monkeypatch.setattr("sentinelbudget.agent.service.build_chat_service", lambda: fake_service)
    monkeypatch.setattr(
        "sys.argv",
        [
            "sentinelbudget-chat",
            "ask",
            "--user-id",
            "11111111-1111-1111-1111-111111111111",
            "--session-id",
            "22222222-2222-2222-2222-222222222222",
            "--message",
            "show category spend",
        ],
    )

    chat_main()

    output = capsys.readouterr().out
    payload = json.loads(output)

    assert fake_service.calls
    assert payload["tools_used"] == ["get_category_spend"]
    assert payload["citations"][0]["tool_name"] == "get_category_spend"
