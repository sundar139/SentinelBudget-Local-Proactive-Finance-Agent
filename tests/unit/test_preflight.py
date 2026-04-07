from __future__ import annotations

from types import SimpleNamespace

from sentinelbudget.preflight import (
    PreflightCheck,
    _build_summary,
    _check_ollama,
    run_preflight,
)


def test_build_summary_counts_required_failures_and_warnings() -> None:
    summary = _build_summary(
        [
            PreflightCheck("config", "pass", "ok", required=True),
            PreflightCheck("db", "fail", "down", required=True),
            PreflightCheck("ollama", "warn", "optional", required=False),
        ]
    )

    assert summary.hard_failures == 1
    assert summary.warnings == 1


def test_run_preflight_short_circuits_on_config_failure(monkeypatch) -> None:
    fail_check = PreflightCheck(
        name="config_validity",
        status="fail",
        detail="bad config",
        required=True,
    )

    monkeypatch.setattr("sentinelbudget.preflight._check_config", lambda: (None, fail_check))
    monkeypatch.setattr("sentinelbudget.preflight._log_summary", lambda logger, summary: None)

    summary, exit_code = run_preflight()

    assert exit_code == 1
    assert len(summary.checks) == 1
    assert summary.checks[0].name == "config_validity"


def test_run_preflight_allows_optional_warnings(monkeypatch) -> None:
    fake_settings = SimpleNamespace(
        sentinel_env="development",
        log_level="INFO",
        ollama_chat_model="llama3.1:8b-instruct",
        memory_embedding_model="nomic-embed-text",
    )

    monkeypatch.setattr(
        "sentinelbudget.preflight._check_config",
        lambda: (
            fake_settings,
            PreflightCheck("config_validity", "pass", "ok", required=True),
        ),
    )
    monkeypatch.setattr(
        "sentinelbudget.preflight._check_db_connectivity",
        lambda settings: PreflightCheck("db_connectivity", "pass", "ok", required=True),
    )
    monkeypatch.setattr(
        "sentinelbudget.preflight._check_pgvector",
        lambda settings: PreflightCheck("pgvector_readiness", "pass", "ok", required=True),
    )
    monkeypatch.setattr(
        "sentinelbudget.preflight._check_schema_and_migrations",
        lambda settings: PreflightCheck("schema_migrations", "pass", "ok", required=True),
    )
    monkeypatch.setattr(
        "sentinelbudget.preflight._check_ollama",
        lambda settings: [PreflightCheck("ollama_reachability", "warn", "unreachable", False)],
    )
    monkeypatch.setattr(
        "sentinelbudget.preflight._check_streamlit_import",
        lambda: PreflightCheck("streamlit_import", "pass", "ok", required=False),
    )
    monkeypatch.setattr(
        "sentinelbudget.preflight._check_demo_seed_data",
        lambda settings: PreflightCheck("demo_seed_data", "warn", "no rows", required=False),
    )
    monkeypatch.setattr("sentinelbudget.preflight._log_summary", lambda logger, summary: None)

    summary, exit_code = run_preflight()

    assert exit_code == 0
    assert summary.hard_failures == 0
    assert summary.warnings == 2


def test_check_ollama_returns_warning_triplet_when_unreachable(monkeypatch) -> None:
    fake_settings = SimpleNamespace(
        ollama_chat_model="llama3.1:8b-instruct",
        memory_embedding_model="nomic-embed-text",
    )
    monkeypatch.setattr(
        "sentinelbudget.preflight._fetch_ollama_model_names",
        lambda settings: (None, "connection refused"),
    )

    checks = _check_ollama(fake_settings)

    assert len(checks) == 3
    assert all(item.status == "warn" for item in checks)


def test_check_ollama_accepts_latest_tag_alias(monkeypatch) -> None:
    fake_settings = SimpleNamespace(
        ollama_chat_model="llama3.1:8b-instruct",
        memory_embedding_model="nomic-embed-text",
    )
    monkeypatch.setattr(
        "sentinelbudget.preflight._fetch_ollama_model_names",
        lambda settings: (
            {
                "llama3.1:8b-instruct:latest",
                "nomic-embed-text:latest",
            },
            "ok",
        ),
    )

    checks = _check_ollama(fake_settings)

    by_name = {check.name: check for check in checks}
    assert by_name["ollama_reachability"].status == "pass"
    assert by_name["ollama_chat_model"].status == "pass"
    assert by_name["ollama_embedding_model"].status == "pass"
