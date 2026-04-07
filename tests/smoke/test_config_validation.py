import pytest
from pydantic import ValidationError
from sentinelbudget.config import Settings

_REQUIRED_ENV_VARS = [
    "POSTGRES_HOST",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "OLLAMA_BASE_URL",
]


def test_settings_fail_when_required_env_vars_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in _REQUIRED_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_settings_load_when_required_env_vars_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_DB", "sentinelbudget")
    monkeypatch.setenv("POSTGRES_USER", "sentinelbudget")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://localhost:11434")

    settings = Settings(_env_file=None)

    assert settings.postgres_host == "localhost"
    assert settings.postgres_db == "sentinelbudget"
    assert settings.postgres_user == "sentinelbudget"
    assert settings.postgres_password.get_secret_value() == "secret"
