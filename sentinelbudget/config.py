from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import AnyHttpUrl, Field, SecretStr, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    sentinel_env: Literal["development", "test", "production"] = Field(
        default="development",
        validation_alias="SENTINEL_ENV",
    )
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    postgres_host: str = Field(validation_alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, validation_alias="POSTGRES_PORT")
    postgres_db: str = Field(validation_alias="POSTGRES_DB")
    postgres_user: str = Field(validation_alias="POSTGRES_USER")
    postgres_password: SecretStr = Field(validation_alias="POSTGRES_PASSWORD")
    postgres_sslmode: str = Field(default="prefer", validation_alias="POSTGRES_SSLMODE")
    postgres_connect_timeout: int = Field(default=5, validation_alias="POSTGRES_CONNECT_TIMEOUT")

    pgvector_extension_name: str = Field(
        default="vector",
        validation_alias="PGVECTOR_EXTENSION_NAME",
    )

    ollama_base_url: AnyHttpUrl = Field(validation_alias="OLLAMA_BASE_URL")
    ollama_chat_model: str = Field(
        default="llama3.1:8b-instruct",
        validation_alias="OLLAMA_CHAT_MODEL",
    )
    ollama_chat_timeout_seconds: int = Field(
        default=30,
        validation_alias="OLLAMA_CHAT_TIMEOUT_SECONDS",
    )
    ollama_chat_temperature: float = Field(
        default=0.0,
        validation_alias="OLLAMA_CHAT_TEMPERATURE",
    )
    agent_max_tool_hops: int = Field(default=4, validation_alias="AGENT_MAX_TOOL_HOPS")
    agent_history_limit: int = Field(default=40, validation_alias="AGENT_HISTORY_LIMIT")

    review_daily_hour_utc: int = Field(default=7, validation_alias="REVIEW_DAILY_HOUR_UTC")
    review_weekly_day_utc: int = Field(default=0, validation_alias="REVIEW_WEEKLY_DAY_UTC")
    review_weekly_hour_utc: int = Field(default=8, validation_alias="REVIEW_WEEKLY_HOUR_UTC")
    review_daemon_poll_seconds: int = Field(
        default=30,
        validation_alias="REVIEW_DAEMON_POLL_SECONDS",
    )
    review_memory_top_k: int = Field(default=5, validation_alias="REVIEW_MEMORY_TOP_K")

    memory_embedding_model: str = Field(
        default="nomic-embed-text",
        validation_alias="MEMORY_EMBEDDING_MODEL",
    )
    memory_embedding_dim: int = Field(default=768, validation_alias="MEMORY_EMBEDDING_DIM")
    memory_embedding_timeout_seconds: int = Field(
        default=30,
        validation_alias="MEMORY_EMBEDDING_TIMEOUT_SECONDS",
    )
    memory_default_top_k: int = Field(default=5, validation_alias="MEMORY_DEFAULT_TOP_K")

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
        normalized = value.upper()
        if normalized not in allowed:
            raise ValueError(f"LOG_LEVEL must be one of: {', '.join(sorted(allowed))}")
        return normalized

    @field_validator("postgres_port")
    @classmethod
    def validate_port(cls, value: int) -> int:
        if value < 1 or value > 65535:
            raise ValueError("POSTGRES_PORT must be between 1 and 65535")
        return value

    @field_validator("postgres_connect_timeout")
    @classmethod
    def validate_connect_timeout(cls, value: int) -> int:
        if value < 1:
            raise ValueError("POSTGRES_CONNECT_TIMEOUT must be a positive integer")
        return value

    @field_validator("memory_embedding_model")
    @classmethod
    def validate_memory_embedding_model(cls, value: str) -> str:
        if value.strip() == "":
            raise ValueError("MEMORY_EMBEDDING_MODEL cannot be empty")
        return value.strip()

    @field_validator("ollama_chat_model")
    @classmethod
    def validate_ollama_chat_model(cls, value: str) -> str:
        if value.strip() == "":
            raise ValueError("OLLAMA_CHAT_MODEL cannot be empty")
        return value.strip()

    @field_validator("ollama_chat_temperature")
    @classmethod
    def validate_ollama_chat_temperature(cls, value: float) -> float:
        if value < 0.0 or value > 1.0:
            raise ValueError("OLLAMA_CHAT_TEMPERATURE must be between 0.0 and 1.0")
        return value

    @field_validator("review_daily_hour_utc", "review_weekly_hour_utc")
    @classmethod
    def validate_review_hours(cls, value: int) -> int:
        if value < 0 or value > 23:
            raise ValueError("Review hour settings must be between 0 and 23")
        return value

    @field_validator("review_weekly_day_utc")
    @classmethod
    def validate_review_weekday(cls, value: int) -> int:
        if value < 0 or value > 6:
            raise ValueError("REVIEW_WEEKLY_DAY_UTC must be between 0 (Mon) and 6 (Sun)")
        return value

    @field_validator("memory_embedding_dim")
    @classmethod
    def validate_memory_embedding_dim(cls, value: int) -> int:
        if value < 1:
            raise ValueError("MEMORY_EMBEDDING_DIM must be a positive integer")
        return value

    @field_validator(
        "ollama_chat_timeout_seconds",
        "agent_max_tool_hops",
        "agent_history_limit",
        "review_daemon_poll_seconds",
        "review_memory_top_k",
        "memory_embedding_timeout_seconds",
        "memory_default_top_k",
    )
    @classmethod
    def validate_positive_memory_numbers(cls, value: int) -> int:
        if value < 1:
            raise ValueError("Memory configuration values must be positive integers")
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached, validated application settings."""

    # pydantic-settings resolves required fields from environment at runtime.
    return Settings()  # type: ignore[call-arg]


def validate_settings() -> Settings:
    """Validate settings and surface a consistent error type to callers."""

    try:
        return get_settings()
    except ValidationError:
        raise
