from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    telegram_bot_token: str = Field(alias="TELEGRAM_BOT_TOKEN")
    telegram_api_id: int = Field(alias="TELEGRAM_API_ID")
    telegram_api_hash: str = Field(alias="TELEGRAM_API_HASH")
    telegram_phone_number: str = Field(alias="TELEGRAM_PHONE_NUMBER")
    telegram_session_name: str = Field(default="seedr_uploader", alias="TELEGRAM_SESSION_NAME")
    telegram_source_chat_id: int = Field(alias="TELEGRAM_SOURCE_CHAT_ID")
    telegram_target_chat_id: int = Field(alias="TELEGRAM_TARGET_CHAT_ID")
    telegram_admin_chat_id: int = Field(alias="TELEGRAM_ADMIN_CHAT_ID")
    seedr_token_json: str | None = Field(default=None, alias="SEEDR_TOKEN_JSON")
    seedr_email: str | None = Field(default=None, alias="SEEDR_EMAIL")
    seedr_password: str | None = Field(default=None, alias="SEEDR_PASSWORD")
    database_path: Path = Field(default=Path("data/seedr_tg.sqlite3"), alias="DATABASE_PATH")
    download_root: Path = Field(default=Path("downloads"), alias="DOWNLOAD_ROOT")
    max_seedr_file_size_bytes: int = Field(
        default=4 * 1024 * 1024 * 1024,
        alias="MAX_SEEDR_FILE_SIZE_BYTES",
    )
    poll_interval_seconds: float = Field(default=10.0, alias="POLL_INTERVAL_SECONDS")
    progress_update_interval_seconds: float = Field(
        default=5.0,
        alias="PROGRESS_UPDATE_INTERVAL_SECONDS",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        alias="LOG_LEVEL",
    )

    @field_validator("database_path", "download_root", mode="before")
    @classmethod
    def _expand_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser()

    @field_validator("seedr_password")
    @classmethod
    def _validate_seedr_password(cls, value: str | None, info: ValidationInfo) -> str | None:
        email = info.data.get("seedr_email")
        if email and not value:
            raise ValueError("SEEDR_PASSWORD is required when SEEDR_EMAIL is set")
        return value

    @model_validator(mode="after")
    def _validate_seedr_auth(self) -> Settings:
        if not self.seedr_token_json and not (self.seedr_email and self.seedr_password):
            raise ValueError(
                "Provide SEEDR_TOKEN_JSON or both SEEDR_EMAIL and SEEDR_PASSWORD for Seedr auth"
            )
        return self


def load_settings() -> Settings:
    settings = Settings()
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    settings.download_root.mkdir(parents=True, exist_ok=True)
    return settings
