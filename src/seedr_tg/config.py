from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    telegram_bot_token: str = Field(alias="TELEGRAM_BOT_TOKEN")
    telegram_api_id: int = Field(alias="TELEGRAM_API_ID")
    telegram_api_hash: str = Field(alias="TELEGRAM_API_HASH")
    telegram_source_chat_id: int = Field(alias="TELEGRAM_SOURCE_CHAT_ID")
    telegram_target_chat_id: int = Field(alias="TELEGRAM_TARGET_CHAT_ID")
    telegram_admin_chat_id: int = Field(alias="TELEGRAM_ADMIN_CHAT_ID")
    telegram_user_session_string: str | None = Field(
        default=None,
        alias="TELEGRAM_USER_SESSION_STRING",
    )
    seedr_token_json: str | None = Field(default=None, alias="SEEDR_TOKEN_JSON")
    mongodb_uri: str = Field(default="mongodb://localhost:27017", alias="MONGODB_URI")
    mongodb_database: str = Field(default="seedr_tg", alias="MONGODB_DATABASE")
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
    download_concurrency: int = Field(default=4, alias="DOWNLOAD_CONCURRENCY")
    upload_concurrency: int = Field(default=2, alias="UPLOAD_CONCURRENCY")
    upload_part_size_kb: int = Field(default=512, alias="UPLOAD_PART_SIZE_KB")
    use_uvloop: bool = Field(default=True, alias="USE_UVLOOP")
    download_connect_timeout_seconds: float = Field(
        default=10.0,
        alias="DOWNLOAD_CONNECT_TIMEOUT_SECONDS",
    )
    download_read_timeout_seconds: float = Field(
        default=120.0,
        alias="DOWNLOAD_READ_TIMEOUT_SECONDS",
    )
    download_write_timeout_seconds: float = Field(
        default=30.0,
        alias="DOWNLOAD_WRITE_TIMEOUT_SECONDS",
    )
    download_pool_timeout_seconds: float = Field(
        default=10.0,
        alias="DOWNLOAD_POOL_TIMEOUT_SECONDS",
    )
    download_max_retries: int = Field(default=4, alias="DOWNLOAD_MAX_RETRIES")
    download_retry_base_delay_seconds: float = Field(
        default=1.0,
        alias="DOWNLOAD_RETRY_BASE_DELAY_SECONDS",
    )
    download_retry_max_delay_seconds: float = Field(
        default=20.0,
        alias="DOWNLOAD_RETRY_MAX_DELAY_SECONDS",
    )
    upload_max_retries: int = Field(default=4, alias="UPLOAD_MAX_RETRIES")
    upload_retry_base_delay_seconds: float = Field(
        default=1.0,
        alias="UPLOAD_RETRY_BASE_DELAY_SECONDS",
    )
    upload_retry_max_delay_seconds: float = Field(
        default=30.0,
        alias="UPLOAD_RETRY_MAX_DELAY_SECONDS",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        alias="LOG_LEVEL",
    )

    @field_validator("download_root", mode="before")
    @classmethod
    def _expand_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser()


def load_settings() -> Settings:
    settings = Settings()
    download_root = Path(settings.download_root)
    download_root.mkdir(parents=True, exist_ok=True)
    return settings
