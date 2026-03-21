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
    mongodb_database: str = "seedr_tg"
    web_api_allowed_origins: tuple[str, ...] = (
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    )
    download_root: Path = Path("downloads")
    max_seedr_file_size_bytes: int = 4 * 1024 * 1024 * 1024
    poll_interval_seconds: float = 10.0
    progress_update_interval_seconds: float = 5.0
    download_concurrency: int = 4
    upload_concurrency: int = 2
    upload_part_size_kb: int = 512
    upload_governor_enabled: bool = True
    upload_governor_min_concurrency: int = 1
    upload_governor_scale_up_after_stable_files: int = 6
    use_uvloop: bool = True
    use_aria2_downloads: bool = True
    aria2_binary: str = "aria2c"
    aria2_split: int = 8
    aria2_max_connection_per_server: int = 4
    aria2_min_split_size: str = "8M"
    aria2_file_allocation: Literal["none", "prealloc", "trunc", "falloc"] = "none"
    download_connect_timeout_seconds: float = 10.0
    download_read_timeout_seconds: float = 120.0
    download_write_timeout_seconds: float = 30.0
    download_pool_timeout_seconds: float = 10.0
    download_max_retries: int = 4
    download_retry_base_delay_seconds: float = 1.0
    download_retry_max_delay_seconds: float = 20.0
    upload_max_retries: int = 4
    upload_retry_base_delay_seconds: float = 1.0
    upload_retry_max_delay_seconds: float = 30.0
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @field_validator("download_root", mode="before")
    @classmethod
    def _expand_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser()

    @field_validator("web_api_allowed_origins", mode="before")
    @classmethod
    def _parse_web_api_allowed_origins(
        cls, value: str | tuple[str, ...] | list[str] | None
    ) -> tuple[str, ...]:
        if value is None:
            return (
                "http://localhost:3000",
                "http://127.0.0.1:3000",
            )
        if isinstance(value, str):
            items = [origin.strip() for origin in value.split(",") if origin.strip()]
            return tuple(items)
        return tuple(str(origin).strip() for origin in value if str(origin).strip())


def load_settings() -> Settings:
    settings = Settings()
    download_root = Path(settings.download_root)
    download_root.mkdir(parents=True, exist_ok=True)
    return settings
