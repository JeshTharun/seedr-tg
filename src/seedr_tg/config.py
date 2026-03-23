from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, computed_field, field_validator
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
    web_api_allowed_origins_raw: str = Field(
        default="http://localhost:3000,http://127.0.0.1:3000,https://seedr-tg.vercel.app",
        alias="WEB_API_ALLOWED_ORIGINS",
    )
    download_root: Path = Path("downloads")
    max_seedr_file_size_bytes: int = 4 * 1024 * 1024 * 1024
    poll_interval_seconds: float = 10.0
    progress_update_interval_seconds: float = 5.0
    queue_concurrency: int = 2
    seedr_active_concurrency: int = 1
    rename_concurrency: int = 2
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
    direct_download_chunk_size_bytes: int = 1024 * 1024
    direct_filename_max_bytes: int = 255
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @field_validator("download_root", mode="before")
    @classmethod
    def _expand_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser()

    @computed_field
    @property
    def web_api_allowed_origins(self) -> tuple[str, ...]:
        raw_value = str(self.web_api_allowed_origins_raw)
        items = [origin.strip() for origin in raw_value.split(",")]
        parsed = tuple(origin for origin in items if origin)
        if parsed:
            return parsed
        return (
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "https://seedr-tg.vercel.app",
        )


def load_settings() -> Settings:
    settings = Settings()
    download_root = Path(settings.download_root)
    download_root.mkdir(parents=True, exist_ok=True)
    return settings
