from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


class JobPhase(StrEnum):
    QUEUED = "queued"
    VALIDATING = "validating"
    WAITING_METADATA = "waiting_metadata"
    DOWNLOADING_SEEDR = "downloading_seedr"
    DOWNLOADING_LOCAL = "downloading_local"
    UPLOADING_TELEGRAM = "uploading_telegram"
    CLEANING = "cleaning"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class UploadMediaType(StrEnum):
    MEDIA = "media"
    DOCUMENT = "document"


class CaptionParseMode(StrEnum):
    HTML = "html"
    MARKDOWN_V2 = "markdownv2"


FINAL_PHASES = {JobPhase.COMPLETED, JobPhase.FAILED, JobPhase.CANCELED}


@dataclass(slots=True)
class JobRecord:
    id: int
    magnet_link: str
    torrent_file_path: str | None
    source_chat_id: int
    source_message_id: int
    created_by_user_id: int | None
    created_by_username: str | None
    created_by_display_name: str | None
    admin_message_id: int | None
    target_chat_id: int
    phase: JobPhase
    queue_position: int
    torrent_name: str | None
    total_size_bytes: int | None
    seedr_torrent_id: int | None
    seedr_folder_id: int | None
    seedr_folder_name: str | None
    progress_percent: float
    download_speed_bps: float
    upload_speed_bps: float
    current_step: str | None
    local_path: str | None
    upload_file_count: int
    uploaded_file_count: int
    failure_reason: str | None
    last_error: str | None
    cancel_requested: bool
    created_at: str
    updated_at: str


@dataclass(slots=True)
class SeedrDeviceCodeRecord:
    device_code: str
    user_code: str
    verification_url: str
    expires_in: int | None
    created_at: str


@dataclass(slots=True)
class TelegramLoginState:
    phone_number: str
    phone_code_hash: str
    session_string: str
    password_required: bool
    created_at: str
    updated_at: str


@dataclass(slots=True)
class TelegramUserSession:
    session_string: str
    phone_number: str | None
    user_id: int | None
    username: str | None
    display_name: str | None
    created_at: str
    updated_at: str


@dataclass(slots=True)
class UploadSettings:
    media_type: UploadMediaType
    caption_template: str | None
    caption_parse_mode: CaptionParseMode
    thumbnail_file_id: str | None
    thumbnail_base64: str | None
    created_at: str
    updated_at: str


@dataclass(slots=True)
class UserSettings:
    user_id: int
    caption_template: str | None
    thumbnail_file_id: str | None
    thumbnail_base64: str | None
    created_at: str
    updated_at: str
