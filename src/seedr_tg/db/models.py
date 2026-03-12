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


FINAL_PHASES = {JobPhase.COMPLETED, JobPhase.FAILED, JobPhase.CANCELED}


@dataclass(slots=True)
class JobRecord:
    id: int
    magnet_link: str
    source_chat_id: int
    source_message_id: int
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
    current_step: str | None
    local_path: str | None
    upload_file_count: int
    uploaded_file_count: int
    failure_reason: str | None
    last_error: str | None
    cancel_requested: bool
    created_at: str
    updated_at: str
