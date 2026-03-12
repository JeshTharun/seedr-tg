from __future__ import annotations

from html import escape

from seedr_tg.db.models import JobRecord


def progress_bar(percent: float, width: int = 12) -> str:
    normalized = max(0.0, min(percent, 100.0))
    filled = round((normalized / 100.0) * width)
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def format_job_status(job: JobRecord) -> str:
    name = escape(job.torrent_name or "Pending metadata")
    step = escape(job.current_step or job.phase.value.replace("_", " ").title())
    failure = f"\n<b>Reason:</b> {escape(job.failure_reason)}" if job.failure_reason else ""
    size = f"{job.total_size_bytes / (1024 ** 3):.2f} GB" if job.total_size_bytes else "Unknown"
    return (
        f"<b>Job #{job.id}</b>\n"
        f"<b>Name:</b> {name}\n"
        f"<b>Phase:</b> {escape(job.phase.value)}\n"
        f"<b>Queue:</b> {job.queue_position}\n"
        f"<b>Step:</b> {step}\n"
        f"<b>Size:</b> {size}\n"
        f"<b>Progress:</b> {progress_bar(job.progress_percent)} {job.progress_percent:.1f}%\n"
        f"<b>Uploads:</b> {job.uploaded_file_count}/{job.upload_file_count}{failure}"
    )
