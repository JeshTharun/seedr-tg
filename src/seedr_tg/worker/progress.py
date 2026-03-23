from __future__ import annotations

from seedr_tg.db.models import JobRecord
from seedr_tg.status.template import format_speed_bps, get_progress_bar_string, render_job_status


def progress_bar(percent: float, width: int = 12) -> str:
    return get_progress_bar_string(percent, width=width)


def format_job_status(job: JobRecord) -> str:
    return render_job_status(job)


__all__ = ["format_job_status", "format_speed_bps", "progress_bar"]
