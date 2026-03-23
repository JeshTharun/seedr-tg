from __future__ import annotations

from seedr_tg.db.models import JobRecord
from seedr_tg.status.template import (
    BotStatusSnapshot,
    format_speed_bps,
    get_progress_bar_string,
    render_job_status,
)


def progress_bar(percent: float, width: int = 12) -> str:
    return get_progress_bar_string(percent, width=width)


def format_job_status(
    job: JobRecord,
    *,
    bot_stats: BotStatusSnapshot | None = None,
    cancel_cmd: str = "cancel",
) -> str:
    return render_job_status(job, bot_stats=bot_stats, cancel_cmd=cancel_cmd)


__all__ = ["format_job_status", "format_speed_bps", "progress_bar"]
