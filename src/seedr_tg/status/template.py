from __future__ import annotations

import shutil
import time
from dataclasses import dataclass
from html import escape

from seedr_tg.db.models import JobRecord

try:
    from psutil import cpu_percent, virtual_memory
except Exception:  # noqa: BLE001
    cpu_percent = None
    virtual_memory = None


@dataclass(slots=True)
class StatusTemplateConfig:
    status_limit: int = 4
    page_step: int = 1
    refresh_interval: int = 10
    download_dir: str = "."
    cancel_cmd: str = "cancel"
    include_system_metrics: bool = False
    bot_start_time: float | None = None


def get_progress_bar_string(percent: float, width: int = 12) -> str:
    normalized = max(0.0, min(percent, 100.0))
    filled = int(normalized // (100 / width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def readable_size(num_bytes: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(max(0.0, num_bytes))
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    return f"{value:.2f}{units[unit_index]}"


def readable_time(seconds: int) -> str:
    total = int(max(0, seconds))
    chunks = [("d", 86400), ("h", 3600), ("m", 60), ("s", 1)]
    out: list[str] = []
    for label, unit in chunks:
        if total >= unit:
            value, total = divmod(total, unit)
            out.append(f"{value}{label}")
    return "".join(out) if out else "0s"


def format_speed_bps(speed_bps: float | None) -> str:
    speed = float(speed_bps or 0.0)
    if speed <= 0:
        return "-"
    units = ["B/s", "KB/s", "MB/s", "GB/s"]
    value = speed
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"


def render_operation_status(
    *,
    title: str,
    fields: list[tuple[str, str]],
    step: str,
    final_name: str | None = None,
    progress_percent: float | None = None,
    progress_detail: str | None = None,
    cfg: StatusTemplateConfig | None = None,
) -> str:
    lines = [f"<b>{escape(title)}</b>"]
    for label, value in fields:
        lines.append(f"<b>{escape(label)}:</b> {escape(value)}")
    lines.append(f"<b>Step:</b> {escape(step)}")
    if final_name:
        lines.append(f"<b>Final:</b> {escape(final_name)}")
    if progress_percent is not None:
        lines.append(
            f"<b>Progress:</b> {get_progress_bar_string(progress_percent)} {progress_percent:.1f}%"
        )
    if progress_detail:
        lines.append(f"<b>Detail:</b> {escape(progress_detail)}")

    if cfg and cfg.include_system_metrics:
        lines.append(_render_metrics_footer(cfg))

    return "\n".join(lines)


def render_job_status(job: JobRecord, cfg: StatusTemplateConfig | None = None) -> str:
    step = job.current_step or job.phase.value.replace("_", " ").title()
    size = f"{job.total_size_bytes / (1024 ** 3):.2f} GB" if job.total_size_bytes else "Unknown"
    lines = [
        f"<b>Job #{job.id}</b>",
        f"<b>Name:</b> {escape(job.torrent_name or 'Pending metadata')}",
        f"<b>Phase:</b> {escape(job.phase.value)}",
        f"<b>Queue:</b> {job.queue_position}",
        f"<b>Step:</b> {escape(step)}",
        f"<b>Size:</b> {size}",
        (
            f"<b>Progress:</b> {get_progress_bar_string(job.progress_percent)} "
            f"{job.progress_percent:.1f}%"
        ),
        f"<b>Download Speed:</b> {format_speed_bps(job.download_speed_bps)}",
        f"<b>Upload Speed:</b> {format_speed_bps(job.upload_speed_bps)}",
        f"<b>Uploads:</b> {job.uploaded_file_count}/{job.upload_file_count}",
    ]
    if job.failure_reason:
        lines.append(f"<b>Reason:</b> {escape(job.failure_reason)}")
    if cfg and cfg.include_system_metrics:
        lines.append(_render_metrics_footer(cfg))
    return "\n".join(lines)


def _render_metrics_footer(cfg: StatusTemplateConfig) -> str:
    try:
        free = readable_size(float(shutil.disk_usage(cfg.download_dir).free))
    except Exception:  # noqa: BLE001
        free = "-"
    uptime = "-"
    if cfg.bot_start_time is not None:
        uptime = readable_time(int(time.time() - cfg.bot_start_time))
    cpu = f"{cpu_percent()}%" if callable(cpu_percent) else "-"
    ram = f"{virtual_memory().percent}%" if callable(virtual_memory) else "-"
    return f"<b>CPU:</b> {cpu} | <b>FREE:</b> {free}\n<b>RAM:</b> {ram} | <b>UPTIME:</b> {uptime}"
