from __future__ import annotations

import shutil
import time
import logging
from dataclasses import dataclass
from datetime import datetime
from html import escape
from pathlib import Path

from seedr_tg.db.models import JobRecord
from seedr_tg.status.unified import ActiveTaskSnapshot

try:
    from psutil import cpu_percent, virtual_memory
except ImportError:
    cpu_percent = None
    virtual_memory = None


LOGGER = logging.getLogger(__name__)
_metrics_state: dict[str, object] = {
    "cpu_sample_prev": None,
    "fallback_warning_logged": False,
}


@dataclass(slots=True)
class StatusTemplateConfig:
    status_limit: int = 4
    page_step: int = 1
    refresh_interval: int = 10
    download_dir: str = "."
    cancel_cmd: str = "cancel"
    include_system_metrics: bool = False
    bot_start_time: float | None = None


@dataclass(slots=True)
class BotStatusSnapshot:
    tasks_count: int
    cpu_percent: float | None
    ram_percent: float | None
    free_bytes: int | None
    free_percent: float | None
    uptime_seconds: int
    download_bps: float
    upload_bps: float


def get_progress_bar_string(percent: float, width: int = 12) -> str:
    normalized = max(0.0, min(percent, 100.0))
    unit = 100.0 / float(max(1, width))
    filled = int(normalized // unit)
    remainder = normalized - (filled * unit)
    has_partial = remainder >= (unit * 0.35) and filled < width
    partial_cells = 1 if has_partial else 0
    empty = max(0, width - filled - partial_cells)
    return "[" + ("■" * filled) + ("▤" * partial_cells) + ("□" * empty) + "]"


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
    cancel_command: str | None = None,
    bot_stats: BotStatusSnapshot | None = None,
    cfg: StatusTemplateConfig | None = None,
) -> str:
    lines: list[str] = [escape(title)]
    if progress_percent is not None:
        lines.append(f"┃ {get_progress_bar_string(progress_percent)} {progress_percent:.2f}%")
    for label, value in fields:
        lines.append(f"┠ {escape(label)}: {escape(value)}")
    lines.append(f"┠ Status: {escape(step)}")
    if final_name:
        lines.append(f"┠ Final: {escape(final_name)}")
    if progress_detail:
        for detail_line in progress_detail.splitlines():
            lines.append(f"┠ {escape(detail_line)}")
    if cancel_command:
        lines.append(f"┖ {escape(cancel_command)}")

    snapshot = bot_stats
    if snapshot is None and cfg and cfg.include_system_metrics:
        snapshot = collect_bot_stats(
            download_dir=cfg.download_dir,
            bot_start_time=cfg.bot_start_time,
            tasks_count=0,
            download_bps=0.0,
            upload_bps=0.0,
        )
    if snapshot is not None:
        lines.extend(_render_bot_stats_block(snapshot))

    return "\n".join(lines)


def render_compact_task_status(
    *,
    title: str,
    progress_percent: float,
    status_text: str,
    speed_bps: float | None,
    eta_seconds: int | None,
    elapsed_seconds: int,
    cancel_command: str | None = None,
) -> str:
    eta_text = readable_time(eta_seconds) if eta_seconds is not None else "-"
    lines = [
        escape(title),
        f"┃ {get_progress_bar_string(progress_percent)} {progress_percent:.2f}%",
        f"┠ Status: {escape(status_text)} | ETA: {eta_text}",
        (
            f"┠ Speed: {format_speed_bps(speed_bps)} | "
            f"Elapsed: {readable_time(elapsed_seconds)}"
        ),
    ]
    lines.append(f"┖ {escape(cancel_command)}" if cancel_command else "┖")
    return "\n".join(lines)


def render_active_task_status(task: ActiveTaskSnapshot) -> str:
    return render_compact_task_status(
        title=task.title,
        progress_percent=task.progress_percent,
        status_text=task.status_text,
        speed_bps=task.speed_bps,
        eta_seconds=task.eta_seconds,
        elapsed_seconds=task.elapsed_seconds,
        cancel_command=task.cancel_command,
    )


def render_job_status(
    job: JobRecord,
    cfg: StatusTemplateConfig | None = None,
    *,
    bot_stats: BotStatusSnapshot | None = None,
    cancel_cmd: str = "cancel",
) -> str:
    step = job.current_step or job.phase.value.replace("_", " ").title()
    total_size = float(job.total_size_bytes or 0)
    processed_size = (total_size * max(0.0, min(100.0, job.progress_percent)) / 100.0)
    elapsed_seconds = _elapsed_from_iso(job.created_at)
    active_speed = (
        float(job.download_speed_bps or 0.0)
        if job.phase in {job.phase.DOWNLOADING_SEEDR, job.phase.DOWNLOADING_LOCAL}
        else float(job.upload_speed_bps or 0.0)
    )
    eta_seconds: int | None = None
    if total_size > 0 and active_speed > 0:
        eta_seconds = int(max(0.0, (total_size - processed_size) / active_speed))

    lines = [
        render_compact_task_status(
            title=job.torrent_name or f"Job #{job.id}",
            progress_percent=job.progress_percent,
            status_text=step,
            speed_bps=active_speed,
            eta_seconds=eta_seconds,
            elapsed_seconds=elapsed_seconds,
            cancel_command=f"/{cancel_cmd} {job.id}",
        )
    ]
    if job.failure_reason:
        lines.append(f"┠ Reason: {escape(job.failure_reason)}")

    snapshot = bot_stats
    if snapshot is None and cfg and cfg.include_system_metrics:
        snapshot = collect_bot_stats(
            download_dir=cfg.download_dir,
            bot_start_time=cfg.bot_start_time,
            tasks_count=1,
            download_bps=float(job.download_speed_bps or 0.0),
            upload_bps=float(job.upload_speed_bps or 0.0),
        )
    if snapshot is not None:
        lines.extend(_render_bot_stats_block(snapshot))

    return "\n".join(lines)


def collect_bot_stats(
    *,
    download_dir: str | Path,
    bot_start_time: float | None,
    tasks_count: int,
    download_bps: float,
    upload_bps: float,
) -> BotStatusSnapshot:
    cpu: float | None = None
    if callable(cpu_percent):
        cpu = float(cpu_percent())
    else:
        cpu = _cpu_percent_from_proc()

    ram: float | None = None
    if callable(virtual_memory):
        ram = float(virtual_memory().percent)
    else:
        ram = _ram_percent_from_proc_meminfo()

    warning_logged = bool(_metrics_state.get("fallback_warning_logged"))
    if cpu is None and ram is None and not warning_logged:
        LOGGER.warning(
            "System metrics unavailable: psutil not installed and /proc fallback unavailable"
        )
        _metrics_state["fallback_warning_logged"] = True

    free_bytes: int | None = None
    free_percent: float | None = None
    try:
        usage = shutil.disk_usage(download_dir)
        free_bytes = int(usage.free)
        total = int(usage.total)
        if total > 0:
            free_percent = (float(free_bytes) / float(total)) * 100.0
    except OSError:
        pass

    uptime_seconds = 0
    if bot_start_time is not None:
        uptime_seconds = int(max(0.0, time.time() - bot_start_time))

    return BotStatusSnapshot(
        tasks_count=int(max(0, tasks_count)),
        cpu_percent=cpu,
        ram_percent=ram,
        free_bytes=free_bytes,
        free_percent=free_percent,
        uptime_seconds=uptime_seconds,
        download_bps=float(max(0.0, download_bps)),
        upload_bps=float(max(0.0, upload_bps)),
    )


def _cpu_percent_from_proc() -> float | None:
    try:
        stat_path = Path("/proc/stat")
        if not stat_path.exists():
            return None
        first_line = stat_path.read_text(encoding="utf-8").splitlines()[0]
        parts = first_line.split()
        if len(parts) < 5 or parts[0] != "cpu":
            return None
        values = [int(v) for v in parts[1:]]
        total = sum(values)
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        prev = _metrics_state.get("cpu_sample_prev")
        if prev is not None and not isinstance(prev, tuple):
            prev = None
        _metrics_state["cpu_sample_prev"] = (total, idle)
        if prev is None:
            return None
        total_delta = total - prev[0]
        idle_delta = idle - prev[1]
        if total_delta <= 0:
            return None
        busy_pct = (1.0 - (float(idle_delta) / float(total_delta))) * 100.0
        return max(0.0, min(100.0, busy_pct))
    except (OSError, ValueError, IndexError, TypeError):
        return None


def _ram_percent_from_proc_meminfo() -> float | None:
    try:
        meminfo_path = Path("/proc/meminfo")
        if not meminfo_path.exists():
            return None
        fields: dict[str, int] = {}
        for raw in meminfo_path.read_text(encoding="utf-8").splitlines():
            if ":" not in raw:
                continue
            key, value = raw.split(":", maxsplit=1)
            number = value.strip().split()[0]
            fields[key] = int(number)
        mem_total_kb = fields.get("MemTotal")
        if not mem_total_kb or mem_total_kb <= 0:
            return None
        mem_available_kb = fields.get("MemAvailable")
        if mem_available_kb is not None:
            used_pct = (1.0 - (float(mem_available_kb) / float(mem_total_kb))) * 100.0
            return max(0.0, min(100.0, used_pct))
        mem_free_kb = int(fields.get("MemFree", 0))
        buffers_kb = int(fields.get("Buffers", 0))
        cached_kb = int(fields.get("Cached", 0))
        reclaimable_kb = int(fields.get("SReclaimable", 0))
        available_kb = mem_free_kb + buffers_kb + cached_kb + reclaimable_kb
        used_pct = (1.0 - (float(available_kb) / float(mem_total_kb))) * 100.0
        return max(0.0, min(100.0, used_pct))
    except (OSError, ValueError, IndexError):
        return None


def _render_bot_stats_block(snapshot: BotStatusSnapshot) -> list[str]:
    cpu_text = f"{snapshot.cpu_percent:.1f}%" if snapshot.cpu_percent is not None else "-"
    ram_text = f"{snapshot.ram_percent:.1f}%" if snapshot.ram_percent is not None else "-"
    free_text = (
        readable_size(float(snapshot.free_bytes))
        if snapshot.free_bytes is not None
        else "-"
    )
    free_pct_text = (
        f"[{snapshot.free_percent:.1f}%]"
        if snapshot.free_percent is not None
        else "[-]"
    )

    return [
        "",
        "[&lt;&gt;] Bot Stats",
        f"┠ Tasks: {snapshot.tasks_count}",
        f"┠ CPU: {cpu_text} | F: {free_text} {free_pct_text}",
        f"┠ RAM: {ram_text} | UPTIME: {readable_time(snapshot.uptime_seconds)}",
        (
            f"┖ DL: {format_speed_bps(snapshot.download_bps)} | "
            f"UL: {format_speed_bps(snapshot.upload_bps)}"
        ),
    ]


def render_bot_stats_footer(snapshot: BotStatusSnapshot) -> str:
    return "\n".join(_render_bot_stats_block(snapshot))


def _elapsed_from_iso(iso_timestamp: str | None) -> int:
    if not iso_timestamp:
        return 0
    try:
        started = datetime.fromisoformat(iso_timestamp)
        return int(max(0.0, time.time() - started.timestamp()))
    except (ValueError, OSError, OverflowError):
        return 0
