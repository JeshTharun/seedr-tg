from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TaskType = Literal["queue", "rename", "direct"]
TaskPhase = Literal["queued", "running", "completed", "failed", "canceled"]


@dataclass(slots=True)
class ActiveTaskSnapshot:
    task_id: str
    task_type: TaskType
    title: str
    progress_percent: float
    status_text: str
    speed_bps: float | None = None
    eta_seconds: int | None = None
    elapsed_seconds: int = 0
    phase: TaskPhase = "running"
    cancel_command: str | None = None
