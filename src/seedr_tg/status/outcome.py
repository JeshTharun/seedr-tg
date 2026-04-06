from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape

from seedr_tg.db.models import JobPhase
from seedr_tg.status.template import readable_size, readable_time


@dataclass(slots=True)
class RequesterIdentity:
    user_id: int | None = None
    username: str | None = None
    display_name: str | None = None


def render_task_outcome_message(
    *,
    title: str,
    size_bytes: int,
    elapsed_seconds: int,
    mode_tags: str,
    total_files: int,
    requester: RequesterIdentity,
    phase: JobPhase,
    file_names: list[str] | None = None,
    failure_reason: str | None = None,
) -> str:
    safe_title = escape((title or "Untitled").strip())
    mention = _format_requester(requester)
    lines: list[str] = [
        safe_title,
        "┃",
        f"┠ Size: {readable_size(float(max(0, size_bytes)))}",
        f"┠ Elapsed: {readable_time(int(max(0, elapsed_seconds)))}",
        f"┠ Mode:  {escape(mode_tags)}",
        f"┠ Total Files: {max(0, int(total_files))}",
        f"┖ By: {mention}",
        "",
    ]

    if phase == JobPhase.COMPLETED:
        lines.extend(
            [
                "File have been Sent to Target Channel.",
            ]
        )
        names = file_names or []
        if names:
            for index, name in enumerate(names, start=1):
                lines.append(f"{index}. {escape(name)}")
        else:
            lines.append("1. Uploaded file")
        return "\n".join(lines)

    if phase == JobPhase.CANCELED:
        lines.append("[&lt;&gt;] Task has been Canceled.")
        return "\n".join(lines)

    lines.append("[&lt;&gt;] Task has Failed.")
    if failure_reason:
        lines.append(f"[&lt;&gt;] Reason: {escape(failure_reason)}")
    return "\n".join(lines)


def elapsed_seconds_from_iso(created_at: str | None) -> int:
    if not created_at:
        return 0
    try:
        started = datetime.fromisoformat(created_at)
    except ValueError:
        return 0
    now = datetime.now(tz=started.tzinfo)
    return int(max(0.0, (now - started).total_seconds()))


def _format_requester(requester: RequesterIdentity) -> str:
    username = (requester.username or "").strip().lstrip("@")
    if username:
        return f"@{escape(username)}"
    if requester.user_id:
        label = requester.display_name or f"user-{requester.user_id}"
        return (
            f'<a href="tg://user?id={int(requester.user_id)}">'
            f"{escape(label)}"
            "</a>"
        )
    if requester.display_name:
        return escape(requester.display_name)
    return "Unknown"
