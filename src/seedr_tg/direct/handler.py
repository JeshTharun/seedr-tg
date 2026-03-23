from __future__ import annotations

import asyncio
import logging
import shlex
import shutil
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from seedr_tg.db.models import JobPhase
from seedr_tg.db.repository import JobRepository
from seedr_tg.direct.downloader import (
    DirectDownloader,
    DirectDownloadError,
    InvalidDirectUrlError,
)
from seedr_tg.direct.renamer import (
    FilenameRenamer,
    RegexSubstitutionRule,
    RenameRequest,
)
from seedr_tg.direct.telegram_uploader import (
    DirectTelegramUploader,
    DirectTelegramUploadError,
)
from seedr_tg.status.outcome import RequesterIdentity, render_task_outcome_message
from seedr_tg.status.unified import ActiveTaskSnapshot, TaskPhase

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class DirectCommandOptions:
    url: str
    rename_value: str | None = None
    prefix: str | None = None
    substitutions: list[RegexSubstitutionRule] = field(default_factory=list)


class DirectDownloadCommandHandler:
    """Handles the /direct command: download, rename, upload, and cleanup."""

    def __init__(
        self,
        *,
        downloader: DirectDownloader,
        renamer: FilenameRenamer,
        uploader: DirectTelegramUploader,
        repository: JobRepository,
        download_root: Path,
        allowed_chat_ids: set[int],
        bot_start_time: float,
        register_active_task_callback: Callable[[ActiveTaskSnapshot], Awaitable[None]],
        update_active_task_callback: Callable[[ActiveTaskSnapshot], Awaitable[None]],
        unregister_active_task_callback: Callable[[str], Awaitable[None]],
    ) -> None:
        self._downloader = downloader
        self._renamer = renamer
        self._uploader = uploader
        self._repository = repository
        self._download_root = download_root
        self._allowed_chat_ids = set(allowed_chat_ids)
        self._bot_start_time = float(bot_start_time)
        self._register_active_task_callback = register_active_task_callback
        self._update_active_task_callback = update_active_task_callback
        self._unregister_active_task_callback = unregister_active_task_callback

    async def handle(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None:
            return
        if chat.id not in self._allowed_chat_ids:
            await message.reply_text("This command is not enabled for this chat.")
            return

        try:
            options = self._parse_options(message.text or "")
        except ValueError as exc:
            await message.reply_text(str(exc))
            return

        started_at = time.monotonic()
        temp_dir = (
            self._download_root
            / "direct"
            / f"msg_{chat.id}_{message.message_id}_{uuid.uuid4().hex[:8]}"
        )
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_download_path = temp_dir / "payload.bin.part"
        task_id = f"direct:{chat.id}:{message.message_id}"
        task_title = options.rename_value or options.url

        async def push_task(
            *,
            phase: TaskPhase,
            step: str,
            progress_percent: float,
            speed_bps: float | None = None,
            eta_seconds: int | None = None,
        ) -> None:
            snapshot = ActiveTaskSnapshot(
                task_id=task_id,
                task_type="direct",
                title=f"[Direct] {task_title}",
                progress_percent=progress_percent,
                status_text=step,
                speed_bps=speed_bps,
                eta_seconds=eta_seconds,
                elapsed_seconds=int(max(0.0, time.monotonic() - started_at)),
                phase=phase,
            )
            if phase == "queued":
                await self._register_active_task_callback(snapshot)
            else:
                await self._update_active_task_callback(snapshot)

        try:
            await push_task(phase="queued", step="Queued", progress_percent=0.0)
            LOGGER.info(
                "Direct transfer started chat_id=%s message_id=%s url=%s",
                chat.id,
                message.message_id,
                options.url,
            )
            await push_task(phase="running", step="Downloading URL", progress_percent=15.0)

            downloaded = await self._downloader.download_to_path(
                url=options.url,
                destination_path=str(temp_download_path),
            )

            rename_request = RenameRequest(
                explicit_name=options.rename_value,
                prefix=options.prefix,
                substitutions=options.substitutions,
            )
            final_name = self._renamer.build_name(
                original_name=downloaded.original_name,
                request=rename_request,
                target_directory=temp_dir,
            )
            await push_task(
                phase="running",
                step="Applying rename",
                progress_percent=35.0,
            )
            final_path = temp_dir / final_name
            await asyncio.to_thread(temp_download_path.rename, final_path)

            await push_task(
                phase="running",
                step="Uploading to Telegram",
                progress_percent=70.0,
            )
            await self._uploader.upload_file(bot=context.bot, chat_id=chat.id, file_path=final_path)

            elapsed = time.monotonic() - started_at
            LOGGER.info(
                (
                    "Direct transfer completed chat_id=%s message_id=%s "
                    "original=%s new=%s size=%s elapsed=%.2fs"
                ),
                chat.id,
                message.message_id,
                downloaded.original_name,
                final_name,
                downloaded.size_bytes,
                elapsed,
            )
            await push_task(
                phase="completed",
                step=f"Completed in {elapsed:.2f}s",
                progress_percent=100.0,
            )
            await message.reply_text(
                render_task_outcome_message(
                    title=final_name,
                    size_bytes=downloaded.size_bytes,
                    elapsed_seconds=int(elapsed),
                    mode_tags="#Leech | #ytdlp",
                    total_files=1,
                    requester=RequesterIdentity(
                        user_id=update.effective_user.id if update.effective_user else None,
                        username=(
                            update.effective_user.username
                            if update.effective_user
                            else None
                        ),
                        display_name=(
                            update.effective_user.full_name
                            if update.effective_user
                            else None
                        ),
                    ),
                    phase=JobPhase.COMPLETED,
                    file_names=[final_name],
                    failure_reason=None,
                ),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except InvalidDirectUrlError as exc:
            LOGGER.warning(
                "Direct transfer rejected due to invalid URL url=%s error=%s",
                options.url,
                exc,
            )
            await push_task(
                phase="failed",
                step=f"Failed: Invalid URL ({exc})",
                progress_percent=0.0,
            )
            await message.reply_text(
                render_task_outcome_message(
                    title=options.url,
                    size_bytes=0,
                    elapsed_seconds=int(time.monotonic() - started_at),
                    mode_tags="#Leech | #ytdlp",
                    total_files=0,
                    requester=RequesterIdentity(
                        user_id=update.effective_user.id if update.effective_user else None,
                        username=(
                            update.effective_user.username
                            if update.effective_user
                            else None
                        ),
                        display_name=(
                            update.effective_user.full_name
                            if update.effective_user
                            else None
                        ),
                    ),
                    phase=JobPhase.FAILED,
                    failure_reason=f"Invalid URL ({exc})",
                ),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except DirectTelegramUploadError as exc:
            LOGGER.exception("Direct upload failed for url=%s", options.url)
            await push_task(
                phase="failed",
                step=f"Failed: Upload error ({exc})",
                progress_percent=70.0,
            )
            await message.reply_text(
                render_task_outcome_message(
                    title=options.url,
                    size_bytes=0,
                    elapsed_seconds=int(time.monotonic() - started_at),
                    mode_tags="#Leech | #ytdlp",
                    total_files=0,
                    requester=RequesterIdentity(
                        user_id=update.effective_user.id if update.effective_user else None,
                        username=(
                            update.effective_user.username
                            if update.effective_user
                            else None
                        ),
                        display_name=(
                            update.effective_user.full_name
                            if update.effective_user
                            else None
                        ),
                    ),
                    phase=JobPhase.FAILED,
                    failure_reason=f"Upload error ({exc})",
                ),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except DirectDownloadError as exc:
            LOGGER.exception("Direct download failed for url=%s", options.url)
            await push_task(
                phase="failed",
                step=f"Failed: Download error ({exc})",
                progress_percent=10.0,
            )
            await message.reply_text(
                render_task_outcome_message(
                    title=options.url,
                    size_bytes=0,
                    elapsed_seconds=int(time.monotonic() - started_at),
                    mode_tags="#Leech | #ytdlp",
                    total_files=0,
                    requester=RequesterIdentity(
                        user_id=update.effective_user.id if update.effective_user else None,
                        username=(
                            update.effective_user.username
                            if update.effective_user
                            else None
                        ),
                        display_name=(
                            update.effective_user.full_name
                            if update.effective_user
                            else None
                        ),
                    ),
                    phase=JobPhase.FAILED,
                    failure_reason=f"Download error ({exc})",
                ),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception:
            LOGGER.exception("Unexpected direct transfer error chat_id=%s", chat.id)
            await push_task(
                phase="failed",
                step="Failed: Unexpected error",
                progress_percent=0.0,
            )
            await message.reply_text(
                render_task_outcome_message(
                    title=options.url,
                    size_bytes=0,
                    elapsed_seconds=int(time.monotonic() - started_at),
                    mode_tags="#Leech | #ytdlp",
                    total_files=0,
                    requester=RequesterIdentity(
                        user_id=update.effective_user.id if update.effective_user else None,
                        username=(
                            update.effective_user.username
                            if update.effective_user
                            else None
                        ),
                        display_name=(
                            update.effective_user.full_name
                            if update.effective_user
                            else None
                        ),
                    ),
                    phase=JobPhase.FAILED,
                    failure_reason="Unexpected error",
                ),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        finally:
            await self._unregister_active_task_callback(task_id)
            await asyncio.to_thread(shutil.rmtree, temp_dir, True)

    @staticmethod
    def _parse_options(text: str) -> DirectCommandOptions:
        usage = (
            "Usage: /direct <url> [--rename <name>] [--prefix <value>] "
            "[--sub <pattern=>replacement>] [--sub-cs <pattern=>replacement>]"
        )
        try:
            tokens = shlex.split(text)
        except ValueError as exc:
            raise ValueError(f"Invalid command format: {exc}. {usage}") from exc

        if len(tokens) < 2:
            raise ValueError(usage)

        url = tokens[1]
        options = DirectCommandOptions(url=url)
        idx = 2
        while idx < len(tokens):
            token = tokens[idx]
            if token == "--rename":
                idx += 1
                options.rename_value = DirectDownloadCommandHandler._require_value(
                    tokens,
                    idx,
                    "--rename",
                )
            elif token == "--prefix":
                idx += 1
                options.prefix = DirectDownloadCommandHandler._require_value(
                    tokens,
                    idx,
                    "--prefix",
                )
            elif token in {"--sub", "--sub-cs"}:
                idx += 1
                rule_value = DirectDownloadCommandHandler._require_value(tokens, idx, token)
                options.substitutions.append(
                    DirectDownloadCommandHandler._parse_substitution_rule(
                        rule_value,
                        case_sensitive=(token == "--sub-cs"),
                    )
                )
            elif token.startswith("--"):
                raise ValueError(f"Unknown option: {token}. {usage}")
            elif options.rename_value is None:
                options.rename_value = token
            else:
                raise ValueError(f"Unexpected token: {token}. {usage}")
            idx += 1
        return options

    @staticmethod
    def _require_value(tokens: list[str], idx: int, option: str) -> str:
        if idx >= len(tokens):
            raise ValueError(f"Missing value for {option}.")
        return tokens[idx]

    @staticmethod
    def _parse_substitution_rule(raw_value: str, *, case_sensitive: bool) -> RegexSubstitutionRule:
        if "=>" in raw_value:
            pattern, replacement = raw_value.split("=>", maxsplit=1)
        elif "::" in raw_value:
            pattern, replacement = raw_value.split("::", maxsplit=1)
        else:
            raise ValueError(
                "Substitution must use 'pattern=>replacement' (or 'pattern::replacement')."
            )
        if not pattern:
            raise ValueError("Substitution pattern cannot be empty.")
        return RegexSubstitutionRule(
            pattern=pattern,
            replacement=replacement,
            case_sensitive=case_sensitive,
        )

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        value = float(max(0, size_bytes))
        for unit in units:
            if value < 1024.0 or unit == units[-1]:
                if unit == "B":
                    return f"{int(value)} {unit}"
                return f"{value:.2f} {unit}"
            value /= 1024.0
        return f"{size_bytes} B"
