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

from telegram import Message, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from seedr_tg.db.models import JobPhase
from seedr_tg.db.repository import JobRepository
from seedr_tg.direct.renamer import FilenameRenamer, RegexSubstitutionRule, RenameRequest
from seedr_tg.status.outcome import RequesterIdentity, render_task_outcome_message
from seedr_tg.status.template import (
    format_speed_bps,
    readable_size,
    readable_time,
)
from seedr_tg.status.unified import ActiveTaskSnapshot, TaskPhase
from seedr_tg.telegram.uploader import TelegramUploader

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class MediaRenameOptions:
    explicit_name: str | None = None
    prefix: str | None = None
    substitutions: list[RegexSubstitutionRule] = field(default_factory=list)


@dataclass(slots=True)
class TelegramMediaDescriptor:
    file_id: str
    original_name: str
    size_bytes: int | None = None


class TelegramMediaRenameHandler:
    """Renames replied Telegram media before re-uploading to target chat."""

    def __init__(
        self,
        *,
        uploader: TelegramUploader,
        repository: JobRepository,
        renamer: FilenameRenamer,
        download_root: Path,
        allowed_chat_ids: set[int],
        bot_start_time: float,
        max_concurrent_tasks: int = 2,
        register_active_task_callback: (
            Callable[[ActiveTaskSnapshot], Awaitable[None]] | None
        ) = None,
        update_active_task_callback: (
            Callable[[ActiveTaskSnapshot], Awaitable[None]] | None
        ) = None,
        unregister_active_task_callback: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._uploader = uploader
        self._repository = repository
        self._renamer = renamer
        self._download_root = download_root
        self._allowed_chat_ids = set(allowed_chat_ids)
        self._bot_start_time = float(bot_start_time)
        self._task_semaphore = asyncio.Semaphore(max(1, int(max_concurrent_tasks)))
        self._register_active_task_callback = register_active_task_callback
        self._update_active_task_callback = update_active_task_callback
        self._unregister_active_task_callback = unregister_active_task_callback

    async def handle(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        requester = RequesterIdentity(
            user_id=update.effective_user.id if update.effective_user else None,
            username=update.effective_user.username if update.effective_user else None,
            display_name=update.effective_user.full_name if update.effective_user else None,
        )
        if message is None or chat is None:
            return
        if chat.id not in self._allowed_chat_ids:
            await message.reply_text("This command is not enabled for this chat.")
            return
        if message.reply_to_message is None:
            await message.reply_text(
                "Reply to a Telegram media message and run: "
                "/rename [--rename <name>] [--prefix <value>] "
                "[--sub <pattern=>replacement>] [--sub-cs <pattern=>replacement>]"
            )
            return

        try:
            descriptor = self._extract_media_descriptor(message.reply_to_message)
        except ValueError as exc:
            await message.reply_text(str(exc))
            return

        try:
            options = self._parse_options(message.text or "")
        except ValueError as exc:
            await message.reply_text(str(exc))
            return

        selected_mode = self._rename_mode_label(options)
        temp_dir = (
            self._download_root
            / "telegram"
            / "rename"
            / f"msg_{chat.id}_{message.message_id}_{uuid.uuid4().hex[:8]}"
        )
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_download_path = temp_dir / "payload.part"
        task_id = f"rename:{chat.id}:{message.message_id}"
        last_progress_percent = 0.0
        status_lock = asyncio.Lock()
        speed_samples: dict[str, tuple[float, int]] = {}

        def measure_speed(channel: str, current_bytes: int) -> float:
            now = time.monotonic()
            previous = speed_samples.get(channel)
            speed_samples[channel] = (now, int(current_bytes))
            if previous is None:
                return 0.0
            prev_at, prev_bytes = previous
            delta_bytes = int(current_bytes) - int(prev_bytes)
            delta_time = now - prev_at
            if delta_bytes <= 0 or delta_time <= 0:
                return 0.0
            return float(delta_bytes) / delta_time

        def render_transfer_detail(
            *,
            phase_label: str,
            current_bytes: int,
            total_bytes: int,
            speed_bps: float,
            elapsed_seconds: int,
        ) -> str:
            processed_line = (
                f"Processed: {readable_size(current_bytes)} of {readable_size(total_bytes)}"
                if total_bytes > 0
                else f"Processed: {readable_size(current_bytes)}"
            )
            eta_seconds: int | None = None
            if total_bytes > current_bytes and speed_bps > 0:
                eta_seconds = int((total_bytes - current_bytes) / speed_bps)
            eta_text = readable_time(eta_seconds) if eta_seconds is not None else "-"
            return (
                f"{processed_line}\n"
                f"Status: {phase_label} | ETA: {eta_text}\n"
                f"Speed: {format_speed_bps(speed_bps)} | Elapsed: {readable_time(elapsed_seconds)}"
            )

        async def update_status(
            *,
            step: str,
            final_name: str | None = None,
            progress_percent: float | None = None,
            progress_detail: str | None = None,
            force: bool = False,
            phase: TaskPhase = "running",
            speed_bps: float | None = None,
            eta_seconds: int | None = None,
        ) -> None:
            del progress_detail, force
            nonlocal last_progress_percent
            async with status_lock:
                if progress_percent is not None:
                    last_progress_percent = progress_percent
                title = final_name or descriptor.original_name
                snapshot = ActiveTaskSnapshot(
                    task_id=task_id,
                    task_type="rename",
                    title=f"[Rename] {title}",
                    progress_percent=max(0.0, min(100.0, last_progress_percent)),
                    status_text=step,
                    speed_bps=speed_bps,
                    eta_seconds=eta_seconds,
                    elapsed_seconds=int(max(0.0, time.monotonic() - started_at)),
                    phase=phase,
                )
                if phase == "queued":
                    if self._register_active_task_callback is not None:
                        await self._register_active_task_callback(snapshot)
                elif self._update_active_task_callback is not None:
                    await self._update_active_task_callback(snapshot)

        try:
            started_at = time.monotonic()
            await update_status(step="Queued", phase="queued", progress_percent=0.0)
            if self._task_semaphore.locked():
                await update_status(step="Waiting for free rename slot")
            async with self._task_semaphore:
                await self._run_rename_flow(
                    message=message,
                    chat_id=chat.id,
                    requester=requester,
                    started_at=started_at,
                    selected_mode=selected_mode,
                    descriptor=descriptor,
                    options=options,
                    context=context,
                    temp_dir=temp_dir,
                    temp_download_path=temp_download_path,
                    speed_samples=speed_samples,
                    measure_speed=measure_speed,
                    render_transfer_detail=render_transfer_detail,
                    update_status=update_status,
                )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception(
                "Telegram media rename failed chat_id=%s message_id=%s",
                chat.id,
                message.message_id,
            )
            await update_status(
                step=f"Failed: {exc}",
                progress_detail="See logs for traceback.",
                force=True,
                phase="failed",
            )
            await message.reply_text(
                render_task_outcome_message(
                    title=descriptor.original_name,
                    size_bytes=int(descriptor.size_bytes or 0),
                    elapsed_seconds=int(time.monotonic() - started_at),
                    mode_tags="#Leech | #telegram",
                    total_files=0,
                    requester=requester,
                    phase=JobPhase.FAILED,
                    failure_reason=str(exc),
                ),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        finally:
            if self._unregister_active_task_callback is not None:
                await self._unregister_active_task_callback(task_id)
            await asyncio.to_thread(shutil.rmtree, temp_dir, True)

    async def _run_rename_flow(
        self,
        *,
        message: Message,
        chat_id: int,
        requester: RequesterIdentity,
        started_at: float,
        selected_mode: str,
        descriptor: TelegramMediaDescriptor,
        options: MediaRenameOptions,
        context: ContextTypes.DEFAULT_TYPE,
        temp_dir: Path,
        temp_download_path: Path,
        speed_samples: dict[str, tuple[float, int]],
        measure_speed,
        render_transfer_detail,
        update_status,
    ) -> None:
        try:
            LOGGER.info(
                (
                    "Telegram media rename started chat_id=%s message_id=%s "
                    "original=%s mode=%s"
                ),
                chat_id,
                message.message_id,
                descriptor.original_name,
                selected_mode,
            )
            await update_status(step="Downloading media")

            try:
                telegram_file = await context.bot.get_file(descriptor.file_id)
                await telegram_file.download_to_drive(custom_path=str(temp_download_path))
                downloaded_path = temp_download_path
            except BadRequest as exc:
                if "file is too big" not in str(exc).lower():
                    raise
                reply_chat = message.reply_to_message.chat
                reply_chat_id = reply_chat.id if reply_chat is not None else chat_id
                is_private_reply_chat = (
                    reply_chat is not None
                    and str(getattr(reply_chat, "type", "")).lower() == "private"
                )
                mtproto_chat_id = self._uploader.resolve_mtproto_chat_id(
                    bot_chat_id=reply_chat_id,
                    is_private_chat=is_private_reply_chat,
                )
                source_chat_id, source_message_id = self._resolve_mtproto_source_message(
                    message.reply_to_message,
                    default_chat_id=mtproto_chat_id,
                )
                LOGGER.info(
                    (
                        "Bot API media download too large; falling back to MTProto "
                        "chat_id=%s message_id=%s"
                    ),
                    source_chat_id,
                    source_message_id,
                )
                speed_samples.pop("download", None)
                await update_status(step="Downloading media (MTProto fallback)")

                async def download_progress_hook(
                    channel: str,
                    current_bytes: int,
                    total_bytes: int,
                ) -> None:
                    if channel != "download":
                        return
                    speed_bps = measure_speed("download", current_bytes)
                    percent = 0.0
                    if total_bytes > 0:
                        percent = (float(current_bytes) / float(total_bytes)) * 100.0
                    eta_seconds = None
                    if total_bytes > current_bytes and speed_bps > 0:
                        eta_seconds = int((total_bytes - current_bytes) / speed_bps)
                    await update_status(
                        step="Downloading media (MTProto fallback)",
                        progress_percent=percent,
                        speed_bps=speed_bps,
                        eta_seconds=eta_seconds,
                    )

                try:
                    downloaded_path = await self._uploader.download_telegram_message_media(
                        chat_id=source_chat_id,
                        message_id=source_message_id,
                        destination=temp_download_path,
                        fallback_file_id=descriptor.file_id,
                        bot_chat_id=reply_chat_id,
                        progress_hook=download_progress_hook,
                    )
                except RuntimeError:
                    if (
                        source_chat_id == mtproto_chat_id
                        and source_message_id == message.reply_to_message.message_id
                    ):
                        raise
                    LOGGER.info(
                        (
                            "MTProto source-forward lookup failed; retrying with replied "
                            "message context chat_id=%s message_id=%s"
                        ),
                        reply_chat_id,
                        message.reply_to_message.message_id,
                    )
                    downloaded_path = await self._uploader.download_telegram_message_media(
                        chat_id=mtproto_chat_id,
                        message_id=message.reply_to_message.message_id,
                        destination=temp_download_path,
                        fallback_file_id=descriptor.file_id,
                        bot_chat_id=reply_chat_id,
                        progress_hook=download_progress_hook,
                    )

            if not downloaded_path.exists() or downloaded_path.stat().st_size <= 0:
                raise RuntimeError("Failed to download replied media for rename (empty file).")

            await update_status(step="Applying rename")
            request = RenameRequest(
                explicit_name=options.explicit_name,
                prefix=options.prefix,
                substitutions=options.substitutions,
            )
            final_name = self._renamer.build_name(
                original_name=descriptor.original_name,
                request=request,
                target_directory=temp_dir,
            )

            final_path = temp_dir / final_name
            await asyncio.to_thread(downloaded_path.rename, final_path)

            upload_settings = await self._repository.get_upload_settings()
            speed_samples.pop("upload", None)

            async def upload_progress_hook(
                completed: int,
                total: int,
                detail: str,
                current: int,
                total_bytes: int,
            ) -> None:
                del completed, total
                percent = 0.0
                if total_bytes > 0:
                    percent = (float(current) / float(total_bytes)) * 100.0
                speed_bps = measure_speed("upload", current)
                eta_seconds = None
                if total_bytes > current and speed_bps > 0:
                    eta_seconds = int((total_bytes - current) / speed_bps)
                await update_status(
                    step="Uploading to Telegram",
                    final_name=final_name,
                    progress_percent=percent,
                    speed_bps=speed_bps,
                    eta_seconds=eta_seconds,
                )

            await update_status(step="Uploading to Telegram", final_name=final_name)
            await self._uploader.upload_files(
                [final_path],
                caption_prefix="Telegram media rename",
                upload_settings=upload_settings,
                max_concurrent_uploads=1,
                progress_hook=upload_progress_hook,
            )

            LOGGER.info(
                (
                    "Telegram media rename upload completed chat_id=%s "
                    "original=%s final=%s"
                ),
                chat_id,
                descriptor.original_name,
                final_name,
            )
            await update_status(
                step="Completed",
                final_name=final_name,
                progress_percent=100.0,
                force=True,
                phase="completed",
            )
            await message.reply_text(
                render_task_outcome_message(
                    title=final_name,
                    size_bytes=int(final_path.stat().st_size if final_path.exists() else 0),
                    elapsed_seconds=int(time.monotonic() - started_at),
                    mode_tags="#Leech | #telegram",
                    total_files=1,
                    requester=requester,
                    phase=JobPhase.COMPLETED,
                    file_names=[final_name],
                    failure_reason=None,
                ),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception:
            raise

    @staticmethod
    def _extract_media_descriptor(message: Message) -> TelegramMediaDescriptor:
        if message.document is not None:
            extension = TelegramMediaRenameHandler._extension_from_name_or_default(
                message.document.file_name,
                ".bin",
            )
            original_name = (
                message.document.file_name
                or f"document_{message.message_id}{extension}"
            )
            return TelegramMediaDescriptor(
                file_id=message.document.file_id,
                original_name=original_name,
                size_bytes=message.document.file_size,
            )

        if message.video is not None:
            extension = TelegramMediaRenameHandler._extension_from_name_or_default(
                message.video.file_name,
                ".mp4",
            )
            original_name = message.video.file_name or f"video_{message.message_id}{extension}"
            return TelegramMediaDescriptor(
                file_id=message.video.file_id,
                original_name=original_name,
                size_bytes=message.video.file_size,
            )

        if message.audio is not None:
            extension = TelegramMediaRenameHandler._extension_from_name_or_default(
                message.audio.file_name,
                ".mp3",
            )
            original_name = message.audio.file_name or f"audio_{message.message_id}{extension}"
            return TelegramMediaDescriptor(
                file_id=message.audio.file_id,
                original_name=original_name,
                size_bytes=message.audio.file_size,
            )

        if message.animation is not None:
            extension = TelegramMediaRenameHandler._extension_from_name_or_default(
                message.animation.file_name,
                ".mp4",
            )
            original_name = (
                message.animation.file_name
                or f"animation_{message.message_id}{extension}"
            )
            return TelegramMediaDescriptor(
                file_id=message.animation.file_id,
                original_name=original_name,
                size_bytes=message.animation.file_size,
            )

        if message.voice is not None:
            return TelegramMediaDescriptor(
                file_id=message.voice.file_id,
                original_name=f"voice_{message.message_id}.ogg",
                size_bytes=message.voice.file_size,
            )

        if message.photo:
            photo = message.photo[-1]
            return TelegramMediaDescriptor(
                file_id=photo.file_id,
                original_name=f"photo_{message.message_id}.jpg",
                size_bytes=photo.file_size,
            )

        raise ValueError(
            "Replied message has no supported media. "
            "Use document/video/audio/photo/animation/voice."
        )

    @staticmethod
    def _extension_from_name_or_default(file_name: str | None, fallback: str) -> str:
        if not file_name:
            return fallback
        suffix = Path(file_name).suffix
        return suffix if suffix else fallback

    @staticmethod
    def _resolve_mtproto_source_message(
        replied_message: Message,
        *,
        default_chat_id: int,
    ) -> tuple[int, int]:
        """Resolve best MTProto source for media download.

        For forwarded media, bot-PM message ids may not be downloadable via user MTProto
        session. Prefer the original forwarded source when Telegram exposes it.
        """
        default_message_id = replied_message.message_id
        forward_origin = getattr(replied_message, "forward_origin", None)
        if forward_origin is not None:
            origin_chat = getattr(forward_origin, "chat", None)
            origin_message_id = getattr(forward_origin, "message_id", None)
            if origin_chat is not None and origin_message_id is not None:
                origin_chat_id = getattr(origin_chat, "id", None)
                if isinstance(origin_chat_id, int):
                    return origin_chat_id, int(origin_message_id)

        legacy_forward_chat = getattr(replied_message, "forward_from_chat", None)
        legacy_forward_message_id = getattr(replied_message, "forward_from_message_id", None)
        if legacy_forward_chat is not None and legacy_forward_message_id is not None:
            legacy_chat_id = getattr(legacy_forward_chat, "id", None)
            if isinstance(legacy_chat_id, int):
                return legacy_chat_id, int(legacy_forward_message_id)

        return default_chat_id, default_message_id

    @staticmethod
    def _parse_options(text: str) -> MediaRenameOptions:
        usage = (
            "Usage: /rename [--rename <name>] [--prefix <value>] "
            "[--sub <pattern=>replacement>] [--sub-cs <pattern=>replacement>]"
        )
        try:
            tokens = shlex.split(text)
        except ValueError as exc:
            raise ValueError(f"Invalid command format: {exc}. {usage}") from exc

        options = MediaRenameOptions()
        idx = 1
        while idx < len(tokens):
            token = tokens[idx]
            if token == "--rename":
                idx += 1
                value, idx = TelegramMediaRenameHandler._consume_non_option_span(tokens, idx)
                if not value:
                    raise ValueError(f"Missing value for {token}.")
                options.explicit_name = value
                continue
            elif token == "--prefix":
                idx += 1
                options.prefix = TelegramMediaRenameHandler._require_value(tokens, idx, token)
            elif token in {"--sub", "--sub-cs"}:
                idx += 1
                raw_rule = TelegramMediaRenameHandler._require_value(tokens, idx, token)
                options.substitutions.append(
                    TelegramMediaRenameHandler._parse_substitution_rule(
                        raw_rule,
                        case_sensitive=(token == "--sub-cs"),
                    )
                )
            elif token.startswith("--"):
                raise ValueError(f"Unknown option: {token}. {usage}")
            elif options.explicit_name is None:
                value, idx = TelegramMediaRenameHandler._consume_non_option_span(tokens, idx)
                if not value:
                    raise ValueError(usage)
                options.explicit_name = value
                continue
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
    def _consume_non_option_span(tokens: list[str], idx: int) -> tuple[str, int]:
        end = idx
        while end < len(tokens) and not tokens[end].startswith("--"):
            end += 1
        return " ".join(tokens[idx:end]).strip(), end

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
    def _rename_mode_label(options: MediaRenameOptions) -> str:
        mode_parts: list[str] = []
        if options.explicit_name:
            mode_parts.append("explicit")
        if options.prefix:
            mode_parts.append("prefix")
        if options.substitutions:
            mode_parts.append(f"regex:{len(options.substitutions)}")
        if not mode_parts:
            mode_parts.append("original")
        return "+".join(mode_parts)
