from __future__ import annotations

import asyncio
import logging
import shlex
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes

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
from seedr_tg.status.template import collect_bot_stats, render_operation_status

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
    ) -> None:
        self._downloader = downloader
        self._renamer = renamer
        self._uploader = uploader
        self._repository = repository
        self._download_root = download_root
        self._allowed_chat_ids = set(allowed_chat_ids)
        self._bot_start_time = float(bot_start_time)

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

        async def build_bot_stats():
            jobs = await self._repository.list_jobs(include_final=False)
            return collect_bot_stats(
                download_dir=self._download_root,
                bot_start_time=self._bot_start_time,
                tasks_count=len(jobs),
                download_bps=sum(float(job.download_speed_bps or 0.0) for job in jobs),
                upload_bps=sum(float(job.upload_speed_bps or 0.0) for job in jobs),
            )

        initial_stats = await build_bot_stats()
        status_message = await message.reply_text(
            render_operation_status(
                title=f"Direct Transfer: {options.url}",
                fields=[("URL", options.url)],
                step="Queued",
                bot_stats=initial_stats,
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

        async def update_status(
            *,
            step: str,
            final_name: str | None = None,
            progress_detail: str | None = None,
        ) -> None:
            try:
                bot_stats = await build_bot_stats()
                await status_message.edit_text(
                    render_operation_status(
                        title=f"Direct Transfer: {options.url}",
                        fields=[("URL", options.url)],
                        step=step,
                        final_name=final_name,
                        progress_detail=progress_detail,
                        bot_stats=bot_stats,
                    ),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except BadRequest as exc:
                if "message is not modified" in str(exc).lower():
                    return
                raise

        try:
            LOGGER.info(
                "Direct transfer started chat_id=%s message_id=%s url=%s",
                chat.id,
                message.message_id,
                options.url,
            )
            await update_status(step="Downloading URL")

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
            await update_status(step="Applying rename", final_name=final_name)
            final_path = temp_dir / final_name
            await asyncio.to_thread(temp_download_path.rename, final_path)

            await update_status(step="Uploading to Telegram", final_name=final_name)
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
            size_text = (
                f"{self._format_size(downloaded.size_bytes)} "
                f"({downloaded.size_bytes} bytes)"
            )
            await status_message.edit_text(
                render_operation_status(
                    title=f"Direct Transfer: {downloaded.original_name}",
                    fields=[
                        ("URL", options.url),
                        ("Original", downloaded.original_name),
                        ("Size", size_text),
                    ],
                    step=f"Completed in {elapsed:.2f}s",
                    final_name=final_name,
                    progress_percent=100.0,
                    bot_stats=await build_bot_stats(),
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
            await update_status(step=f"Failed: Invalid URL ({exc})")
        except DirectTelegramUploadError as exc:
            LOGGER.exception("Direct upload failed for url=%s", options.url)
            await update_status(step=f"Failed: Upload error ({exc})")
        except DirectDownloadError as exc:
            LOGGER.exception("Direct download failed for url=%s", options.url)
            await update_status(step=f"Failed: Download error ({exc})")
        except Exception:
            LOGGER.exception("Unexpected direct transfer error chat_id=%s", chat.id)
            await update_status(step="Failed: Unexpected error")
        finally:
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
