from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from html import escape
from pathlib import Path
from typing import Literal

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from seedr_tg.db.models import (
    CaptionParseMode,
    JobRecord,
    SeedrDeviceCodeRecord,
    TelegramLoginState,
    TelegramUserSession,
    UploadMediaType,
    UploadSettings,
)
from seedr_tg.status.template import collect_bot_stats
from seedr_tg.telegram.uploader import (
    TelegramCodeExpiredError,
    TelegramCodeInvalidError,
    TelegramPasswordRequiredError,
)
from seedr_tg.worker.progress import format_job_status

LOGGER = logging.getLogger(__name__)
MAGNET_PATTERN = re.compile(r"magnet:\?[^\s]+", re.IGNORECASE)
SETTINGS_ACTION_CAPTION = "caption"
SETTINGS_ACTION_THUMBNAIL = "thumbnail"
STATUS_PAGE_SIZE = 3
STATUS_FILTER_ALL = "all"
STATUS_FILTER_ACTIVE = "active"
STATUS_FILTER_TRANSFERS = "transfers"
STATUS_FILTER_QUEUED = "queued"
StatusFilter = Literal["all", "active", "transfers", "queued"]
VALID_STATUS_FILTERS: tuple[StatusFilter, ...] = (
    STATUS_FILTER_ALL,
    STATUS_FILTER_ACTIVE,
    STATUS_FILTER_TRANSFERS,
    STATUS_FILTER_QUEUED,
)


class TelegramBotApp:
    def __init__(
        self,
        *,
        token: str,
        source_chat_id: int,
        admin_chat_id: int,
        enqueue_callback: Callable[[str, int, int], Awaitable[JobRecord | None]],
        list_jobs_callback: Callable[[], Awaitable[list[JobRecord]]],
        cancel_callback: Callable[[int], Awaitable[JobRecord]],
        set_admin_message_id_callback: Callable[[int, int], Awaitable[JobRecord]],
        start_seedr_auth_callback: Callable[[], Awaitable[SeedrDeviceCodeRecord]],
        complete_seedr_auth_callback: Callable[[], Awaitable[str]],
        start_user_session_callback: Callable[[str], Awaitable[TelegramLoginState]],
        submit_user_session_code_callback: Callable[[str], Awaitable[TelegramUserSession]],
        submit_user_session_password_callback: Callable[[str], Awaitable[TelegramUserSession]],
        get_upload_settings_callback: Callable[[], Awaitable[UploadSettings]],
        update_upload_settings_callback: Callable[..., Awaitable[UploadSettings]],
        reset_upload_settings_callback: Callable[[], Awaitable[UploadSettings]],
        direct_download_handler: Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]],
        telegram_media_rename_handler: Callable[
            [Update, ContextTypes.DEFAULT_TYPE],
            Awaitable[None],
        ],
        status_download_dir: Path,
        bot_start_time: float,
    ) -> None:
        self._source_chat_id = source_chat_id
        self._admin_chat_id = admin_chat_id
        self._enqueue_callback = enqueue_callback
        self._list_jobs_callback = list_jobs_callback
        self._cancel_callback = cancel_callback
        self._set_admin_message_id_callback = set_admin_message_id_callback
        self._start_seedr_auth_callback = start_seedr_auth_callback
        self._complete_seedr_auth_callback = complete_seedr_auth_callback
        self._start_user_session_callback = start_user_session_callback
        self._submit_user_session_code_callback = submit_user_session_code_callback
        self._submit_user_session_password_callback = submit_user_session_password_callback
        self._get_upload_settings_callback = get_upload_settings_callback
        self._update_upload_settings_callback = update_upload_settings_callback
        self._reset_upload_settings_callback = reset_upload_settings_callback
        self._status_download_dir = status_download_dir
        self._bot_start_time = float(bot_start_time)
        self._admin_message_cache: dict[int, tuple[str, int | None]] = {}
        self._admin_message_locks: dict[int, asyncio.Lock] = {}
        self._pending_settings_action: dict[int, str] = {}
        self._status_locks: dict[int, asyncio.Lock] = {}
        self._application = Application.builder().token(token).build()
        self._application.add_handler(CommandHandler("status", self._status))
        self._application.add_handler(CommandHandler("seedr_auth", self._seedr_auth))
        self._application.add_handler(CommandHandler("seedr_auth_done", self._seedr_auth_done))
        self._application.add_handler(CommandHandler("session_start", self._session_start))
        self._application.add_handler(CommandHandler("session_code", self._session_code))
        self._application.add_handler(CommandHandler("session_password", self._session_password))
        self._application.add_handler(CommandHandler("direct", direct_download_handler))
        self._application.add_handler(CommandHandler("rename", telegram_media_rename_handler))
        self._application.add_handler(CommandHandler("settings", self._settings))
        self._application.add_handler(
            CallbackQueryHandler(self._handle_settings_callback, pattern=r"^settings:")
        )
        self._application.add_handler(
            CallbackQueryHandler(self._handle_cancel, pattern=r"^cancel:\d+$")
        )
        self._application.add_handler(
            CallbackQueryHandler(self._handle_status_callback, pattern=r"^status:")
        )
        self._application.add_handler(
            MessageHandler(filters.Chat(chat_id=source_chat_id) & filters.TEXT, self._on_message)
        )
        self._application.add_handler(
            MessageHandler(
                filters.Chat(chat_id=admin_chat_id)
                & ~filters.COMMAND
                & (filters.TEXT | filters.PHOTO | filters.Document.IMAGE),
                self._handle_admin_settings_input,
            )
        )

    async def start(self) -> None:
        await self._application.initialize()
        await self._application.start()
        await self._application.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    async def stop(self) -> None:
        updater = self._application.updater
        if updater is not None:
            await updater.stop()
        await self._application.stop()
        await self._application.shutdown()

    async def post_admin_message(self, text: str, job_id: int | None = None) -> int:
        reply_markup = None
        if job_id is not None:
            reply_markup = InlineKeyboardMarkup.from_button(
                InlineKeyboardButton(text="Cancel current", callback_data=f"cancel:{job_id}")
            )
        attempts = 0
        while True:
            attempts += 1
            try:
                message = await self._application.bot.send_message(
                    chat_id=self._admin_chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                )
                break
            except RetryAfter as exc:
                wait_seconds = float(exc.retry_after)
                LOGGER.warning(
                    "Telegram flood control on admin message post. retry_after=%s",
                    wait_seconds,
                )
                if attempts >= 3:
                    raise
                await asyncio.sleep(wait_seconds)
        self._admin_message_cache[message.message_id] = (text, job_id)
        return message.message_id

    async def update_admin_message(
        self,
        message_id: int,
        text: str,
        job_id: int | None = None,
    ) -> None:
        lock = self._admin_message_locks.setdefault(message_id, asyncio.Lock())
        async with lock:
            cached = self._admin_message_cache.get(message_id)
            if cached == (text, job_id):
                return
            reply_markup = None
            if job_id is not None:
                reply_markup = InlineKeyboardMarkup.from_button(
                    InlineKeyboardButton(text="Cancel current", callback_data=f"cancel:{job_id}")
                )
            try:
                await self._application.bot.edit_message_text(
                    chat_id=self._admin_chat_id,
                    message_id=message_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                )
                self._admin_message_cache[message_id] = (text, job_id)
            except RetryAfter as exc:
                LOGGER.warning(
                    "Telegram flood control on admin message update. message_id=%s retry_after=%s",
                    message_id,
                    exc.retry_after,
                )
                await asyncio.sleep(float(exc.retry_after))
                return
            except BadRequest as exc:
                if "message is not modified" in str(exc).lower():
                    self._admin_message_cache[message_id] = (text, job_id)
                    LOGGER.debug("Skipped no-op admin message edit for message_id=%s", message_id)
                    return
                raise

    async def _on_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None:
            return
        magnet = self._extract_magnet(message.text or "")
        if magnet is None:
            return
        job = await self._enqueue_callback(magnet, chat.id, message.message_id)
        if job is None:
            await self.post_admin_message("<b>Ignored duplicate magnet</b>")
            return
        # Queue runner owns lifecycle status messages to avoid duplicate posts from race conditions.
        await asyncio.sleep(0)

    async def _status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        if not await self._ensure_admin(update):
            return
        message = update.effective_message
        if message is None:
            return
        payload, keyboard = await self._render_status_page(
            selected_filter=STATUS_FILTER_ACTIVE,
            page=0,
        )
        await message.reply_text(
            payload,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )

    async def _handle_status_callback(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        del context
        query = update.callback_query
        if query is None:
            return
        if query.message is None or query.message.chat_id != self._admin_chat_id:
            await query.answer("Admin only", show_alert=True)
            return
        data = query.data or ""
        parts = data.split(":")
        if len(parts) < 4:
            await query.answer()
            return

        action = parts[1]
        selected_filter = self._parse_status_filter(parts[2])
        page = self._parse_status_page(parts[3])

        if action == "setfilter" and len(parts) >= 5:
            selected_filter = self._parse_status_filter(parts[4])
            page = 0
        elif action == "prev":
            page = max(0, page - 1)
        elif action == "next":
            page = page + 1

        if action == "refresh":
            await query.answer("Refreshing...")
        else:
            await query.answer()

        message_id = query.message.message_id
        lock = self._status_locks.setdefault(message_id, asyncio.Lock())
        async with lock:
            payload, keyboard = await self._render_status_page(
                selected_filter=selected_filter,
                page=page,
            )
            try:
                await query.edit_message_text(
                    text=payload,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                    disable_web_page_preview=True,
                )
            except BadRequest as exc:
                message = str(exc).lower()
                if "message is not modified" in message:
                    return
                if "can't parse entities" in message:
                    await query.edit_message_text(
                        text=payload,
                        reply_markup=keyboard,
                        disable_web_page_preview=True,
                    )
                    return
                raise

    async def _render_status_page(
        self,
        *,
        selected_filter: StatusFilter,
        page: int,
    ) -> tuple[str, InlineKeyboardMarkup]:
        jobs = await self._list_jobs_callback()
        filtered_jobs = self._filter_jobs(jobs, selected_filter)
        if not filtered_jobs and selected_filter != STATUS_FILTER_ALL:
            selected_filter = STATUS_FILTER_ALL
            filtered_jobs = jobs

        total_items = len(filtered_jobs)
        page_count = max(1, (total_items + STATUS_PAGE_SIZE - 1) // STATUS_PAGE_SIZE)
        safe_page = min(max(0, page), page_count - 1)
        start = safe_page * STATUS_PAGE_SIZE
        window = filtered_jobs[start : start + STATUS_PAGE_SIZE]

        header = (
            "<b>Queue Status</b>\n"
            f"Filter: <b>{escape(selected_filter.title())}</b> | "
            f"Page: <b>{safe_page + 1}/{page_count}</b> | "
            f"Items: <b>{total_items}</b>"
        )
        if not window:
            payload = f"{header}\n\nNo jobs in this filter."
            return payload, self._status_keyboard(selected_filter, safe_page, page_count)

        bot_stats = self._build_bot_stats(jobs)
        bodies = [format_job_status(job, bot_stats=bot_stats) for job in window]
        payload = f"{header}\n\n" + "\n\n────────────────\n\n".join(bodies)
        return payload, self._status_keyboard(selected_filter, safe_page, page_count)

    @staticmethod
    def _parse_status_page(raw_page: str) -> int:
        try:
            return max(0, int(raw_page))
        except ValueError:
            return 0

    @staticmethod
    def _parse_status_filter(raw_filter: str) -> StatusFilter:
        if raw_filter in VALID_STATUS_FILTERS:
            return raw_filter
        return STATUS_FILTER_ACTIVE

    @staticmethod
    def _filter_jobs(jobs: list[JobRecord], selected_filter: StatusFilter) -> list[JobRecord]:
        if selected_filter == STATUS_FILTER_ALL:
            return jobs
        if selected_filter == STATUS_FILTER_QUEUED:
            return [job for job in jobs if job.phase.value == "queued"]
        if selected_filter == STATUS_FILTER_TRANSFERS:
            transfer_phases = {"downloading_seedr", "downloading_local", "uploading_telegram"}
            return [job for job in jobs if job.phase.value in transfer_phases]
        final_phases = {"completed", "failed", "canceled"}
        return [job for job in jobs if job.phase.value not in final_phases]

    @staticmethod
    def _status_keyboard(
        selected_filter: StatusFilter,
        page: int,
        page_count: int,
    ) -> InlineKeyboardMarkup:
        prev_page = max(0, page - 1)
        next_page = min(max(0, page_count - 1), page + 1)
        has_prev = page > 0
        has_next = page < (page_count - 1)
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Refresh",
                        callback_data=f"status:refresh:{selected_filter}:{page}",
                    ),
                    InlineKeyboardButton(
                        "Prev",
                        callback_data=f"status:prev:{selected_filter}:{prev_page}",
                    ),
                    InlineKeyboardButton(
                        "Next",
                        callback_data=f"status:next:{selected_filter}:{next_page}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "All" if selected_filter != STATUS_FILTER_ALL else "All ✓",
                        callback_data=f"status:setfilter:{selected_filter}:{page}:{STATUS_FILTER_ALL}",
                    ),
                    InlineKeyboardButton(
                        (
                            "Active"
                            if selected_filter != STATUS_FILTER_ACTIVE
                            else "Active ✓"
                        ),
                        callback_data=f"status:setfilter:{selected_filter}:{page}:{STATUS_FILTER_ACTIVE}",
                    ),
                    InlineKeyboardButton(
                        (
                            "Transfers"
                            if selected_filter != STATUS_FILTER_TRANSFERS
                            else "Transfers ✓"
                        ),
                        callback_data=f"status:setfilter:{selected_filter}:{page}:{STATUS_FILTER_TRANSFERS}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "Queued" if selected_filter != STATUS_FILTER_QUEUED else "Queued ✓",
                        callback_data=f"status:setfilter:{selected_filter}:{page}:{STATUS_FILTER_QUEUED}",
                    ),
                    InlineKeyboardButton(
                        (
                            f"Page {page + 1}/{max(1, page_count)}"
                            if has_prev or has_next
                            else "Single Page"
                        ),
                        callback_data=f"status:refresh:{selected_filter}:{page}",
                    ),
                ],
            ]
        )

    async def _seedr_auth(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        if not await self._ensure_admin(update):
            return
        state = await self._start_seedr_auth_callback()
        await update.effective_message.reply_text(
            text=(
                "<b>Authorize Seedr</b>\n"
                f"Open: {escape(state.verification_url)}\n"
                f"Code: <code>{escape(state.user_code)}</code>\n"
                "After approving the device, run /seedr_auth_done"
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def _seedr_auth_done(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        if not await self._ensure_admin(update):
            return
        account_name = await self._complete_seedr_auth_callback()
        await update.effective_message.reply_text(
            f"Seedr authenticated as {account_name}.",
            disable_web_page_preview=True,
        )

    async def _session_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        if not context.args:
            await update.effective_message.reply_text("Usage: /session_start <phone_number>")
            return
        try:
            state = await self._start_user_session_callback(context.args[0])
        except Exception as exc:  # noqa: BLE001
            await update.effective_message.reply_text(f"Session start failed: {exc}")
            return
        await update.effective_message.reply_text(
            text=(
                "Login code sent.\n"
                f"Phone: {escape(state.phone_number)}\n"
                "Reply with /session_code &lt;code&gt;."
            ),
            parse_mode=ParseMode.HTML,
        )

    async def _session_code(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        if not context.args:
            await update.effective_message.reply_text("Usage: /session_code <code>")
            return
        raw_code = "".join(context.args).strip()
        normalized_code = raw_code.replace(" ", "").replace("-", "")
        code = normalized_code or raw_code
        try:
            session = await self._submit_user_session_code_callback(code)
        except (
            TelegramPasswordRequiredError,
            TelegramCodeExpiredError,
            TelegramCodeInvalidError,
        ) as exc:
            await update.effective_message.reply_text(str(exc))
            return
        except Exception as exc:  # noqa: BLE001
            await update.effective_message.reply_text(f"Session login failed: {exc}")
            return
        await update.effective_message.reply_text(self._format_session_success(session))

    async def _session_password(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._ensure_admin(update):
            return
        if not context.args:
            await update.effective_message.reply_text("Usage: /session_password <password>")
            return
        try:
            session = await self._submit_user_session_password_callback(" ".join(context.args))
        except Exception as exc:  # noqa: BLE001
            await update.effective_message.reply_text(f"Password login failed: {exc}")
            return
        await update.effective_message.reply_text(self._format_session_success(session))

    async def _settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        if not await self._ensure_admin(update):
            return
        settings = await self._get_upload_settings_callback()
        await update.effective_message.reply_text(
            text=self._format_settings_text(settings),
            parse_mode=ParseMode.HTML,
            reply_markup=self._settings_keyboard(settings),
            disable_web_page_preview=True,
        )

    async def _handle_settings_callback(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        del context
        query = update.callback_query
        if query is None:
            return
        if query.message is None or query.message.chat_id != self._admin_chat_id:
            await query.answer("Admin only", show_alert=True)
            return
        data = query.data or ""
        parts = data.split(":")
        await query.answer()
        if len(parts) < 2:
            return
        action = parts[1]
        if action == "refresh":
            await self._refresh_settings_message(query)
            return
        if action == "media_toggle":
            current = await self._get_upload_settings_callback()
            target = (
                UploadMediaType.DOCUMENT
                if current.media_type == UploadMediaType.MEDIA
                else UploadMediaType.MEDIA
            )
            await self._update_upload_settings_callback(media_type=target)
            await self._refresh_settings_message(query)
            return
        if action == "parse_mode_toggle":
            current = await self._get_upload_settings_callback()
            target = (
                CaptionParseMode.MARKDOWN_V2
                if current.caption_parse_mode == CaptionParseMode.HTML
                else CaptionParseMode.HTML
            )
            await self._update_upload_settings_callback(caption_parse_mode=target)
            await self._refresh_settings_message(query)
            return
        if action == "media" and len(parts) >= 3:
            media_type = UploadMediaType(parts[2])
            await self._update_upload_settings_callback(media_type=media_type)
            await self._refresh_settings_message(query)
            return
        if action == "parse_mode" and len(parts) >= 3:
            parse_mode = CaptionParseMode(parts[2])
            await self._update_upload_settings_callback(caption_parse_mode=parse_mode)
            await self._refresh_settings_message(query)
            return
        if action == "caption_set":
            self._pending_settings_action[self._admin_chat_id] = SETTINGS_ACTION_CAPTION
            await query.message.reply_text(
                "Send your custom caption text now.\n"
                "Allowed placeholders: {filename}, {torrent_name}, {job_id}."
            )
            return
        if action == "caption_clear":
            await self._update_upload_settings_callback(caption_template=None)
            await self._refresh_settings_message(query)
            return
        if action == "thumb_set":
            self._pending_settings_action[self._admin_chat_id] = SETTINGS_ACTION_THUMBNAIL
            await query.message.reply_text("Send the thumbnail as a photo or image document now.")
            return
        if action == "thumb_clear":
            await self._update_upload_settings_callback(
                thumbnail_file_id=None,
                thumbnail_local_path=None,
            )
            await self._refresh_settings_message(query)
            return
        if action == "reset":
            await self._reset_upload_settings_callback()
            await self._refresh_settings_message(query)

    async def _handle_admin_settings_input(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        del context
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or chat.id != self._admin_chat_id:
            return
        pending = self._pending_settings_action.get(chat.id)
        if pending is None:
            return
        if pending == SETTINGS_ACTION_CAPTION:
            if not message.text:
                await message.reply_text("Send caption text as plain message.")
                return
            await self._update_upload_settings_callback(caption_template=message.text.strip())
            self._pending_settings_action.pop(chat.id, None)
            await message.reply_text("Custom caption saved.")
            return
        if pending == SETTINGS_ACTION_THUMBNAIL:
            image = None
            file_id = None
            suffix = ".jpg"
            if message.photo:
                image = message.photo[-1]
                file_id = image.file_id
            elif message.document and (message.document.mime_type or "").startswith("image/"):
                image = message.document
                file_id = image.file_id
                if image.file_name and "." in image.file_name:
                    suffix = Path(image.file_name).suffix or suffix
            if image is None or file_id is None:
                await message.reply_text("Send a photo or image document for thumbnail.")
                return
            telegram_file = await image.get_file()
            thumbnails_dir = Path("downloads") / "thumbnails"
            thumbnails_dir.mkdir(parents=True, exist_ok=True)
            local_path = thumbnails_dir / f"custom_thumbnail{suffix}"
            await telegram_file.download_to_drive(custom_path=str(local_path))
            await self._update_upload_settings_callback(
                thumbnail_file_id=file_id,
                thumbnail_local_path=str(local_path),
            )
            self._pending_settings_action.pop(chat.id, None)
            await message.reply_text("Custom thumbnail saved.")

    async def _refresh_settings_message(self, query) -> None:
        settings = await self._get_upload_settings_callback()
        await query.edit_message_text(
            text=self._format_settings_text(settings),
            parse_mode=ParseMode.HTML,
            reply_markup=self._settings_keyboard(settings),
            disable_web_page_preview=True,
        )

    async def _handle_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        _, raw_job_id = query.data.split(":", maxsplit=1)
        job = await self._cancel_callback(int(raw_job_id))
        jobs = await self._list_jobs_callback()
        bot_stats = self._build_bot_stats(jobs)
        attempts = 0
        while True:
            attempts += 1
            try:
                await query.edit_message_text(
                    text=format_job_status(job, bot_stats=bot_stats),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                break
            except RetryAfter as exc:
                wait_seconds = float(exc.retry_after)
                LOGGER.warning(
                    "Telegram flood control on cancel message edit. retry_after=%s",
                    wait_seconds,
                )
                if attempts >= 3:
                    raise
                await asyncio.sleep(wait_seconds)

    def _build_bot_stats(self, jobs: list[JobRecord]):
        aggregate_dl = sum(float(job.download_speed_bps or 0.0) for job in jobs)
        aggregate_ul = sum(float(job.upload_speed_bps or 0.0) for job in jobs)
        return collect_bot_stats(
            download_dir=self._status_download_dir,
            bot_start_time=self._bot_start_time,
            tasks_count=len(jobs),
            download_bps=aggregate_dl,
            upload_bps=aggregate_ul,
        )

    @staticmethod
    def _extract_magnet(text: str) -> str | None:
        match = MAGNET_PATTERN.search(text)
        return match.group(0) if match else None

    async def _ensure_admin(self, update: Update) -> bool:
        chat = update.effective_chat
        message = update.effective_message
        if chat is None or message is None:
            return False
        if chat.id == self._admin_chat_id:
            return True
        await message.reply_text("This command is only available in the configured admin chat.")
        return False

    @staticmethod
    def _format_session_success(session: TelegramUserSession) -> str:
        identity = (
            session.display_name
            or session.username
            or str(session.user_id or "unknown user")
        )
        return f"Telegram user session saved for {identity}."

    @staticmethod
    def _format_settings_text(settings: UploadSettings) -> str:
        caption = (
            escape(settings.caption_template)
            if settings.caption_template
            else "<i>Not set</i>"
        )
        thumb = (
            escape(settings.thumbnail_local_path)
            if settings.thumbnail_local_path
            else "<i>Not set</i>"
        )
        return (
            "<b>Upload Settings</b>\n"
            f"<b>Media Type:</b> {escape(settings.media_type.value)}\n"
            f"<b>Caption Mode:</b> {escape(settings.caption_parse_mode.value)}\n"
            f"<b>Custom Caption:</b> {caption}\n"
            f"<b>Thumbnail:</b> {thumb}"
        )

    @staticmethod
    def _settings_keyboard(settings: UploadSettings) -> InlineKeyboardMarkup:
        media_label = (
            "Media: Document" if settings.media_type == UploadMediaType.DOCUMENT else "Media: Media"
        )
        mode_label = (
            "Caption Mode: MarkdownV2"
            if settings.caption_parse_mode == CaptionParseMode.MARKDOWN_V2
            else "Caption Mode: HTML"
        )
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(media_label, callback_data="settings:media_toggle"),
                    InlineKeyboardButton(mode_label, callback_data="settings:parse_mode_toggle"),
                ],
                [
                    InlineKeyboardButton("Set Caption", callback_data="settings:caption_set"),
                    InlineKeyboardButton("Clear Caption", callback_data="settings:caption_clear"),
                ],
                [
                    InlineKeyboardButton("Set Thumbnail", callback_data="settings:thumb_set"),
                    InlineKeyboardButton("Clear Thumbnail", callback_data="settings:thumb_clear"),
                ],
                [
                    InlineKeyboardButton("Reset Defaults", callback_data="settings:reset"),
                    InlineKeyboardButton("Refresh", callback_data="settings:refresh"),
                ],
            ]
        )
