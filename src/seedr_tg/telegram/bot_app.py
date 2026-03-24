from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import re
import time
from collections.abc import Awaitable, Callable
from html import escape
from typing import Literal

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatType
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
    UserSettings,
)
from seedr_tg.status.template import collect_bot_stats
from seedr_tg.status.unified import ActiveTaskSnapshot
from seedr_tg.telegram.uploader import (
    TelegramCodeExpiredError,
    TelegramCodeInvalidError,
    TelegramPasswordRequiredError,
)
from seedr_tg.worker.progress import format_job_status

LOGGER = logging.getLogger(__name__)
MAGNET_PATTERN = re.compile(r"magnet:\?[^\s]+", re.IGNORECASE)
TASK_CANCEL_PATTERN = re.compile(r"^(direct|rename):-?\d+:\d+$", re.IGNORECASE)
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
        enqueue_callback: Callable[
            [str, int, int, int | None, str | None, str | None],
            Awaitable[JobRecord | None],
        ],
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
        get_user_settings_callback: Callable[[int], Awaitable[UserSettings]],
        update_user_settings_callback: Callable[..., Awaitable[UserSettings]],
        get_authorized_chat_ids_callback: Callable[[], Awaitable[set[int]]],
        authorize_chat_callback: Callable[[int], Awaitable[set[int]]],
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
        self._get_user_settings_callback = get_user_settings_callback
        self._update_user_settings_callback = update_user_settings_callback
        self._get_authorized_chat_ids_callback = get_authorized_chat_ids_callback
        self._authorize_chat_callback = authorize_chat_callback
        self._status_download_dir = status_download_dir
        self._bot_start_time = float(bot_start_time)
        self._authorized_chat_ids: set[int] = {source_chat_id, admin_chat_id}
        self._authorized_chat_lock = asyncio.Lock()
        self._admin_message_cache: dict[tuple[int, int], tuple[str, int | None]] = {}
        self._admin_message_locks: dict[tuple[int, int], asyncio.Lock] = {}
        self._pending_settings_action: dict[int, str] = {}
        self._pending_user_settings_action: dict[int, str] = {}
        self._status_locks: dict[tuple[int, int], asyncio.Lock] = {}
        self._queue_status_message_ids: dict[int, int] = {}
        self._queue_status_filters: dict[int, StatusFilter] = {}
        self._queue_status_pages: dict[int, int] = {}
        self._queue_status_locks: dict[int, asyncio.Lock] = {}
        self._queue_status_last_payloads: dict[int, str | None] = {}
        self._queue_status_last_update_ats: dict[int, float] = {}
        self._queue_status_cooldown_untils: dict[int, float] = {}
        self._queue_status_last_flood_log_ats: dict[int, float] = {}
        self._active_tasks: dict[str, ActiveTaskSnapshot] = {}
        self._task_cancel_flags: dict[str, bool] = {}
        self._active_tasks_lock = asyncio.Lock()
        self._application = (
            Application.builder()
            .token(token)
            .concurrent_updates(True)
            .build()
        )
        self._application.add_handler(CommandHandler("status", self._status))
        self._application.add_handler(CommandHandler("seedr_auth", self._seedr_auth))
        self._application.add_handler(CommandHandler("seedr_auth_done", self._seedr_auth_done))
        self._application.add_handler(CommandHandler("session_start", self._session_start))
        self._application.add_handler(CommandHandler("session_code", self._session_code))
        self._application.add_handler(CommandHandler("session_password", self._session_password))
        self._application.add_handler(CommandHandler("authorize", self._authorize_group_chat))
        self._application.add_handler(CommandHandler("direct", direct_download_handler))
        self._application.add_handler(CommandHandler("rename", telegram_media_rename_handler))
        self._application.add_handler(CommandHandler("cancel", self._cancel_command))
        self._application.add_handler(CommandHandler("settings", self._settings))
        self._application.add_handler(CommandHandler("mysettings", self._mysettings))
        self._application.add_handler(
            CallbackQueryHandler(self._handle_settings_callback, pattern=r"^settings:")
        )
        self._application.add_handler(
            CallbackQueryHandler(self._handle_mysettings_callback, pattern=r"^mysettings:")
        )
        self._application.add_handler(
            CallbackQueryHandler(self._handle_cancel, pattern=r"^cancel:.+$")
        )
        self._application.add_handler(
            CallbackQueryHandler(self._handle_status_callback, pattern=r"^status:")
        )
        self._application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._on_message)
        )
        self._application.add_handler(
            MessageHandler(
                filters.Chat(chat_id=admin_chat_id)
                & ~filters.COMMAND
                & (filters.TEXT | filters.PHOTO | filters.Document.IMAGE),
                self._handle_admin_settings_input,
            )
        )
        self._application.add_handler(
            MessageHandler(
                ~filters.COMMAND & (filters.TEXT | filters.PHOTO | filters.Document.IMAGE),
                self._handle_user_settings_input,
            )
        )

    async def start(self) -> None:
        await self._refresh_authorized_chat_ids()
        await self._application.initialize()
        await self._application.start()
        await self._application.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    async def stop(self) -> None:
        updater = self._application.updater
        if updater is not None:
            await updater.stop()
        await self._application.stop()
        await self._application.shutdown()

    async def post_admin_message(
        self,
        text: str,
        job_id: int | None = None,
        chat_id: int | None = None,
    ) -> int:
        target_chat_id = self._admin_chat_id if chat_id is None else int(chat_id)
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
                    chat_id=target_chat_id,
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
        self._admin_message_cache[(target_chat_id, message.message_id)] = (text, job_id)
        return message.message_id

    async def update_admin_message(
        self,
        message_id: int,
        text: str,
        job_id: int | None = None,
        chat_id: int | None = None,
    ) -> None:
        target_chat_id = self._admin_chat_id if chat_id is None else int(chat_id)
        cache_key = (target_chat_id, message_id)
        lock = self._admin_message_locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            cached = self._admin_message_cache.get(cache_key)
            if cached == (text, job_id):
                return
            reply_markup = None
            if job_id is not None:
                reply_markup = InlineKeyboardMarkup.from_button(
                    InlineKeyboardButton(text="Cancel current", callback_data=f"cancel:{job_id}")
                )
            try:
                await self._application.bot.edit_message_text(
                    chat_id=target_chat_id,
                    message_id=message_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                )
                self._admin_message_cache[cache_key] = (text, job_id)
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
                    self._admin_message_cache[cache_key] = (text, job_id)
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
        if not await self.is_chat_authorized(chat.id):
            await message.reply_text(
                "This chat is not authorized. A group admin can run /authorize first."
            )
            return
        user = update.effective_user
        job = await self._enqueue_callback(
            magnet,
            chat.id,
            message.message_id,
            user.id if user is not None else None,
            user.username if user is not None else None,
            user.full_name if user is not None else None,
        )
        if job is None:
            await self.post_admin_message("<b>Ignored duplicate magnet</b>")
            return
        # Queue runner owns lifecycle status messages to avoid duplicate posts from race conditions.
        await asyncio.sleep(0)

    async def _status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        if chat is None:
            return
        if not await self.is_chat_authorized(chat.id):
            await update.effective_message.reply_text(
                "This chat is not authorized. A group admin can run /authorize first."
            )
            return
        await self.upsert_queue_status_panel(chat_id=chat.id, force_create=True)

    async def _handle_status_callback(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        del context
        query = update.callback_query
        if query is None:
            return
        if query.message is None:
            return
        chat_id = int(query.message.chat_id)
        if not await self.is_chat_authorized(chat_id):
            await query.answer("Unauthorized chat", show_alert=True)
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

        message_id = int(query.message.message_id)
        lock = self._status_locks.setdefault((chat_id, message_id), asyncio.Lock())
        async with lock:
            payload, keyboard, normalized_filter, normalized_page = await self._render_status_page(
                chat_id=chat_id,
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
            self._queue_status_message_ids[chat_id] = message_id
            self._queue_status_filters[chat_id] = normalized_filter
            self._queue_status_pages[chat_id] = normalized_page
            self._queue_status_last_payloads[chat_id] = payload
            self._queue_status_last_update_ats[chat_id] = time.monotonic()

    async def _render_status_page(
        self,
        *,
        chat_id: int,
        selected_filter: StatusFilter,
        page: int,
    ) -> tuple[str, InlineKeyboardMarkup, StatusFilter, int]:
        jobs = await self._list_jobs_callback()
        async with self._active_tasks_lock:
            active_tasks = list(self._active_tasks.values())
        filtered_entries = self._filter_entries(jobs, active_tasks, selected_filter, chat_id)
        if not filtered_entries and selected_filter != STATUS_FILTER_ALL:
            selected_filter = STATUS_FILTER_ALL
            filtered_entries = self._filter_entries(jobs, active_tasks, selected_filter, chat_id)

        total_items = len(filtered_entries)
        page_count = max(1, (total_items + STATUS_PAGE_SIZE - 1) // STATUS_PAGE_SIZE)
        safe_page = min(max(0, page), page_count - 1)
        start = safe_page * STATUS_PAGE_SIZE
        window = filtered_entries[start : start + STATUS_PAGE_SIZE]

        header = (
            "<b>Queue Status</b>\n"
            f"Filter: <b>{escape(selected_filter.title())}</b> | "
            f"Page: <b>{safe_page + 1}/{page_count}</b> | "
            f"Items: <b>{total_items}</b>"
        )
        if not window:
            payload = f"{header}\n\nNo jobs in this filter."
            return (
                payload,
                self._status_keyboard(selected_filter, safe_page, page_count),
                selected_filter,
                safe_page,
            )

        visible_jobs = [job for job in jobs if int(job.source_chat_id) == int(chat_id)]
        visible_tasks = [
            task
            for task in active_tasks
            if self._task_chat_id(task.task_id) == int(chat_id)
        ]
        bot_stats = self._build_bot_stats(visible_jobs, visible_tasks)
        bodies = [self._render_status_entry(entry) for entry in window]
        payload = f"{header}\n\n" + "\n\n────────────────\n\n".join(bodies)
        payload += "\n" + self._render_bot_stats_footer(bot_stats)
        return (
            payload,
            self._status_keyboard(selected_filter, safe_page, page_count),
            selected_filter,
            safe_page,
        )

    async def upsert_queue_status_panel(self, *, chat_id: int, force_create: bool = False) -> None:
        normalized_chat_id = int(chat_id)
        queue_lock = self._queue_status_locks.setdefault(normalized_chat_id, asyncio.Lock())
        async with queue_lock:
            jobs = await self._list_jobs_callback()
            async with self._active_tasks_lock:
                active_tasks = list(self._active_tasks.values())
            visible_jobs = [job for job in jobs if int(job.source_chat_id) == normalized_chat_id]
            visible_tasks = [
                task
                for task in active_tasks
                if self._task_chat_id(task.task_id) == normalized_chat_id
            ]
            if not force_create and self._all_tasks_completed(visible_jobs, visible_tasks):
                await self._delete_queue_status_message_locked(chat_id=normalized_chat_id)
                return

            selected_filter = self._queue_status_filters.get(normalized_chat_id, STATUS_FILTER_ACTIVE)
            selected_page = self._queue_status_pages.get(normalized_chat_id, 0)
            payload, keyboard, selected_filter, safe_page = await self._render_status_page(
                chat_id=normalized_chat_id,
                selected_filter=selected_filter,
                page=selected_page,
            )
            self._queue_status_filters[normalized_chat_id] = selected_filter
            self._queue_status_pages[normalized_chat_id] = safe_page
            now = time.monotonic()

            message_id = self._queue_status_message_ids.get(normalized_chat_id)
            last_payload = self._queue_status_last_payloads.get(normalized_chat_id)
            last_update_at = self._queue_status_last_update_ats.get(normalized_chat_id, 0.0)
            cooldown_until = self._queue_status_cooldown_untils.get(normalized_chat_id, 0.0)
            last_flood_log_at = self._queue_status_last_flood_log_ats.get(normalized_chat_id, 0.0)
            if (
                message_id is not None
                and payload == last_payload
                and not force_create
            ):
                return
            if message_id is not None and now < cooldown_until:
                return
            if (
                message_id is not None
                and not force_create
                and (now - last_update_at) < 2.0
            ):
                return
            if force_create and message_id is None:
                posted_id = await self.post_admin_message(payload, chat_id=normalized_chat_id)
                self._queue_status_message_ids[normalized_chat_id] = posted_id
                self._queue_status_last_payloads[normalized_chat_id] = payload
                self._queue_status_last_update_ats[normalized_chat_id] = time.monotonic()
                try:
                    await self._application.bot.edit_message_reply_markup(
                        chat_id=normalized_chat_id,
                        message_id=posted_id,
                        reply_markup=keyboard,
                    )
                except Exception:  # noqa: BLE001
                    with contextlib.suppress(Exception):
                        await self._application.bot.edit_message_text(
                            chat_id=normalized_chat_id,
                            message_id=posted_id,
                            text=payload,
                            parse_mode=ParseMode.HTML,
                            reply_markup=keyboard,
                            disable_web_page_preview=True,
                        )
                return

            if message_id is None:
                posted_id = await self.post_admin_message(payload, chat_id=normalized_chat_id)
                self._queue_status_message_ids[normalized_chat_id] = posted_id
                self._queue_status_last_payloads[normalized_chat_id] = payload
                self._queue_status_last_update_ats[normalized_chat_id] = time.monotonic()
                with contextlib.suppress(Exception):
                    await self._application.bot.edit_message_text(
                        chat_id=normalized_chat_id,
                        message_id=posted_id,
                        text=payload,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard,
                        disable_web_page_preview=True,
                    )
                return

            try:
                await self._application.bot.edit_message_text(
                    chat_id=normalized_chat_id,
                    message_id=message_id,
                    text=payload,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                    disable_web_page_preview=True,
                )
                self._queue_status_last_payloads[normalized_chat_id] = payload
                self._queue_status_last_update_ats[normalized_chat_id] = time.monotonic()
            except RetryAfter as exc:
                wait_seconds = max(1.0, float(exc.retry_after) * 1.08)
                self._queue_status_cooldown_untils[normalized_chat_id] = (
                    time.monotonic() + wait_seconds
                )
                if (time.monotonic() - last_flood_log_at) >= 15.0:
                    LOGGER.warning(
                        "Queue status panel rate-limited; cooling down %.2fs",
                        wait_seconds,
                    )
                    self._queue_status_last_flood_log_ats[normalized_chat_id] = time.monotonic()
                return
            except BadRequest as exc:
                text = str(exc).lower()
                if "message is not modified" in text:
                    self._queue_status_last_payloads[normalized_chat_id] = payload
                    self._queue_status_last_update_ats[normalized_chat_id] = time.monotonic()
                    return
                if "message to edit not found" in text:
                    posted_id = await self.post_admin_message(payload, chat_id=normalized_chat_id)
                    self._queue_status_message_ids[normalized_chat_id] = posted_id
                    self._queue_status_last_payloads[normalized_chat_id] = payload
                    self._queue_status_last_update_ats[normalized_chat_id] = time.monotonic()
                    with contextlib.suppress(Exception):
                        await self._application.bot.edit_message_text(
                            chat_id=normalized_chat_id,
                            message_id=posted_id,
                            text=payload,
                            parse_mode=ParseMode.HTML,
                            reply_markup=keyboard,
                            disable_web_page_preview=True,
                        )
                    return
                if "can't parse entities" in text:
                    await self._application.bot.edit_message_text(
                        chat_id=normalized_chat_id,
                        message_id=message_id,
                        text=payload,
                        reply_markup=keyboard,
                        disable_web_page_preview=True,
                    )
                    self._queue_status_last_payloads[normalized_chat_id] = payload
                    self._queue_status_last_update_ats[normalized_chat_id] = time.monotonic()
                    return
                raise

    async def _delete_queue_status_message_locked(self, *, chat_id: int) -> None:
        normalized_chat_id = int(chat_id)
        message_id = self._queue_status_message_ids.pop(normalized_chat_id, None)
        self._queue_status_last_payloads.pop(normalized_chat_id, None)
        self._queue_status_last_update_ats.pop(normalized_chat_id, None)
        self._queue_status_cooldown_untils.pop(normalized_chat_id, None)
        self._queue_status_last_flood_log_ats.pop(normalized_chat_id, None)
        self._queue_status_pages.pop(normalized_chat_id, None)
        self._queue_status_filters.pop(normalized_chat_id, None)
        if message_id is None:
            return
        try:
            await self._application.bot.delete_message(
                chat_id=normalized_chat_id,
                message_id=message_id,
            )
        except BadRequest as exc:
            if "message to delete not found" in str(exc).lower():
                return
            raise

    @staticmethod
    def _all_tasks_completed(
        jobs: list[JobRecord],
        tasks: list[ActiveTaskSnapshot],
    ) -> bool:
        final_phases = {"completed", "failed", "canceled"}
        if any(task.phase not in final_phases for task in tasks):
            return False
        return all(job.phase.value in final_phases for job in jobs)

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
    def _filter_entries(
        jobs: list[JobRecord],
        tasks: list[ActiveTaskSnapshot],
        selected_filter: StatusFilter,
        chat_id: int,
    ) -> list[JobRecord | ActiveTaskSnapshot]:
        filtered_jobs = [job for job in jobs if int(job.source_chat_id) == int(chat_id)]
        filtered_tasks = [
            task
            for task in tasks
            if TelegramBotApp._task_chat_id(task.task_id) == int(chat_id)
        ]
        entries: list[JobRecord | ActiveTaskSnapshot] = [*filtered_jobs, *filtered_tasks]
        if selected_filter == STATUS_FILTER_ALL:
            return entries
        if selected_filter == STATUS_FILTER_QUEUED:
            queued_jobs = [job for job in filtered_jobs if job.phase.value == "queued"]
            queued_tasks = [task for task in filtered_tasks if task.phase == "queued"]
            return [*queued_jobs, *queued_tasks]
        if selected_filter == STATUS_FILTER_TRANSFERS:
            transfer_phases = {"downloading_seedr", "downloading_local", "uploading_telegram"}
            transfer_jobs = [job for job in filtered_jobs if job.phase.value in transfer_phases]
            transfer_tasks = [
                task for task in filtered_tasks if task.phase in {"running", "queued"}
            ]
            return [*transfer_jobs, *transfer_tasks]
        final_phases = {"completed", "failed", "canceled"}
        active_jobs = [job for job in filtered_jobs if job.phase.value not in final_phases]
        active_tasks = [task for task in filtered_tasks if task.phase not in final_phases]
        return [*active_jobs, *active_tasks]

    @staticmethod
    def _render_status_entry(entry: JobRecord | ActiveTaskSnapshot) -> str:
        if isinstance(entry, JobRecord):
            return format_job_status(entry)
        from seedr_tg.status.template import render_active_task_status

        return render_active_task_status(entry)

    @staticmethod
    def _render_bot_stats_footer(bot_stats) -> str:
        from seedr_tg.status.template import render_bot_stats_footer

        return render_bot_stats_footer(bot_stats)

    def _build_bot_stats(
        self,
        jobs: list[JobRecord],
        tasks: list[ActiveTaskSnapshot] | None = None,
    ):
        tasks = tasks or []
        aggregate_dl = sum(float(job.download_speed_bps or 0.0) for job in jobs)
        aggregate_ul = sum(float(job.upload_speed_bps or 0.0) for job in jobs)
        for task in tasks:
            speed = float(task.speed_bps or 0.0)
            if speed <= 0:
                continue
            status = (task.status_text or "").lower()
            if "upload" in status:
                aggregate_ul += speed
            else:
                aggregate_dl += speed
        return collect_bot_stats(
            download_dir=self._status_download_dir,
            bot_start_time=self._bot_start_time,
            tasks_count=len(jobs) + len(tasks),
            download_bps=aggregate_dl,
            upload_bps=aggregate_ul,
        )

    async def register_active_task(self, task: ActiveTaskSnapshot) -> None:
        async with self._active_tasks_lock:
            self._active_tasks[task.task_id] = task
            self._task_cancel_flags[task.task_id] = False
        chat_id = self._task_chat_id(task.task_id)
        if chat_id is None:
            return
        await self.upsert_queue_status_panel(chat_id=chat_id, force_create=False)

    async def update_active_task(self, task: ActiveTaskSnapshot) -> None:
        async with self._active_tasks_lock:
            if task.task_id in self._active_tasks:
                self._active_tasks[task.task_id] = task
        chat_id = self._task_chat_id(task.task_id)
        if chat_id is None:
            return
        await self.upsert_queue_status_panel(chat_id=chat_id, force_create=False)

    async def unregister_active_task(self, task_id: str) -> None:
        async with self._active_tasks_lock:
            self._active_tasks.pop(task_id, None)
            self._task_cancel_flags.pop(task_id, None)
        chat_id = self._task_chat_id(task_id)
        if chat_id is None:
            return
        await self.upsert_queue_status_panel(chat_id=chat_id, force_create=False)

    async def request_task_cancel(self, task_id: str) -> bool:
        normalized = str(task_id).strip()
        async with self._active_tasks_lock:
            if normalized not in self._active_tasks:
                return False
            self._task_cancel_flags[normalized] = True
        chat_id = self._task_chat_id(normalized)
        if chat_id is not None:
            await self.upsert_queue_status_panel(chat_id=chat_id, force_create=False)
        return True

    async def is_task_cancel_requested(self, task_id: str) -> bool:
        normalized = str(task_id).strip()
        async with self._active_tasks_lock:
            return bool(self._task_cancel_flags.get(normalized, False))

    @staticmethod
    def _task_chat_id(task_id: str) -> int | None:
        parts = str(task_id).split(":")
        if len(parts) < 3:
            return None
        try:
            return int(parts[1])
        except ValueError:
            return None

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

    async def _authorize_group_chat(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        del context
        message = update.effective_message
        chat = update.effective_chat
        user = update.effective_user
        if message is None or chat is None or user is None:
            return
        if chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
            await message.reply_text("Use /authorize inside a group chat.")
            return

        try:
            bot_user = await self._application.bot.get_me()
            bot_member = await self._application.bot.get_chat_member(chat.id, bot_user.id)
            requester_member = await self._application.bot.get_chat_member(chat.id, user.id)
        except BadRequest as exc:
            await message.reply_text(f"Authorization check failed: {exc}")
            return

        if not self._is_admin_member_status(str(bot_member.status)):
            await message.reply_text(
                "I must be an admin in this group before authorization can be enabled."
            )
            return

        if not self._is_admin_member_status(str(requester_member.status)):
            await message.reply_text("Only a group admin can run /authorize.")
            return

        authorized_ids = await self._authorize_chat_callback(chat.id)
        async with self._authorized_chat_lock:
            self._authorized_chat_ids = {
                self._source_chat_id,
                self._admin_chat_id,
                *authorized_ids,
            }
        await message.reply_text(
            "Group authorized. Any user in this group can now use bot commands here."
        )

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
                thumbnail_base64=None,
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
            if message.photo:
                image = message.photo[-1]
                file_id = image.file_id
            elif message.document and (message.document.mime_type or "").startswith("image/"):
                image = message.document
                file_id = image.file_id
            if image is None or file_id is None:
                await message.reply_text("Send a photo or image document for thumbnail.")
                return
            telegram_file = await image.get_file()
            thumbnail_bytes = await telegram_file.download_as_bytearray()
            thumbnail_base64 = base64.b64encode(bytes(thumbnail_bytes)).decode("ascii")
            
            await self._update_upload_settings_callback(
                thumbnail_file_id=file_id,
                thumbnail_base64=thumbnail_base64,
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
        _, raw_target = query.data.split(":", maxsplit=1)
        raw_target = raw_target.strip()
        if self._is_task_cancel_target(raw_target):
            canceled = await self.request_task_cancel(raw_target)
            if canceled:
                await query.edit_message_text(
                    f"Cancellation requested for task <code>{escape(raw_target)}</code>.",
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            else:
                await query.edit_message_text("Task not found or already finished.")
            return
        if not raw_target.isdigit():
            await query.edit_message_text("Invalid cancel target.")
            return

        job = await self._cancel_callback(int(raw_target))
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

    async def _cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None:
            return
        if not await self.is_chat_authorized(chat.id):
            await message.reply_text("This chat is not authorized.")
            return
        if not context.args:
            await message.reply_text("Usage: /cancel <job_id|task_id>")
            return

        raw_target = str(context.args[0]).strip()
        if self._is_task_cancel_target(raw_target):
            canceled = await self.request_task_cancel(raw_target)
            if canceled:
                await message.reply_text(f"Cancellation requested for task {raw_target}.")
            else:
                await message.reply_text("Task not found or already finished.")
            return
        if raw_target.isdigit():
            try:
                await self._cancel_callback(int(raw_target))
            except LookupError:
                await message.reply_text(f"Job {raw_target} not found.")
                return
            await message.reply_text(f"Cancellation requested for job {raw_target}.")
            return
        await message.reply_text("Invalid cancel target. Use numeric job id or direct:/rename task id.")

    @staticmethod
    def _is_task_cancel_target(raw_target: str) -> bool:
        return bool(TASK_CANCEL_PATTERN.fullmatch(str(raw_target).strip()))

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

    async def _refresh_authorized_chat_ids(self) -> None:
        authorized_ids = await self._get_authorized_chat_ids_callback()
        async with self._authorized_chat_lock:
            self._authorized_chat_ids = {
                self._source_chat_id,
                self._admin_chat_id,
                *authorized_ids,
            }

    async def is_chat_authorized(self, chat_id: int) -> bool:
        async with self._authorized_chat_lock:
            return int(chat_id) in self._authorized_chat_ids

    @staticmethod
    def _is_admin_member_status(status: str) -> bool:
        normalized_status = str(status).lower().strip()
        return normalized_status in {"administrator", "creator"}

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
            "Set (Custom)"
            if settings.thumbnail_base64 or settings.thumbnail_file_id
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

    async def _mysettings(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        if user is None:
            return
        settings = await self._get_user_settings_callback(user.id)
        await update.message.reply_text(
            text=self._format_user_settings_text(settings),
            parse_mode=ParseMode.HTML,
            reply_markup=self._user_settings_keyboard(),
        )

    async def _handle_mysettings_callback(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        if query is None or query.data is None:
            return
        user_id = query.from_user.id
        await query.answer()

        action = query.data.split(":", 1)[1]
        
        if action == "caption_set":
            self._pending_user_settings_action[user_id] = SETTINGS_ACTION_CAPTION
            await query.message.reply_text(
                "Send the custom caption text. Available placeholders:\n"
                "{filename}, {torrent_name}, {job_id}"
            )
            return
        if action == "caption_clear":
            await self._update_user_settings_callback(user_id=user_id, caption_template=None)
            await self._refresh_mysettings_message(query, user_id)
            return

        if action == "thumb_set":
            self._pending_user_settings_action[user_id] = SETTINGS_ACTION_THUMBNAIL
            await query.message.reply_text("Send the thumbnail as a photo or image document now.")
            return
        if action == "thumb_clear":
            await self._update_user_settings_callback(
                user_id=user_id,
                thumbnail_file_id=None,
                thumbnail_base64=None,
            )
            await self._refresh_mysettings_message(query, user_id)
            return
        if action == "refresh":
            await self._refresh_mysettings_message(query, user_id)
            return

    async def _handle_user_settings_input(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        message = update.effective_message
        user = update.effective_user
        if message is None or user is None:
            return
        pending = self._pending_user_settings_action.get(user.id)
        if pending is None:
            return

        if pending == SETTINGS_ACTION_CAPTION:
            if not message.text:
                await message.reply_text("Send caption text as plain message.")
                return
            await self._update_user_settings_callback(user_id=user.id, caption_template=message.text.strip())
            self._pending_user_settings_action.pop(user.id, None)
            await message.reply_text("Your custom caption saved.")
            return

        if pending == SETTINGS_ACTION_THUMBNAIL:
            image = None
            file_id = None
            if message.photo:
                image = message.photo[-1]
                file_id = image.file_id
            elif message.document and (message.document.mime_type or "").startswith("image/"):
                image = message.document
                file_id = image.file_id
            
            if image is None or file_id is None:
                await message.reply_text("Send a photo or image document for thumbnail.")
                return

            telegram_file = await image.get_file()
            thumbnail_bytes = await telegram_file.download_as_bytearray()
            thumbnail_base64 = base64.b64encode(bytes(thumbnail_bytes)).decode("ascii")
            
            await self._update_user_settings_callback(
                user_id=user.id,
                thumbnail_file_id=file_id,
                thumbnail_base64=thumbnail_base64,
            )
            self._pending_user_settings_action.pop(user.id, None)
            await message.reply_text("Your custom thumbnail saved.")

    async def _refresh_mysettings_message(self, query, user_id: int) -> None:
        settings = await self._get_user_settings_callback(user_id)
        await query.edit_message_text(
            text=self._format_user_settings_text(settings),
            parse_mode=ParseMode.HTML,
            reply_markup=self._user_settings_keyboard(),
        )

    @staticmethod
    def _format_user_settings_text(settings: UserSettings) -> str:
        caption_disp = (
            escape(settings.caption_template)
            if settings.caption_template
            else "<i>Not set (Using Global)</i>"
        )
        thumb_disp = (
            "Set (Custom)"
            if settings.thumbnail_base64 or settings.thumbnail_file_id
            else "<i>Not set (Using Global)</i>"
        )
        return (
            "<b>Your User Settings</b>\n\n"
            f"<b>Caption:</b>\n{caption_disp}\n\n"
            f"<b>Thumbnail:</b> {thumb_disp}\n"
        )

    @staticmethod
    def _user_settings_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Set Caption", callback_data="mysettings:caption_set"),
                    InlineKeyboardButton("Clear Caption", callback_data="mysettings:caption_clear"),
                ],
                [
                    InlineKeyboardButton("Set Thumbnail", callback_data="mysettings:thumb_set"),
                    InlineKeyboardButton("Clear Thumbnail", callback_data="mysettings:thumb_clear"),
                ],
                [
                    InlineKeyboardButton("Refresh", callback_data="mysettings:refresh"),
                ],
            ]
        )
