from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from seedr_tg.db.models import JobRecord
from seedr_tg.worker.progress import format_job_status

LOGGER = logging.getLogger(__name__)
MAGNET_PATTERN = re.compile(r"magnet:\?[^\s]+", re.IGNORECASE)


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
    ) -> None:
        self._source_chat_id = source_chat_id
        self._admin_chat_id = admin_chat_id
        self._enqueue_callback = enqueue_callback
        self._list_jobs_callback = list_jobs_callback
        self._cancel_callback = cancel_callback
        self._set_admin_message_id_callback = set_admin_message_id_callback
        self._application = Application.builder().token(token).build()
        self._application.add_handler(CommandHandler("status", self._status))
        self._application.add_handler(
            CallbackQueryHandler(self._handle_cancel, pattern=r"^cancel:\d+$")
        )
        self._application.add_handler(
            MessageHandler(filters.Chat(chat_id=source_chat_id) & filters.TEXT, self._on_message)
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
        message = await self._application.bot.send_message(
            chat_id=self._admin_chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
        return message.message_id

    async def update_admin_message(
        self,
        message_id: int,
        text: str,
        job_id: int | None = None,
    ) -> None:
        reply_markup = None
        if job_id is not None:
            reply_markup = InlineKeyboardMarkup.from_button(
                InlineKeyboardButton(text="Cancel current", callback_data=f"cancel:{job_id}")
            )
        await self._application.bot.edit_message_text(
            chat_id=self._admin_chat_id,
            message_id=message_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )

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
        text = format_job_status(job)
        admin_message_id = await self.post_admin_message(text, job.id)
        await self._set_admin_message_id_callback(job.id, admin_message_id)
        await asyncio.sleep(0)

    async def _status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        jobs = await self._list_jobs_callback()
        if not jobs:
            await update.effective_message.reply_text("Queue is empty.")
            return
        payload = "\n\n".join(format_job_status(job) for job in jobs[:5])
        await update.effective_message.reply_text(payload, parse_mode=ParseMode.HTML)

    async def _handle_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        _, raw_job_id = query.data.split(":", maxsplit=1)
        job = await self._cancel_callback(int(raw_job_id))
        await query.edit_message_text(
            text=format_job_status(job),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    @staticmethod
    def _extract_magnet(text: str) -> str | None:
        match = MAGNET_PATTERN.search(text)
        return match.group(0) if match else None
