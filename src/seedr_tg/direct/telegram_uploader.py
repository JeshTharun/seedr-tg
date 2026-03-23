from __future__ import annotations

from pathlib import Path

from telegram import Bot, InputFile
from telegram.error import TelegramError


class DirectTelegramUploadError(RuntimeError):
    """Raised when Telegram file upload fails."""


class DirectTelegramUploader:
    """Uploads local files to Telegram chat via Bot API."""

    async def upload_file(self, *, bot: Bot, chat_id: int, file_path: Path) -> None:
        try:
            with file_path.open("rb") as handle:
                await bot.send_document(
                    chat_id=chat_id,
                    document=InputFile(handle, filename=file_path.name, read_file_handle=False),
                )
        except TelegramError as exc:
            raise DirectTelegramUploadError(str(exc)) from exc
