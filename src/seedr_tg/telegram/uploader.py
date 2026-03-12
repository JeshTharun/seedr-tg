from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path

from telethon import TelegramClient

LOGGER = logging.getLogger(__name__)


class TelegramUploader:
    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        phone_number: str,
        session_name: str,
        target_chat_id: int,
    ) -> None:
        self._phone_number = phone_number
        self._target_chat_id = target_chat_id
        self._client = TelegramClient(session_name, api_id, api_hash)

    async def start(self) -> None:
        await self._client.start(phone=self._phone_number)

    async def stop(self) -> None:
        await self._client.disconnect()

    async def upload_files(
        self,
        file_paths: list[Path],
        *,
        caption_prefix: str,
        progress_hook: Callable[[int, int, str], Awaitable[None]] | None = None,
    ) -> None:
        total_files = len(file_paths)
        for index, file_path in enumerate(file_paths, start=1):
            def on_progress(
                current: int,
                total: int,
                *,
                current_index: int = index,
                current_name: str = file_path.name,
            ) -> None:
                if progress_hook is not None:
                    asyncio.create_task(
                        progress_hook(
                            current_index,
                            total_files,
                            f"{current_name} {current}/{total}",
                        )
                    )

            await self._client.send_file(
                entity=self._target_chat_id,
                file=str(file_path),
                caption=f"{caption_prefix}\n{file_path.name}",
                supports_streaming=True,
                progress_callback=on_progress,
            )
            LOGGER.info("Uploaded %s to Telegram", file_path)
