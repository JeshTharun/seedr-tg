from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from html import escape
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import PhoneCodeExpiredError, PhoneCodeInvalidError, SessionPasswordNeededError
from telethon.sessions import StringSession

from seedr_tg.db.models import (
    CaptionParseMode,
    TelegramLoginState,
    TelegramUserSession,
    UploadMediaType,
    UploadSettings,
)
from seedr_tg.db.repository import JobRepository

LOGGER = logging.getLogger(__name__)


class TelegramPasswordRequiredError(RuntimeError):
    pass


class TelegramCodeExpiredError(RuntimeError):
    pass


class TelegramCodeInvalidError(RuntimeError):
    pass


class TelegramUploader:
    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        target_chat_id: int,
        repository: JobRepository,
        bootstrap_session_string: str | None = None,
    ) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._target_chat_id = target_chat_id
        self._repository = repository
        self._bootstrap_session_string = bootstrap_session_string
        self._client: TelegramClient | None = None

    async def start(self) -> None:
        if (
            self._bootstrap_session_string
            and await self._repository.get_telegram_user_session() is None
        ):
            client = await self._connect_client(self._bootstrap_session_string)
            try:
                await self._persist_authorized_session(client, phone_number=None)
            finally:
                await client.disconnect()
        stored_session = await self._repository.get_telegram_user_session()
        if stored_session is not None:
            self._client = await self._connect_client(stored_session.session_string)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.disconnect()
            self._client = None

    async def begin_login(self, phone_number: str) -> TelegramLoginState:
        client = TelegramClient(StringSession(), self._api_id, self._api_hash)
        await client.connect()
        try:
            sent = await client.send_code_request(phone_number)
            login_state = await self._repository.save_telegram_login_state(
                phone_number=phone_number,
                phone_code_hash=sent.phone_code_hash,
                session_string=client.session.save(),
                password_required=False,
            )
        finally:
            await client.disconnect()
        return login_state

    async def complete_login_with_code(self, code: str) -> TelegramUserSession:
        state = await self._repository.get_telegram_login_state()
        if state is None:
            raise RuntimeError("No pending Telegram login. Run /session_start <phone> first.")
        client = TelegramClient(StringSession(state.session_string), self._api_id, self._api_hash)
        await client.connect()
        try:
            await client.sign_in(
                phone=state.phone_number,
                code=code,
                phone_code_hash=state.phone_code_hash,
            )
        except SessionPasswordNeededError as exc:
            await self._repository.save_telegram_login_state(
                phone_number=state.phone_number,
                phone_code_hash=state.phone_code_hash,
                session_string=client.session.save(),
                password_required=True,
            )
            raise TelegramPasswordRequiredError(
                "Telegram account requires 2FA password. Run /session_password <password>."
            ) from exc
        except PhoneCodeExpiredError as exc:
            await self._repository.clear_telegram_login_state()
            await client.disconnect()
            raise TelegramCodeExpiredError(
                "Login code expired. Run /session_start <phone_number> and try /session_code again."
            ) from exc
        except PhoneCodeInvalidError as exc:
            await client.disconnect()
            raise TelegramCodeInvalidError(
                "Invalid login code. Run /session_code with the exact code Telegram sent."
            ) from exc
        except Exception:
            await client.disconnect()
            raise
        return await self._promote_client_session(client, state.phone_number)

    async def complete_login_with_password(self, password: str) -> TelegramUserSession:
        state = await self._repository.get_telegram_login_state()
        if state is None:
            raise RuntimeError("No pending Telegram login. Run /session_start <phone> first.")
        if not state.password_required:
            raise RuntimeError("Current Telegram login does not require a password.")
        client = TelegramClient(StringSession(state.session_string), self._api_id, self._api_hash)
        await client.connect()
        try:
            await client.sign_in(password=password)
        except Exception:
            await client.disconnect()
            raise
        return await self._promote_client_session(client, state.phone_number)

    async def upload_files(
        self,
        file_paths: list[Path],
        *,
        caption_prefix: str,
        job_id: int | None = None,
        upload_settings: UploadSettings | None = None,
        progress_hook: Callable[[int, int, str, int, int], Awaitable[None]] | None = None,
        max_concurrent_uploads: int = 1,
        upload_part_size_kb: int = 512,
    ) -> None:
        client = await self._get_client()
        total_files = len(file_paths)
        semaphore = asyncio.Semaphore(max(1, int(max_concurrent_uploads)))
        progress_lock = asyncio.Lock()
        completed_files = 0

        async def upload_one(file_path: Path) -> None:
            nonlocal completed_files
            last_emit = 0.0

            def on_progress(
                current: int,
                total: int,
                *,
                current_name: str = file_path.name,
            ) -> None:
                nonlocal last_emit
                if progress_hook is None:
                    return
                now = time.monotonic()
                if current < total and (now - last_emit) < 1.0:
                    return
                last_emit = now

                async def emit() -> None:
                    async with progress_lock:
                        done = completed_files
                    await progress_hook(
                        done,
                        total_files,
                        f"{current_name} {current}/{total}",
                        int(current),
                        int(total),
                    )

                asyncio.create_task(emit())

            caption, parse_mode = self._render_caption(
                file_path=file_path,
                caption_prefix=caption_prefix,
                job_id=job_id,
                upload_settings=upload_settings,
            )
            thumb_path = self._resolve_thumbnail_path(upload_settings)
            kwargs = {
                "entity": self._target_chat_id,
                "file": str(file_path),
                "caption": caption,
                "supports_streaming": True,
                "progress_callback": on_progress,
                "part_size_kb": int(upload_part_size_kb),
            }
            if parse_mode is not None:
                kwargs["parse_mode"] = parse_mode
            if upload_settings and upload_settings.media_type == UploadMediaType.DOCUMENT:
                kwargs["force_document"] = True
            if thumb_path is not None:
                kwargs["thumb"] = str(thumb_path)

            async with semaphore:
                await client.send_file(**kwargs)

            async with progress_lock:
                completed_files += 1
                done = completed_files
            if progress_hook is not None:
                await progress_hook(
                    done,
                    total_files,
                    f"Uploaded {file_path.name}",
                    1,
                    1,
                )
            LOGGER.info("Uploaded %s to Telegram", file_path)

        tasks = [asyncio.create_task(upload_one(file_path)) for file_path in file_paths]
        await asyncio.gather(*tasks)

    @staticmethod
    def _resolve_thumbnail_path(upload_settings: UploadSettings | None) -> Path | None:
        if upload_settings is None or not upload_settings.thumbnail_local_path:
            return None
        path = Path(upload_settings.thumbnail_local_path)
        if not path.exists():
            LOGGER.warning("Configured thumbnail not found at %s; sending without thumbnail", path)
            return None
        return path

    @staticmethod
    def _render_caption(
        *,
        file_path: Path,
        caption_prefix: str,
        job_id: int | None,
        upload_settings: UploadSettings | None,
    ) -> tuple[str, str | None]:
        filename = file_path.name
        if upload_settings is None or not upload_settings.caption_template:
            return f"{caption_prefix}\n{filename}", None
        template = upload_settings.caption_template
        include_filename_prefix = "{filename}" not in template
        template_filename = filename
        template_torrent_name = caption_prefix
        if upload_settings.caption_parse_mode == CaptionParseMode.HTML:
            template_filename = escape(filename)
            template_torrent_name = escape(caption_prefix)
        try:
            rendered = template.format(
                filename=template_filename,
                torrent_name=template_torrent_name,
                job_id=job_id if job_id is not None else "",
            )
        except (KeyError, ValueError) as exc:
            LOGGER.warning("Invalid caption template, using fallback caption: %s", exc)
            return f"{caption_prefix}\n{filename}", None
        parse_mode = "html"
        if upload_settings.caption_parse_mode == CaptionParseMode.MARKDOWN_V2:
            parse_mode = "md"
        if upload_settings.caption_parse_mode == CaptionParseMode.HTML:
            if include_filename_prefix:
                safe_filename = escape(filename)
                return f"{safe_filename}\n{rendered}", parse_mode
            return rendered, parse_mode
        if include_filename_prefix:
            return f"{filename}\n{rendered}", parse_mode
        return rendered, parse_mode

    async def _get_client(self) -> TelegramClient:
        if self._client is not None and self._client.is_connected():
            return self._client
        stored_session = await self._repository.get_telegram_user_session()
        if stored_session is None:
            raise RuntimeError(
                "Telegram uploader session is not configured. "
                "Run /session_start <phone> in the admin chat."
            )
        self._client = await self._connect_client(stored_session.session_string)
        return self._client

    async def _connect_client(self, session_string: str) -> TelegramClient:
        client = TelegramClient(StringSession(session_string), self._api_id, self._api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError("Stored Telegram user session is no longer authorized.")
        return client

    async def _promote_client_session(
        self,
        client: TelegramClient,
        phone_number: str | None,
    ) -> TelegramUserSession:
        session = await self._persist_authorized_session(client, phone_number=phone_number)
        await self._repository.clear_telegram_login_state()
        if self._client is not None:
            await self._client.disconnect()
        self._client = client
        return session

    async def _persist_authorized_session(
        self,
        client: TelegramClient,
        *,
        phone_number: str | None,
    ) -> TelegramUserSession:
        me = await client.get_me()
        session_string = client.session.save()
        display_name = None
        if me is not None:
            parts = [getattr(me, "first_name", None), getattr(me, "last_name", None)]
            display_name = " ".join(part for part in parts if part) or getattr(me, "username", None)
        return await self._repository.save_telegram_user_session(
            session_string=session_string,
            phone_number=phone_number,
            user_id=getattr(me, "id", None),
            username=getattr(me, "username", None),
            display_name=display_name,
        )
