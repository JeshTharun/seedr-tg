from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import PhoneCodeExpiredError, PhoneCodeInvalidError, SessionPasswordNeededError
from telethon.sessions import StringSession

from seedr_tg.db.models import TelegramLoginState, TelegramUserSession
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
        progress_hook: Callable[[int, int, str], Awaitable[None]] | None = None,
    ) -> None:
        client = await self._get_client()
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

            await client.send_file(
                entity=self._target_chat_id,
                file=str(file_path),
                caption=f"{caption_prefix}\n{file_path.name}",
                supports_streaming=True,
                progress_callback=on_progress,
            )
            LOGGER.info("Uploaded %s to Telegram", file_path)

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
