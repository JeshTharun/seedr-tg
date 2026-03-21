from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Awaitable, Callable
from html import escape
from pathlib import Path
from typing import Any

from telegram import Bot, InputFile
from telegram.constants import ParseMode
from telegram.error import NetworkError, RetryAfter, TelegramError, TimedOut
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    RPCError,
    SessionPasswordNeededError,
)
from telethon.network.connection.tcpabridged import ConnectionTcpAbridged
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
    _UPLOAD_RETRY_BASE_DELAY_SECONDS = 1.0
    _UPLOAD_RETRY_MAX_DELAY_SECONDS = 30.0
    _UPLOAD_GOVERNOR_ENABLED = True
    _UPLOAD_GOVERNOR_MIN_CONCURRENCY = 1
    _UPLOAD_GOVERNOR_SCALE_UP_AFTER_STABLE_FILES = 6
    _PREMIUM_UPLOAD_THRESHOLD_BYTES = 2 * 1024 * 1024 * 1024
    _BOT_CONNECT_TIMEOUT_SECONDS = 30.0
    _BOT_POOL_TIMEOUT_SECONDS = 30.0
    _BOT_WRITE_TIMEOUT_SECONDS = 900.0
    _BOT_READ_TIMEOUT_SECONDS = 900.0

    @staticmethod
    def _create_client(session_string: str, api_id: int, api_hash: str) -> TelegramClient:
        # Abridged transport reduces MTProto framing overhead vs TcpFull.
        return TelegramClient(
            StringSession(session_string),
            api_id,
            api_hash,
            connection=ConnectionTcpAbridged,
        )

    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        bot_token: str,
        target_chat_id: int,
        repository: JobRepository,
        bootstrap_session_string: str | None = None,
        upload_retry_base_delay_seconds: float | None = None,
        upload_retry_max_delay_seconds: float | None = None,
        upload_governor_enabled: bool | None = None,
        upload_governor_min_concurrency: int | None = None,
        upload_governor_scale_up_after_stable_files: int | None = None,
    ) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._bot_token = bot_token
        self._target_chat_id = target_chat_id
        self._repository = repository
        self._bootstrap_session_string = bootstrap_session_string
        self._upload_retry_base_delay_seconds = (
            float(upload_retry_base_delay_seconds)
            if upload_retry_base_delay_seconds is not None
            else self._UPLOAD_RETRY_BASE_DELAY_SECONDS
        )
        self._upload_retry_max_delay_seconds = (
            float(upload_retry_max_delay_seconds)
            if upload_retry_max_delay_seconds is not None
            else self._UPLOAD_RETRY_MAX_DELAY_SECONDS
        )
        self._upload_governor_enabled = (
            bool(upload_governor_enabled)
            if upload_governor_enabled is not None
            else self._UPLOAD_GOVERNOR_ENABLED
        )
        self._upload_governor_min_concurrency = max(
            1,
            int(upload_governor_min_concurrency)
            if upload_governor_min_concurrency is not None
            else self._UPLOAD_GOVERNOR_MIN_CONCURRENCY,
        )
        self._upload_governor_scale_up_after_stable_files = max(
            1,
            int(upload_governor_scale_up_after_stable_files)
            if upload_governor_scale_up_after_stable_files is not None
            else self._UPLOAD_GOVERNOR_SCALE_UP_AFTER_STABLE_FILES,
        )
        self._client: TelegramClient | None = None
        self._bot: Bot | None = None
        self._governor_lock = asyncio.Lock()
        self._adaptive_upload_cap: int | None = None
        self._stable_upload_streak = 0

    async def start(self) -> None:
        if self._bot is None:
            self._bot = Bot(self._bot_token)
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
            with contextlib.suppress(Exception):
                await self._client.disconnect()
            self._client = None
        if self._bot is not None:
            try:
                await self._bot.close()
            except RetryAfter as exc:
                LOGGER.warning(
                    "Skipping bot.close() due to Telegram flood wait during shutdown: "
                    "retry_after=%s",
                    exc.retry_after,
                )
            except (TelegramError, NetworkError, TimedOut) as exc:
                LOGGER.warning("Skipping bot.close() due to shutdown error: %s", exc)
            self._bot = None

    async def begin_login(self, phone_number: str) -> TelegramLoginState:
        client = self._create_client("", self._api_id, self._api_hash)
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
        client = self._create_client(state.session_string, self._api_id, self._api_hash)
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
        client = self._create_client(state.session_string, self._api_id, self._api_hash)
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
        upload_max_retries: int = 4,
    ) -> None:
        total_files = len(file_paths)
        requested_upload_concurrency = max(1, int(max_concurrent_uploads))
        min_upload_concurrency = self._upload_governor_min_concurrency
        effective_upload_concurrency = await self._determine_effective_upload_concurrency(
            requested_upload_concurrency=requested_upload_concurrency,
            upload_governor_enabled=self._upload_governor_enabled,
            upload_governor_min_concurrency=min_upload_concurrency,
        )
        semaphore = asyncio.Semaphore(effective_upload_concurrency)
        progress_lock = asyncio.Lock()
        completed_files = 0

        async def upload_one(file_path: Path) -> None:
            nonlocal completed_files
            last_emit = 0.0
            progress_emit_task: asyncio.Task[None] | None = None

            def on_progress(
                current: int,
                total: int,
                *,
                current_name: str = file_path.name,
            ) -> None:
                nonlocal last_emit, progress_emit_task
                if progress_hook is None:
                    return
                now = time.monotonic()
                if current < total and (now - last_emit) < 1.0:
                    return
                last_emit = now
                if (
                    progress_emit_task is not None
                    and not progress_emit_task.done()
                    and current < total
                ):
                    return

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

                progress_emit_task = asyncio.create_task(emit())
                progress_emit_task.add_done_callback(self._handle_progress_emit_done)

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
                file_size_bytes = file_path.stat().st_size if file_path.exists() else 0
                use_premium_session = file_size_bytes > self._PREMIUM_UPLOAD_THRESHOLD_BYTES
                if use_premium_session:
                    client = await self._get_client()
                    had_flood_wait, retry_count = await self._send_file_with_retry(
                        client,
                        kwargs,
                        upload_max_retries=upload_max_retries,
                    )
                else:
                    had_flood_wait, retry_count = await self._send_file_via_bot_with_retry(
                        file_path=file_path,
                        caption=caption,
                        parse_mode=parse_mode,
                        media_type=(
                            upload_settings.media_type if upload_settings else UploadMediaType.MEDIA
                        ),
                        thumb_path=thumb_path,
                        upload_max_retries=upload_max_retries,
                        progress_hook=progress_hook,
                        completed_files=completed_files,
                        total_files=total_files,
                    )
                if self._upload_governor_enabled:
                    await self._record_upload_outcome(
                        requested_upload_concurrency=requested_upload_concurrency,
                        upload_governor_min_concurrency=min_upload_concurrency,
                        upload_governor_scale_up_after_stable_files=self._upload_governor_scale_up_after_stable_files,
                        had_flood_wait=had_flood_wait,
                        retry_count=retry_count,
                    )

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
            if progress_emit_task is not None and not progress_emit_task.done():
                with contextlib.suppress(asyncio.CancelledError):
                    await progress_emit_task
            LOGGER.info("Uploaded %s to Telegram", file_path)

        tasks = [asyncio.create_task(upload_one(file_path)) for file_path in file_paths]
        try:
            await asyncio.gather(*tasks)
        except Exception:  # noqa: BLE001
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _send_file_with_retry(
        self,
        client: TelegramClient,
        kwargs: dict[str, Any],
        *,
        upload_max_retries: int,
    ) -> tuple[bool, int]:
        max_attempts = max(1, int(upload_max_retries))
        had_flood_wait = False
        active_client = client
        for attempt in range(1, max_attempts + 1):
            try:
                await active_client.send_file(**kwargs)
                return had_flood_wait, attempt - 1
            except FloodWaitError as exc:
                had_flood_wait = True
                wait_seconds = max(1, int(exc.seconds))
                LOGGER.warning("Telegram flood wait while uploading; sleeping %ss", wait_seconds)
                await asyncio.sleep(wait_seconds)
                if attempt >= max_attempts:
                    raise
            except (RPCError, OSError, TimeoutError) as exc:
                if not self._is_retryable_upload_error(exc) or attempt >= max_attempts:
                    raise
                if isinstance(exc, OSError | TimeoutError):
                    active_client = await self._reset_telethon_client_for_retry(active_client, exc)
                backoff = min(
                    self._upload_retry_base_delay_seconds * (2 ** (attempt - 1)),
                    self._upload_retry_max_delay_seconds,
                )
                jitter = random.uniform(0.0, self._upload_retry_base_delay_seconds)
                delay = backoff + jitter
                LOGGER.warning(
                    "Retrying Telegram upload (attempt %s/%s) in %.2fs due to %s",
                    attempt,
                    max_attempts,
                    delay,
                    repr(exc),
                )
                await asyncio.sleep(delay)
        return had_flood_wait, max_attempts - 1

    async def _send_file_via_bot_with_retry(
        self,
        *,
        file_path: Path,
        caption: str,
        parse_mode: str | None,
        media_type: UploadMediaType,
        thumb_path: Path | None,
        upload_max_retries: int,
        progress_hook: Callable[[int, int, str, int, int], Awaitable[None]] | None,
        completed_files: int,
        total_files: int,
    ) -> tuple[bool, int]:
        max_attempts = max(1, int(upload_max_retries))
        had_flood_wait = False
        total_bytes = file_path.stat().st_size if file_path.exists() else 0
        bot_parse_mode = self._resolve_bot_parse_mode(parse_mode)
        for attempt in range(1, max_attempts + 1):
            bot = self._bot
            if bot is None:
                self._bot = Bot(self._bot_token)
                bot = self._bot
            if bot is None:
                raise RuntimeError("Telegram bot client is not initialized")
            try:
                with file_path.open("rb") as handle:
                    upload_input = InputFile(
                        handle,
                        filename=file_path.name,
                        read_file_handle=False,
                    )
                    if media_type == UploadMediaType.DOCUMENT:
                        if thumb_path is not None and thumb_path.exists():
                            with thumb_path.open("rb") as thumb_handle:
                                thumb_input = InputFile(
                                    thumb_handle,
                                    filename=thumb_path.name,
                                    read_file_handle=False,
                                )
                                await bot.send_document(
                                    chat_id=self._target_chat_id,
                                    document=upload_input,
                                    caption=caption,
                                    parse_mode=bot_parse_mode,
                                    thumbnail=thumb_input,
                                    connect_timeout=self._BOT_CONNECT_TIMEOUT_SECONDS,
                                    pool_timeout=self._BOT_POOL_TIMEOUT_SECONDS,
                                    write_timeout=self._BOT_WRITE_TIMEOUT_SECONDS,
                                    read_timeout=self._BOT_READ_TIMEOUT_SECONDS,
                                )
                        else:
                            await bot.send_document(
                                chat_id=self._target_chat_id,
                                document=upload_input,
                                caption=caption,
                                parse_mode=bot_parse_mode,
                                connect_timeout=self._BOT_CONNECT_TIMEOUT_SECONDS,
                                pool_timeout=self._BOT_POOL_TIMEOUT_SECONDS,
                                write_timeout=self._BOT_WRITE_TIMEOUT_SECONDS,
                                read_timeout=self._BOT_READ_TIMEOUT_SECONDS,
                            )
                    elif thumb_path is not None and thumb_path.exists():
                        with thumb_path.open("rb") as thumb_handle:
                            thumb_input = InputFile(
                                thumb_handle,
                                filename=thumb_path.name,
                                read_file_handle=False,
                            )
                            await bot.send_video(
                                chat_id=self._target_chat_id,
                                video=upload_input,
                                caption=caption,
                                parse_mode=bot_parse_mode,
                                supports_streaming=True,
                                thumbnail=thumb_input,
                                connect_timeout=self._BOT_CONNECT_TIMEOUT_SECONDS,
                                pool_timeout=self._BOT_POOL_TIMEOUT_SECONDS,
                                write_timeout=self._BOT_WRITE_TIMEOUT_SECONDS,
                                read_timeout=self._BOT_READ_TIMEOUT_SECONDS,
                            )
                    else:
                        await bot.send_video(
                            chat_id=self._target_chat_id,
                            video=upload_input,
                            caption=caption,
                            parse_mode=bot_parse_mode,
                            supports_streaming=True,
                            connect_timeout=self._BOT_CONNECT_TIMEOUT_SECONDS,
                            pool_timeout=self._BOT_POOL_TIMEOUT_SECONDS,
                            write_timeout=self._BOT_WRITE_TIMEOUT_SECONDS,
                            read_timeout=self._BOT_READ_TIMEOUT_SECONDS,
                        )

                if progress_hook is not None:
                    await progress_hook(
                        completed_files,
                        total_files,
                        f"{file_path.name} {total_bytes}/{total_bytes}",
                        int(total_bytes),
                        int(total_bytes),
                    )
                return had_flood_wait, attempt - 1
            except RetryAfter as exc:
                had_flood_wait = True
                wait_seconds = max(1, int(exc.retry_after))
                LOGGER.warning("Bot API flood wait while uploading; sleeping %ss", wait_seconds)
                await asyncio.sleep(wait_seconds)
                if attempt >= max_attempts:
                    raise
            except (TimedOut, NetworkError, TelegramError, OSError, TimeoutError) as exc:
                if attempt >= max_attempts:
                    raise
                if isinstance(exc, TimedOut | NetworkError | TimeoutError | OSError):
                    await self._reset_bot_client_for_retry(exc)
                backoff = min(
                    self._upload_retry_base_delay_seconds * (2 ** (attempt - 1)),
                    self._upload_retry_max_delay_seconds,
                )
                jitter = random.uniform(0.0, self._upload_retry_base_delay_seconds)
                delay = backoff + jitter
                LOGGER.warning(
                    "Retrying bot upload (attempt %s/%s) in %.2fs due to %s",
                    attempt,
                    max_attempts,
                    delay,
                    repr(exc),
                )
                await asyncio.sleep(delay)
        return had_flood_wait, max_attempts - 1

    async def _reset_bot_client_for_retry(self, exc: BaseException) -> None:
        old_bot = self._bot
        self._bot = Bot(self._bot_token)
        if old_bot is None:
            return
        LOGGER.warning("Resetting bot client after upload network error: %s", repr(exc))
        with contextlib.suppress(Exception):
            await asyncio.wait_for(old_bot.close(), timeout=10.0)

    async def _reset_telethon_client_for_retry(
        self,
        client: TelegramClient,
        exc: BaseException,
    ) -> TelegramClient:
        LOGGER.warning("Resetting Telethon client after upload network error: %s", repr(exc))
        with contextlib.suppress(Exception):
            await client.disconnect()
        with contextlib.suppress(Exception):
            await client.connect()
            if await client.is_user_authorized():
                return client
        return await self._get_client()

    async def _determine_effective_upload_concurrency(
        self,
        *,
        requested_upload_concurrency: int,
        upload_governor_enabled: bool,
        upload_governor_min_concurrency: int,
    ) -> int:
        if not upload_governor_enabled:
            return requested_upload_concurrency
        async with self._governor_lock:
            if self._adaptive_upload_cap is None:
                self._adaptive_upload_cap = requested_upload_concurrency
            self._adaptive_upload_cap = max(
                upload_governor_min_concurrency,
                min(self._adaptive_upload_cap, requested_upload_concurrency),
            )
            effective = self._adaptive_upload_cap
        if effective < requested_upload_concurrency:
            LOGGER.info(
                "Upload governor limiting concurrency to %s (requested=%s)",
                effective,
                requested_upload_concurrency,
            )
        return effective

    async def _record_upload_outcome(
        self,
        *,
        requested_upload_concurrency: int,
        upload_governor_min_concurrency: int,
        upload_governor_scale_up_after_stable_files: int,
        had_flood_wait: bool,
        retry_count: int,
    ) -> None:
        async with self._governor_lock:
            current_cap = self._adaptive_upload_cap or requested_upload_concurrency
            if had_flood_wait:
                new_cap = max(upload_governor_min_concurrency, current_cap - 1)
                self._adaptive_upload_cap = new_cap
                self._stable_upload_streak = 0
                if new_cap < current_cap:
                    LOGGER.warning(
                        "Upload governor reduced concurrency to %s after flood wait",
                        new_cap,
                    )
                return

            if retry_count > 0:
                self._stable_upload_streak = 0
                return

            self._stable_upload_streak += 1
            stable_target = max(1, int(upload_governor_scale_up_after_stable_files))
            if (
                self._stable_upload_streak >= stable_target
                and current_cap < requested_upload_concurrency
            ):
                self._adaptive_upload_cap = current_cap + 1
                self._stable_upload_streak = 0
                LOGGER.info(
                    "Upload governor increased concurrency to %s after stable uploads",
                    self._adaptive_upload_cap,
                )

    @staticmethod
    def _is_retryable_upload_error(exc: BaseException) -> bool:
        if isinstance(exc, FloodWaitError):
            return True
        name = exc.__class__.__name__.lower()
        return any(token in name for token in ("timeout", "connection", "rpc", "server"))

    @staticmethod
    def _resolve_bot_parse_mode(parse_mode: str | None) -> str | None:
        if parse_mode is None:
            return None
        normalized = parse_mode.strip().lower()
        if normalized == "html":
            return ParseMode.HTML
        if normalized in {"md", "markdown", "markdownv2", "markdown_v2"}:
            return ParseMode.MARKDOWN_V2
        return None

    @staticmethod
    def _handle_progress_emit_done(task: asyncio.Task[None]) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            exc = task.exception()
            if exc is not None:
                LOGGER.warning("Upload progress callback failed: %s", exc)

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
        client = self._create_client(session_string, self._api_id, self._api_hash)
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
