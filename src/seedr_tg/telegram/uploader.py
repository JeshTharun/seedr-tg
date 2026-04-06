from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import math
import random
import re
import shutil
import tempfile
import time
from collections.abc import Awaitable, Callable
from html import escape
from pathlib import Path
from typing import Any

from pyrogram import Client
from pyrogram.enums import ParseMode as PyrogramParseMode
from pyrogram.errors import (
    FloodWait,
    PeerIdInvalid,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    RPCError,
    SessionPasswordNeeded,
)
from telegram import Bot, InputFile
from telegram.request import HTTPXRequest
from telegram.constants import ParseMode as BotParseMode
from telegram.error import NetworkError, RetryAfter, TelegramError, TimedOut

from seedr_tg.db.models import (
    CaptionParseMode,
    TelegramLoginState,
    TelegramUserSession,
    UploadMediaType,
    UploadSettings,
    UserSettings,
)
from seedr_tg.db.repository import JobRepository
from seedr_tg.status.template import (
    format_speed_bps,
    get_progress_bar_string,
    readable_size,
    readable_time,
)

LOGGER = logging.getLogger(__name__)
_TELEGRAM_FILENAME_MAX_CHARS = 60
_INVALID_FILENAME_CHARS = re.compile(r"[\\/:*?\"<>|\x00-\x1f]")
_LEADING_1TAMILMV_PREFIX = re.compile(
    r"^\s*(?:www\.)?1tamilmv\.[a-z0-9.-]+\s*[-_]+\s*",
    re.IGNORECASE,
)
_CAPTION_PLACEHOLDER_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
try:
    from pyrogram.errors import FloodPremiumWait

    FLOOD_WAIT_ERRORS = (FloodWait, FloodPremiumWait)
except Exception:  # noqa: BLE001
    FLOOD_WAIT_ERRORS = (FloodWait,)


class TelegramPasswordRequiredError(RuntimeError):
    pass


class TelegramCodeExpiredError(RuntimeError):
    pass


class TelegramCodeInvalidError(RuntimeError):
    pass


class TelegramUploadTooLargeError(RuntimeError):
    pass


class TelegramUploader:
    _UPLOAD_RETRY_BASE_DELAY_SECONDS = 1.0
    _UPLOAD_RETRY_MAX_DELAY_SECONDS = 30.0
    _UPLOAD_GOVERNOR_ENABLED = True
    _UPLOAD_GOVERNOR_MIN_CONCURRENCY = 1
    _UPLOAD_GOVERNOR_SCALE_UP_AFTER_STABLE_FILES = 6
    _BOT_API_FILE_SIZE_LIMIT_BYTES = 2000 * 1024 * 1024
    _MTPROTO_STANDARD_FILE_SIZE_LIMIT_BYTES = 2000 * 1024 * 1024
    _MTPROTO_PREMIUM_FILE_SIZE_LIMIT_BYTES = 4000 * 1024 * 1024
    _BOT_CONNECT_TIMEOUT_SECONDS = 30.0
    _BOT_POOL_TIMEOUT_SECONDS = 30.0
    _BOT_WRITE_TIMEOUT_SECONDS = 900.0
    _BOT_READ_TIMEOUT_SECONDS = 900.0
    _MTD_DOWNLOAD_TIMEOUT_SECONDS = 1800.0
    _MTPROTO_UPLOAD_FILE_SIZE_LIMIT_BYTES = 4000 * 1024 * 1024
    _UPLOAD_SPLIT_ENABLED = True
    _UPLOAD_SPLIT_SIZE_BYTES = 1900 * 1024 * 1024
    _UPLOAD_SPLIT_USE_FFMPEG_FOR_VIDEO = True
    _UPLOAD_SPLIT_FFMPEG_BINARY = "ffmpeg"
    _UPLOAD_SPLIT_FFPROBE_BINARY = "ffprobe"
    _UPLOAD_HYBRID_MODE = True
    _FLOOD_WAIT_SAFETY_MULTIPLIER = 1.08
    _UPLOAD_PROGRESS_EMIT_MIN_INTERVAL_SECONDS = 2.0
    _VIDEO_EXTENSIONS = {
        ".mp4",
        ".mkv",
        ".mov",
        ".webm",
        ".avi",
        ".m4v",
        ".ts",
        ".m2ts",
    }
    _SPLIT_IO_CHUNK_BYTES = 8 * 1024 * 1024

    def _create_client(
        self,
        session_string: str | None,
        *,
        name: str,
        in_memory: bool = True,
    ) -> Client:
        kwargs: dict[str, Any] = {
            "name": name,
            "api_id": self._api_id,
            "api_hash": self._api_hash,
            "in_memory": in_memory,
            "no_updates": True,
            "sleep_threshold": 0,
        }
        if session_string:
            kwargs["session_string"] = session_string
        return Client(**kwargs)

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
        upload_hybrid_mode: bool | None = None,
        upload_split_enabled: bool | None = None,
        upload_split_size_bytes: int | None = None,
        upload_split_use_ffmpeg_for_video: bool | None = None,
        upload_split_ffmpeg_binary: str | None = None,
        upload_split_ffprobe_binary: str | None = None,
    ) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._bot_token = bot_token
        self._target_chat_id = target_chat_id
        self._repository = repository
        self._bootstrap_session_string = bootstrap_session_string
        self._bot_user_id = self._parse_bot_user_id(bot_token)
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
        self._upload_hybrid_mode = (
            bool(upload_hybrid_mode)
            if upload_hybrid_mode is not None
            else self._UPLOAD_HYBRID_MODE
        )
        self._upload_split_enabled = (
            bool(upload_split_enabled)
            if upload_split_enabled is not None
            else self._UPLOAD_SPLIT_ENABLED
        )
        self._upload_split_size_bytes = max(
            64 * 1024 * 1024,
            int(upload_split_size_bytes)
            if upload_split_size_bytes is not None
            else self._UPLOAD_SPLIT_SIZE_BYTES,
        )
        self._upload_split_use_ffmpeg_for_video = (
            bool(upload_split_use_ffmpeg_for_video)
            if upload_split_use_ffmpeg_for_video is not None
            else self._UPLOAD_SPLIT_USE_FFMPEG_FOR_VIDEO
        )
        self._upload_split_ffmpeg_binary = (
            str(upload_split_ffmpeg_binary).strip()
            if upload_split_ffmpeg_binary
            else self._UPLOAD_SPLIT_FFMPEG_BINARY
        )
        self._upload_split_ffprobe_binary = (
            str(upload_split_ffprobe_binary).strip()
            if upload_split_ffprobe_binary
            else self._UPLOAD_SPLIT_FFPROBE_BINARY
        )
        self._client: Client | None = None
        self._bot_mtproto_client: Client | None = None
        self._pending_login_client: Client | None = None
        self._pending_login_phone_number: str | None = None
        self._bot: Bot | None = None
        self._governor_lock = asyncio.Lock()
        self._mtproto_download_lock = asyncio.Lock()
        self._adaptive_upload_cap: int | None = None
        self._stable_upload_streak = 0
        self._user_is_premium: bool | None = None

    @staticmethod
    def _parse_bot_user_id(bot_token: str) -> int | None:
        token_prefix, _, _ = bot_token.partition(":")
        try:
            parsed = int(token_prefix)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    def resolve_mtproto_chat_id(self, *, bot_chat_id: int, is_private_chat: bool) -> int:
        """Resolve a Bot API chat id to a peer id usable by MTProto user sessions."""
        if self._bot_user_id is None:
            return bot_chat_id
        # Bot API private chats expose the human user's positive id, but MTProto
        # user sessions need to access bot PM media via the bot peer id.
        if is_private_chat or bot_chat_id > 0:
            return self._bot_user_id
        return bot_chat_id

    @property
    def mtproto_upload_file_size_limit_bytes(self) -> int:
        return int(self._MTPROTO_UPLOAD_FILE_SIZE_LIMIT_BYTES)

    @property
    def mtproto_premium_file_size_limit_bytes(self) -> int:
        return int(self._MTPROTO_PREMIUM_FILE_SIZE_LIMIT_BYTES)

    def _create_bot(self) -> Bot:
        request = HTTPXRequest(
            connection_pool_size=8,
            connect_timeout=self._BOT_CONNECT_TIMEOUT_SECONDS,
            pool_timeout=self._BOT_POOL_TIMEOUT_SECONDS,
            read_timeout=self._BOT_READ_TIMEOUT_SECONDS,
            write_timeout=self._BOT_WRITE_TIMEOUT_SECONDS,
            media_write_timeout=self._BOT_WRITE_TIMEOUT_SECONDS,
        )
        return Bot(self._bot_token, request=request)

    async def start(self) -> None:
        if self._bot is None:
            self._bot = self._create_bot()
        if (
            self._bootstrap_session_string
            and await self._repository.get_telegram_user_session() is None
        ):
            try:
                client = await self._connect_client(self._bootstrap_session_string)
                try:
                    await self._persist_authorized_session(client, phone_number=None)
                finally:
                    with contextlib.suppress(Exception):
                        await client.disconnect()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "Bootstrap MTProto session is not compatible with Kurigram. "
                    "Run /session_start <phone> to create a new session. details=%s",
                    exc,
                )
        stored_session = await self._repository.get_telegram_user_session()
        if stored_session is not None:
            try:
                self._client = await self._connect_client(stored_session.session_string)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "Stored MTProto session is not valid for Kurigram uploader. "
                    "Run /session_start <phone> to recreate it. details=%s",
                    exc,
                )
                self._client = None

    async def stop(self) -> None:
        if self._pending_login_client is not None:
            with contextlib.suppress(Exception):
                await self._pending_login_client.disconnect()
            self._pending_login_client = None
            self._pending_login_phone_number = None
        if self._bot_mtproto_client is not None:
            with contextlib.suppress(Exception):
                await self._bot_mtproto_client.stop()
            with contextlib.suppress(Exception):
                await self._bot_mtproto_client.disconnect()
            self._bot_mtproto_client = None
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
        if self._pending_login_client is not None:
            with contextlib.suppress(Exception):
                await self._pending_login_client.disconnect()
        session_name = f"seedr_tg_login_pending_{int(time.time())}_{random.randint(1000, 9999)}"
        client = self._create_client(None, name=session_name, in_memory=True)
        await client.connect()
        try:
            sent = await client.send_code(phone_number)
            login_state = await self._repository.save_telegram_login_state(
                phone_number=phone_number,
                phone_code_hash=sent.phone_code_hash,
                session_string="__pending_login_in_memory__",
                password_required=False,
            )
        except Exception:
            with contextlib.suppress(Exception):
                await client.disconnect()
            raise
        self._pending_login_client = client
        self._pending_login_phone_number = phone_number
        return login_state

    async def complete_login_with_code(self, code: str) -> TelegramUserSession:
        state = await self._repository.get_telegram_login_state()
        if state is None:
            raise RuntimeError("No pending Telegram login. Run /session_start <phone> first.")
        client = self._pending_login_client
        if client is None or self._pending_login_phone_number != state.phone_number:
            raise RuntimeError(
                "Pending login session was lost. Run /session_start <phone> again."
            )
        try:
            await client.sign_in(
                phone_number=state.phone_number,
                phone_code_hash=state.phone_code_hash,
                phone_code=code,
            )
        except SessionPasswordNeeded as exc:
            await self._repository.save_telegram_login_state(
                phone_number=state.phone_number,
                phone_code_hash=state.phone_code_hash,
                session_string="__pending_login_in_memory__",
                password_required=True,
            )
            raise TelegramPasswordRequiredError(
                "Telegram account requires 2FA password. Run /session_password <password>."
            ) from exc
        except PhoneCodeExpired as exc:
            await self._repository.clear_telegram_login_state()
            with contextlib.suppress(Exception):
                await client.disconnect()
            self._pending_login_client = None
            self._pending_login_phone_number = None
            raise TelegramCodeExpiredError(
                "Login code expired. Run /session_start <phone_number> and try /session_code again."
            ) from exc
        except PhoneCodeInvalid as exc:
            raise TelegramCodeInvalidError(
                "Invalid login code. Run /session_code with the exact code Telegram sent."
            ) from exc
        except Exception:
            raise
        return await self._promote_client_session(client, state.phone_number)

    async def complete_login_with_password(self, password: str) -> TelegramUserSession:
        state = await self._repository.get_telegram_login_state()
        if state is None:
            raise RuntimeError("No pending Telegram login. Run /session_start <phone> first.")
        if not state.password_required:
            raise RuntimeError("Current Telegram login does not require a password.")
        client = self._pending_login_client
        if client is None or self._pending_login_phone_number != state.phone_number:
            raise RuntimeError(
                "Pending login session was lost. Run /session_start <phone> again."
            )
        try:
            await client.check_password(password)
        except Exception:
            raise
        return await self._promote_client_session(client, state.phone_number)

    async def download_telegram_message_media(
        self,
        *,
        chat_id: int,
        message_id: int,
        destination: Path,
        fallback_file_id: str | None = None,
        bot_chat_id: int | None = None,
        progress_hook: Callable[[str, int, int], Awaitable[None]] | None = None,
    ) -> Path:
        """Download media from a Telegram message via MTProto (Kurigram).

        Tries bot-authenticated MTProto first for bot-PM media (if `bot_chat_id`
        is supplied), then falls back to user-authenticated MTProto session.
        """
        destination.parent.mkdir(parents=True, exist_ok=True)
        clients_to_try: list[tuple[str, Client, int]] = []

        if bot_chat_id is not None:
            bot_client = await self._get_bot_mtproto_client()
            clients_to_try.append(("bot", bot_client, int(bot_chat_id)))

        user_client = await self._get_client()
        clients_to_try.append(("user", user_client, int(chat_id)))

        # Keep MTProto media pulls serialized so parallel /rename requests do not
        # trigger repeated flood waits from upload.GetFile.
        async with self._mtproto_download_lock:
            last_error: Exception | None = None
            for client_kind, client, effective_chat_id in clients_to_try:
                try:
                    LOGGER.info(
                        (
                            "MTProto media download attempt started via %s client "
                            "chat_id=%s message_id=%s"
                        ),
                        client_kind,
                        effective_chat_id,
                        message_id,
                    )
                    started_at = time.monotonic()
                    downloaded = await self._download_media_with_client(
                        client=client,
                        chat_id=effective_chat_id,
                        message_id=message_id,
                        destination=destination,
                        fallback_file_id=fallback_file_id,
                        progress_hook=progress_hook,
                    )
                    elapsed = time.monotonic() - started_at
                    LOGGER.info(
                        (
                            "MTProto media download attempt completed via %s client "
                            "chat_id=%s message_id=%s elapsed=%.2fs"
                        ),
                        client_kind,
                        effective_chat_id,
                        message_id,
                        elapsed,
                    )
                    return downloaded
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    LOGGER.warning(
                        (
                            "MTProto media download attempt failed via %s client "
                            "chat_id=%s message_id=%s error=%s"
                        ),
                        client_kind,
                        effective_chat_id,
                        message_id,
                        exc,
                    )

        if last_error is not None:
            raise RuntimeError(
                "Unable to download replied Telegram media via MTProto."
            ) from last_error
        raise RuntimeError("Unable to download replied Telegram media via MTProto.")

    async def _download_media_with_client(
        self,
        *,
        client: Client,
        chat_id: int,
        message_id: int,
        destination: Path,
        fallback_file_id: str | None,
        progress_hook: Callable[[str, int, int], Awaitable[None]] | None,
    ) -> Path:
        media_fields = (
            "document",
            "video",
            "audio",
            "photo",
            "animation",
            "voice",
        )

        def has_media_fields(msg: Any) -> bool:
            return any(getattr(msg, field, None) is not None for field in media_fields)

        def is_valid_download(path: Path) -> bool:
            return path.exists() and path.is_file() and path.stat().st_size > 0

        loop = asyncio.get_running_loop()

        def on_download_progress(current: int, total: int, *_: object) -> None:
            if progress_hook is None:
                return

            def schedule_emit() -> None:
                task = asyncio.create_task(progress_hook("download", int(current), int(total)))
                task.add_done_callback(self._handle_progress_emit_done)

            loop.call_soon_threadsafe(schedule_emit)

        for attempt in range(1, 3):
            if destination.exists():
                with contextlib.suppress(Exception):
                    destination.unlink()
            try:
                message = await self._call_with_mtproto_flood_wait_retry(
                    lambda: client.get_messages(chat_id=chat_id, message_ids=message_id),
                    context=f"get_messages chat_id={chat_id} message_id={message_id}",
                )
            except PeerIdInvalid:
                LOGGER.info(
                    "MTProto peer cache miss for chat_id=%s; priming peers and retrying",
                    chat_id,
                )
                await self._prime_mtproto_peer_cache(client, chat_id=chat_id)
                message = await self._call_with_mtproto_flood_wait_retry(
                    lambda: client.get_messages(chat_id=chat_id, message_ids=message_id),
                    context=f"get_messages retry chat_id={chat_id} message_id={message_id}",
                )
            if isinstance(message, list):
                message = message[0] if message else None

            if message is not None and has_media_fields(message):
                message_for_download = message
                try:
                    saved_path = await self._call_with_mtproto_flood_wait_retry(
                        lambda media=message_for_download: asyncio.wait_for(
                            client.download_media(
                                media,
                                file_name=str(destination),
                                progress=on_download_progress,
                            ),
                            timeout=self._MTD_DOWNLOAD_TIMEOUT_SECONDS,
                        ),
                        context=(
                            f"download_media(message) chat_id={chat_id} "
                            f"message_id={message_id}"
                        ),
                    )
                except RPCError as exc:
                    if "file_reference" in str(exc).lower() and attempt < 2:
                        LOGGER.info(
                            "MTProto media file reference expired; retrying refresh attempt=%s",
                            attempt,
                        )
                        await asyncio.sleep(0.2)
                        continue
                    raise
                if saved_path is not None:
                    path = Path(saved_path)
                    if is_valid_download(path):
                        return path

            if fallback_file_id:
                try:
                    saved_path = await self._call_with_mtproto_flood_wait_retry(
                        lambda: asyncio.wait_for(
                            client.download_media(
                                fallback_file_id,
                                file_name=str(destination),
                                progress=on_download_progress,
                            ),
                            timeout=self._MTD_DOWNLOAD_TIMEOUT_SECONDS,
                        ),
                        context=(
                            f"download_media(file_id) chat_id={chat_id} "
                            f"message_id={message_id}"
                        ),
                    )
                except RPCError as exc:
                    if "file_reference" in str(exc).lower() and attempt < 2:
                        LOGGER.info(
                            "MTProto file-id reference expired; retrying refresh attempt=%s",
                            attempt,
                        )
                        await asyncio.sleep(0.2)
                        continue
                    raise
                if saved_path is not None:
                    path = Path(saved_path)
                    if is_valid_download(path):
                        return path

        raise RuntimeError(
            "Unable to download replied Telegram media via MTProto or downloaded file is empty."
        )

    async def _call_with_mtproto_flood_wait_retry(
        self,
        call: Callable[[], Awaitable[Any]],
        *,
        context: str,
        max_attempts: int = 8,
    ) -> Any:
        attempts = max(1, int(max_attempts))
        for attempt in range(1, attempts + 1):
            try:
                return await call()
            except FLOOD_WAIT_ERRORS as exc:
                wait_seconds = max(
                    1,
                    int(getattr(exc, "value", getattr(exc, "x", getattr(exc, "seconds", 1)))),
                )
                wait_seconds = max(1.0, float(wait_seconds) * self._FLOOD_WAIT_SAFETY_MULTIPLIER)
                if attempt >= attempts:
                    raise
                LOGGER.warning(
                    "MTProto flood wait during %s; retrying in %.2fs (attempt %s/%s)",
                    context,
                    wait_seconds,
                    attempt,
                    attempts,
                )
                await asyncio.sleep(wait_seconds)

    async def _get_bot_mtproto_client(self) -> Client:
        if (
            self._bot_mtproto_client is not None
            and self._is_client_connected(self._bot_mtproto_client)
        ):
            return self._bot_mtproto_client

        client = Client(
            name="seedr_tg_bot_media",
            api_id=self._api_id,
            api_hash=self._api_hash,
            bot_token=self._bot_token,
            in_memory=True,
            no_updates=True,
            sleep_threshold=0,
        )
        await client.start()
        try:
            await client.get_me()
        except Exception:
            with contextlib.suppress(Exception):
                await client.stop()
            with contextlib.suppress(Exception):
                await client.disconnect()
            raise
        self._bot_mtproto_client = client
        return client

    async def _prime_mtproto_peer_cache(self, client: Client, *, chat_id: int) -> None:
        """Populate Kurigram peer cache so numeric chat ids can resolve."""
        with contextlib.suppress(Exception):
            await client.resolve_peer(chat_id)
            return

        if chat_id > 0:
            with contextlib.suppress(Exception):
                await client.get_users(chat_id)
                with contextlib.suppress(Exception):
                    await client.resolve_peer(chat_id)
                    return

        with contextlib.suppress(Exception):
            async for dialog in client.get_dialogs():
                dialog_chat = getattr(dialog, "chat", None)
                if getattr(dialog_chat, "id", None) == chat_id:
                    break

        with contextlib.suppress(Exception):
            await client.resolve_peer(chat_id)

    async def upload_files(
        self,
        file_paths: list[Path],
        *,
        caption_prefix: str,
        job_id: int | None = None,
        upload_settings: UploadSettings | None = None,
        user_settings: UserSettings | None = None,
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
        loop = asyncio.get_running_loop()
        premium_available = await self._is_premium_user_session_available()

        async def upload_one(file_path: Path) -> None:
            nonlocal completed_files
            last_emit = 0.0
            speed_prev_at: float | None = None
            speed_prev_bytes = 0
            progress_emit_task: asyncio.Task[None] | None = None
            telegram_filename = self._build_telegram_filename(file_path.name)
            split_temp_dir: Path | None = None

            def on_progress(
                current: int,
                total: int,
                *,
                current_name: str = telegram_filename,
                base_offset: int = 0,
                total_size: int | None = None,
            ) -> None:
                nonlocal last_emit, progress_emit_task, speed_prev_at, speed_prev_bytes
                if progress_hook is None:
                    return
                now = time.monotonic()
                if (
                    current < total
                    and (now - last_emit) < self._UPLOAD_PROGRESS_EMIT_MIN_INTERVAL_SECONDS
                ):
                    return
                last_emit = now
                if (
                    progress_emit_task is not None
                    and not progress_emit_task.done()
                    and current < total
                ):
                    return

                aggregate_total = int(total_size or total)
                aggregate_current = int(max(0, min(aggregate_total, base_offset + current)))
                speed_bps = 0.0
                if speed_prev_at is not None:
                    delta_t = now - speed_prev_at
                    delta_b = max(0, aggregate_current - speed_prev_bytes)
                    if delta_t > 0:
                        speed_bps = float(delta_b) / float(delta_t)
                speed_prev_at = now
                speed_prev_bytes = aggregate_current
                detail = self._format_upload_progress_detail(
                    name=current_name,
                    processed_bytes=aggregate_current,
                    total_bytes=aggregate_total,
                    speed_bps=speed_bps,
                )

                async def emit() -> None:
                    async with progress_lock:
                        done = completed_files
                    await progress_hook(
                        done,
                        total_files,
                        detail,
                        aggregate_current,
                        aggregate_total,
                    )

                def schedule_emit() -> None:
                    nonlocal progress_emit_task
                    progress_emit_task = asyncio.create_task(emit())
                    progress_emit_task.add_done_callback(self._handle_progress_emit_done)

                loop.call_soon_threadsafe(schedule_emit)

            caption, parse_mode = self._render_caption(
                file_path=file_path,
                caption_prefix=caption_prefix,
                job_id=job_id,
                upload_settings=upload_settings,
                user_settings=user_settings,
                display_filename=file_path.name,
            )
            thumb_path = self._resolve_thumbnail_path(upload_settings, user_settings)
            upload_payload: dict[str, Any] = {
                "chat_id": self._target_chat_id,
                "file_path": str(file_path),
                "file_name": telegram_filename,
                "caption": caption,
                "parse_mode": parse_mode,
                "thumb_path": str(thumb_path) if thumb_path is not None else None,
                "force_document": bool(
                    upload_settings and upload_settings.media_type == UploadMediaType.DOCUMENT
                ),
                "supports_streaming": True,
                "progress_callback": on_progress,
            }
            file_size_bytes = file_path.stat().st_size if file_path.exists() else 0
            if file_size_bytes <= 0:
                raise RuntimeError(f"Refusing to upload empty file: {file_path}")

            upload_targets: list[tuple[Path, int, str]] = []
            effective_user_limit = (
                self._MTPROTO_PREMIUM_FILE_SIZE_LIMIT_BYTES
                if premium_available
                else self._MTPROTO_STANDARD_FILE_SIZE_LIMIT_BYTES
            )
            effective_mtproto_limit = min(
                int(effective_user_limit),
                int(self._MTPROTO_UPLOAD_FILE_SIZE_LIMIT_BYTES),
            )

            if (
                self._upload_hybrid_mode
                and file_size_bytes > self._MTPROTO_STANDARD_FILE_SIZE_LIMIT_BYTES
            ):
                if premium_available and file_size_bytes <= effective_mtproto_limit:
                    upload_targets = [(file_path, 0, telegram_filename)]
                else:
                    if not self._upload_split_enabled:
                        max_mib = effective_mtproto_limit / (1024 * 1024)
                        actual_mib = file_size_bytes / (1024 * 1024)
                        raise TelegramUploadTooLargeError(
                            "File is too large for active MTProto upload limit and splitting is "
                            f"disabled ({actual_mib:.2f} MiB > {max_mib:.0f} MiB)."
                        )
                    split_cap = min(
                        max(64 * 1024 * 1024, int(self._upload_split_size_bytes)),
                        effective_mtproto_limit,
                    )
                    split_temp_dir = Path(
                        tempfile.mkdtemp(
                            prefix=f"split_{file_path.stem}_",
                            dir=str(file_path.parent),
                        )
                    )
                    split_parts = await self._split_for_upload(
                        file_path=file_path,
                        output_dir=split_temp_dir,
                        split_size_bytes=split_cap,
                    )
                    offset = 0
                    for index, part_path in enumerate(split_parts, start=1):
                        upload_targets.append(
                            (
                                part_path,
                                offset,
                                f"{telegram_filename}.part{index:03d}",
                            )
                        )
                        offset += int(part_path.stat().st_size)
                    LOGGER.info(
                        (
                            "Prepared split upload file=%s parts=%s split_size=%s"
                        ),
                        file_path,
                        len(split_parts),
                        split_cap,
                    )
            else:
                upload_targets = [(file_path, 0, telegram_filename)]

            try:
                for target_path, offset_base, target_name in upload_targets:
                    part_size_bytes = (
                        target_path.stat().st_size if target_path.exists() else 0
                    )
                    if part_size_bytes <= 0:
                        raise RuntimeError(f"Refusing to upload empty part: {target_path}")
                    async with semaphore:
                        target_payload = dict(upload_payload)
                        target_payload["file_path"] = str(target_path)
                        target_payload["file_name"] = target_name
                        target_payload["progress_callback"] = self._build_progress_adapter(
                            on_progress=on_progress,
                            display_name=telegram_filename,
                            base_offset=offset_base,
                            total_size=file_size_bytes,
                        )
                        if part_size_bytes > effective_mtproto_limit:
                            max_mib = effective_mtproto_limit / (1024 * 1024)
                            actual_mib = part_size_bytes / (1024 * 1024)
                            raise TelegramUploadTooLargeError(
                                "Split part exceeds active MTProto limit "
                                f"({actual_mib:.2f} MiB > {max_mib:.0f} MiB)."
                            )
                        client = await self._get_client()
                        had_flood_wait, retry_count = await self._send_file_with_retry(
                            client,
                            target_payload,
                            upload_max_retries=upload_max_retries,
                        )

                        if self._upload_governor_enabled:
                            await self._record_upload_outcome(
                                requested_upload_concurrency=requested_upload_concurrency,
                                upload_governor_min_concurrency=min_upload_concurrency,
                                upload_governor_scale_up_after_stable_files=self._upload_governor_scale_up_after_stable_files,
                                had_flood_wait=had_flood_wait,
                                retry_count=retry_count,
                            )
            finally:
                if split_temp_dir is not None:
                    await asyncio.to_thread(shutil.rmtree, split_temp_dir, True)

            async with progress_lock:
                completed_files += 1
                done = completed_files
            if progress_hook is not None:
                await progress_hook(
                    done,
                    total_files,
                    f"Uploaded {telegram_filename}",
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
        client: Client,
        upload_payload: dict[str, Any],
        *,
        upload_max_retries: int,
    ) -> tuple[bool, int]:
        max_attempts = max(1, int(upload_max_retries))
        had_flood_wait = False
        active_client = client
        for attempt in range(1, max_attempts + 1):
            payload = dict(upload_payload)
            try:
                parse_mode = self._resolve_pyrogram_parse_mode(payload.get("parse_mode"))
                kwargs: dict[str, Any] = {
                    "chat_id": payload["chat_id"],
                    "caption": payload["caption"],
                    "parse_mode": parse_mode,
                    "file_name": payload["file_name"],
                    "progress": payload["progress_callback"],
                }
                thumb_path = payload.get("thumb_path")
                if thumb_path:
                    kwargs["thumb"] = thumb_path
                if payload.get("force_document"):
                    await active_client.send_document(
                        document=payload["file_path"],
                        **kwargs,
                    )
                else:
                    await active_client.send_video(
                        video=payload["file_path"],
                        supports_streaming=bool(payload.get("supports_streaming", True)),
                        **kwargs,
                    )
                return had_flood_wait, attempt - 1
            except FLOOD_WAIT_ERRORS as exc:
                had_flood_wait = True
                wait_seconds = max(
                    1,
                    int(getattr(exc, "value", getattr(exc, "x", getattr(exc, "seconds", 1)))),
                )
                wait_seconds = max(1.0, float(wait_seconds) * self._FLOOD_WAIT_SAFETY_MULTIPLIER)
                LOGGER.warning("Telegram flood wait while uploading; sleeping %.2fs", wait_seconds)
                await asyncio.sleep(wait_seconds)
                if attempt >= max_attempts:
                    raise
            except (RPCError, OSError, TimeoutError) as exc:
                if not self._is_retryable_upload_error(exc) or attempt >= max_attempts:
                    raise
                if isinstance(exc, OSError | TimeoutError):
                    active_client = await self._reset_mtproto_client_for_retry(active_client, exc)
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
        telegram_filename: str,
        upload_max_retries: int,
        progress_hook: Callable[[int, int, str, int, int], Awaitable[None]] | None,
        completed_files: int,
        total_files: int,
        mtproto_fallback_payload: dict[str, Any] | None = None,
        mtproto_upload_max_retries: int | None = None,
    ) -> tuple[bool, int]:
        max_attempts = max(1, int(upload_max_retries))
        had_flood_wait = False
        total_bytes = file_path.stat().st_size if file_path.exists() else 0
        bot_parse_mode = self._resolve_bot_parse_mode(parse_mode)
        last_exception: BaseException | None = None
        for attempt in range(1, max_attempts + 1):
            bot = self._bot
            if bot is None:
                self._bot = self._create_bot()
                bot = self._bot
            if bot is None:
                raise RuntimeError("Telegram bot client is not initialized")
            try:
                with file_path.open("rb") as handle:
                    upload_input = InputFile(
                        handle,
                        filename=telegram_filename,
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
                        f"{telegram_filename} {total_bytes}/{total_bytes}",
                        int(total_bytes),
                        int(total_bytes),
                    )
                return had_flood_wait, attempt - 1
            except RetryAfter as exc:
                had_flood_wait = True
                wait_seconds = max(1, int(exc.retry_after))
                wait_seconds = max(1.0, float(wait_seconds) * self._FLOOD_WAIT_SAFETY_MULTIPLIER)
                LOGGER.warning("Bot API flood wait while uploading; sleeping %.2fs", wait_seconds)
                if attempt >= max_attempts:
                    last_exception = exc
                    break
                await asyncio.sleep(wait_seconds)
            except (TimedOut, NetworkError, TelegramError, OSError, TimeoutError) as exc:
                last_exception = exc
                if (
                    isinstance(exc, TelegramError)
                    and "file must be non-empty" in str(exc).lower()
                ):
                    raise RuntimeError(
                        f"Telegram rejected empty upload payload: {file_path}"
                    ) from exc
                if self._is_bot_payload_too_large_error(exc):
                    LOGGER.warning(
                        "Bot API rejected upload payload as too large; skipping retries and "
                        "falling back to MTProto"
                    )
                    break
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
                if attempt >= max_attempts:
                    break
                await asyncio.sleep(delay)
        if (
            mtproto_fallback_payload is not None
            and last_exception is not None
            and isinstance(last_exception, (TimedOut, NetworkError, TimeoutError, OSError))
        ):
            LOGGER.warning(
                (
                    "Bot upload failed after %s attempts; falling back to MTProto user "
                    "session due to %s"
                ),
                max_attempts,
                repr(last_exception),
            )
            client = await self._get_client()
            mtproto_attempts = (
                int(mtproto_upload_max_retries)
                if mtproto_upload_max_retries is not None
                else max_attempts
            )
            mtproto_had_flood, mtproto_retry_count = await self._send_file_with_retry(
                client,
                mtproto_fallback_payload,
                upload_max_retries=mtproto_attempts,
            )
            return (
                had_flood_wait or mtproto_had_flood,
                (max_attempts - 1) + mtproto_retry_count,
            )
        if last_exception is not None:
            raise last_exception
        return had_flood_wait, max_attempts - 1

    async def _reset_bot_client_for_retry(self, exc: BaseException) -> None:
        old_bot = self._bot
        self._bot = self._create_bot()
        if old_bot is None:
            return
        LOGGER.warning("Resetting bot client after upload network error: %s", repr(exc))
        with contextlib.suppress(Exception):
            await asyncio.wait_for(old_bot.close(), timeout=10.0)

    async def _reset_mtproto_client_for_retry(
        self,
        client: Client,
        exc: BaseException,
    ) -> Client:
        LOGGER.warning("Resetting Kurigram client after upload network error: %s", repr(exc))
        with contextlib.suppress(Exception):
            await client.disconnect()
        with contextlib.suppress(Exception):
            await client.connect()
            await client.get_me()
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

    async def _is_premium_user_session_available(self) -> bool:
        if self._user_is_premium is not None:
            return bool(self._user_is_premium)
        try:
            client = await self._get_client()
        except RuntimeError:
            self._user_is_premium = False
            return False
        me = await client.get_me()
        # save_file() uses `client.me.is_premium` to pick 2000 vs 4000 MiB.
        setattr(client, "me", me)
        self._user_is_premium = bool(getattr(me, "is_premium", False))
        LOGGER.info("User session premium=%s", self._user_is_premium)
        return bool(self._user_is_premium)

    @staticmethod
    def _format_upload_progress_detail(
        *,
        name: str,
        processed_bytes: int,
        total_bytes: int,
        speed_bps: float,
    ) -> str:
        total_value = max(1, int(total_bytes))
        done_value = max(0, min(int(processed_bytes), total_value))
        percent = (float(done_value) / float(total_value)) * 100.0
        eta_seconds: int | None = None
        if speed_bps > 0 and done_value < total_value:
            eta_seconds = int((total_value - done_value) / speed_bps)
        eta_text = readable_time(eta_seconds) if eta_seconds is not None else "-"
        bar = get_progress_bar_string(percent)
        return (
            f"{name} {bar} {percent:.2f}% | "
            f"{readable_size(done_value)} / {readable_size(total_value)} | "
            f"{format_speed_bps(speed_bps)} | ETA {eta_text}"
        )

    async def _split_for_upload(
        self,
        *,
        file_path: Path,
        output_dir: Path,
        split_size_bytes: int,
    ) -> list[Path]:
        if (
            self._upload_split_use_ffmpeg_for_video
            and file_path.suffix.lower() in self._VIDEO_EXTENSIONS
        ):
            ffmpeg_parts = await self._try_ffmpeg_split(
                file_path=file_path,
                output_dir=output_dir,
                split_size_bytes=split_size_bytes,
            )
            if ffmpeg_parts:
                return ffmpeg_parts
        return await self._binary_split(
            file_path=file_path,
            output_dir=output_dir,
            split_size_bytes=split_size_bytes,
        )

    @staticmethod
    def _build_progress_adapter(
        *,
        on_progress: Callable[..., None],
        display_name: str,
        base_offset: int,
        total_size: int,
    ) -> Callable[[int, int], None]:
        def callback(current: int, total: int) -> None:
            on_progress(
                current,
                total,
                current_name=display_name,
                base_offset=base_offset,
                total_size=total_size,
            )

        return callback

    async def _try_ffmpeg_split(
        self,
        *,
        file_path: Path,
        output_dir: Path,
        split_size_bytes: int,
    ) -> list[Path] | None:
        ffmpeg_bin = shutil.which(self._upload_split_ffmpeg_binary)
        ffprobe_bin = shutil.which(self._upload_split_ffprobe_binary)
        if ffmpeg_bin is None or ffprobe_bin is None:
            return None

        probe = await asyncio.create_subprocess_exec(
            ffprobe_bin,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(file_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        probe_stdout, _probe_stderr = await probe.communicate()
        if probe.returncode != 0:
            return None
        try:
            duration = float(probe_stdout.decode("utf-8").strip())
        except ValueError:
            return None
        if duration <= 0:
            return None

        source_size = file_path.stat().st_size if file_path.exists() else 0
        if source_size <= 0:
            return None
        bytes_per_second = float(source_size) / float(duration)
        if bytes_per_second <= 0:
            return None
        segment_seconds = max(1, int(math.floor(float(split_size_bytes) / bytes_per_second)))

        pattern = output_dir / f"{file_path.stem}.part%03d{file_path.suffix}"
        process = await asyncio.create_subprocess_exec(
            ffmpeg_bin,
            "-y",
            "-v",
            "warning",
            "-i",
            str(file_path),
            "-c",
            "copy",
            "-map",
            "0",
            "-f",
            "segment",
            "-segment_time",
            str(segment_seconds),
            "-reset_timestamps",
            "1",
            str(pattern),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            LOGGER.warning(
                "ffmpeg split failed for %s (rc=%s): %s %s",
                file_path,
                process.returncode,
                stdout.decode("utf-8", errors="ignore").strip(),
                stderr.decode("utf-8", errors="ignore").strip(),
            )
            return None

        parts = sorted(output_dir.glob(f"{file_path.stem}.part*{file_path.suffix}"))
        if not parts:
            return None
        if any(part.stat().st_size <= 0 for part in parts):
            return None
        if any(part.stat().st_size > split_size_bytes for part in parts):
            LOGGER.warning(
                "ffmpeg split produced oversize part(s) for %s; falling back to binary split",
                file_path,
            )
            return None
        return parts

    async def _binary_split(
        self,
        *,
        file_path: Path,
        output_dir: Path,
        split_size_bytes: int,
    ) -> list[Path]:
        def split_sync() -> list[Path]:
            parts: list[Path] = []
            index = 1
            with file_path.open("rb") as src:
                while True:
                    part_path = output_dir / f"{file_path.stem}.part{index:03d}{file_path.suffix}"
                    written = 0
                    with part_path.open("wb") as dst:
                        while written < split_size_bytes:
                            to_read = min(
                                self._SPLIT_IO_CHUNK_BYTES,
                                split_size_bytes - written,
                            )
                            block = src.read(to_read)
                            if not block:
                                break
                            dst.write(block)
                            written += len(block)
                    if written <= 0:
                        with contextlib.suppress(FileNotFoundError):
                            part_path.unlink()
                        break
                    if part_path.stat().st_size <= 0:
                        raise RuntimeError(f"Empty split part generated: {part_path}")
                    parts.append(part_path)
                    index += 1
            return parts

        parts = await asyncio.to_thread(split_sync)
        if not parts:
            raise RuntimeError(f"Failed to split oversized file: {file_path}")
        return parts

    @staticmethod
    def _is_retryable_upload_error(exc: BaseException) -> bool:
        if isinstance(exc, FloodWait):
            return True
        name = exc.__class__.__name__.lower()
        return any(token in name for token in ("timeout", "connection", "rpc", "server"))

    @staticmethod
    def _is_bot_payload_too_large_error(exc: BaseException) -> bool:
        text = str(exc).lower()
        return (
            "request entity too large" in text
            or "entity too large" in text
            or "payload too large" in text
            or "413" in text
        )

    @staticmethod
    def _resolve_pyrogram_parse_mode(parse_mode: str | None) -> str | None:
        if parse_mode is None:
            return None
        normalized = parse_mode.strip().lower()
        if normalized == "html":
            return PyrogramParseMode.HTML
        if normalized in {"md", "markdown", "markdownv2", "markdown_v2"}:
            return PyrogramParseMode.MARKDOWN
        return None

    @staticmethod
    def _resolve_bot_parse_mode(parse_mode: str | None) -> str | None:
        if parse_mode is None:
            return None
        normalized = parse_mode.strip().lower()
        if normalized == "html":
            return BotParseMode.HTML
        if normalized in {"md", "markdown", "markdownv2", "markdown_v2"}:
            return BotParseMode.MARKDOWN_V2
        return None

    @staticmethod
    def _handle_progress_emit_done(task: asyncio.Task[None]) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            exc = task.exception()
            if exc is not None:
                LOGGER.warning("Upload progress callback failed: %s", exc)

    @staticmethod
    def _resolve_thumbnail_path(
        upload_settings: UploadSettings | None,
        user_settings: UserSettings | None = None,
    ) -> Path | None:
        thumbnails_dir = Path("downloads") / "thumbnails"
        thumbnails_dir.mkdir(parents=True, exist_ok=True)

        # Check user settings first
        if user_settings and (user_settings.thumbnail_base64 or user_settings.thumbnail_file_id):
            if user_settings.thumbnail_base64:
                path = thumbnails_dir / f"user_{user_settings.user_id}.jpg"
                return TelegramUploader._sync_thumbnail_cache_file(
                    path=path,
                    thumbnail_base64=user_settings.thumbnail_base64,
                    warning_context=f"user {user_settings.user_id}",
                )

        # Fallback to global settings
        if upload_settings and (upload_settings.thumbnail_base64 or upload_settings.thumbnail_file_id):
            if upload_settings.thumbnail_base64:
                path = thumbnails_dir / "global.jpg"
                return TelegramUploader._sync_thumbnail_cache_file(
                    path=path,
                    thumbnail_base64=upload_settings.thumbnail_base64,
                    warning_context="global settings",
                )

        return None

    @staticmethod
    def _sync_thumbnail_cache_file(
        *,
        path: Path,
        thumbnail_base64: str,
        warning_context: str,
    ) -> Path | None:
        try:
            decoded = base64.b64decode(thumbnail_base64, validate=True)
        except (ValueError, TypeError) as exc:
            LOGGER.warning("Invalid thumbnail base64 for %s: %s", warning_context, exc)
            return None

        try:
            current = path.read_bytes() if path.exists() else None
            if current != decoded:
                path.write_bytes(decoded)
        except OSError as exc:
            LOGGER.warning("Failed to persist thumbnail cache for %s: %s", warning_context, exc)
            return None
        return path

    @staticmethod
    def _render_caption(
        *,
        file_path: Path,
        caption_prefix: str,
        job_id: int | None,
        upload_settings: UploadSettings | None,
        user_settings: UserSettings | None = None,
        display_filename: str | None = None,
    ) -> tuple[str, str | None]:
        filename = display_filename or file_path.name
        
        template = None
        if user_settings and user_settings.caption_template:
            template = user_settings.caption_template
        elif upload_settings and upload_settings.caption_template:
            template = upload_settings.caption_template

        if not template:
            return f"{caption_prefix}\n{filename}", None
            
        parse_mode_setting = (
            upload_settings.caption_parse_mode if upload_settings else CaptionParseMode.HTML
        )
        include_filename_prefix = "{filename}" not in template and "{file_name}" not in template
        template_filename = filename
        template_torrent_name = caption_prefix
        if parse_mode_setting == CaptionParseMode.HTML:
            template_filename = escape(filename)
            template_torrent_name = escape(caption_prefix)

        rendered = TelegramUploader._render_caption_template(
            template=template,
            filename=template_filename,
            torrent_name=template_torrent_name,
            job_id=job_id,
        )
        parse_mode = "html"
        if parse_mode_setting == CaptionParseMode.MARKDOWN_V2:
            parse_mode = "md"
        if parse_mode_setting == CaptionParseMode.HTML:
            if include_filename_prefix:
                safe_filename = escape(filename)
                return f"{safe_filename}\n{rendered}", parse_mode
            return rendered, parse_mode
        if include_filename_prefix:
            return f"{filename}\n{rendered}", parse_mode
        return rendered, parse_mode

    @staticmethod
    def _render_caption_template(
        *,
        template: str,
        filename: str,
        torrent_name: str,
        job_id: int | None,
    ) -> str:
        replacements = {
            "filename": filename,
            "file_name": filename,
            "name": filename,
            "torrent_name": torrent_name,
            "torrent": torrent_name,
            "title": torrent_name,
            "job_id": "" if job_id is None else str(job_id),
            "job": "" if job_id is None else str(job_id),
        }

        def replace_match(match: re.Match[str]) -> str:
            key = match.group(1)
            return replacements.get(key, match.group(0))

        return _CAPTION_PLACEHOLDER_PATTERN.sub(replace_match, template)

    @staticmethod
    def _build_telegram_filename(filename: str) -> str:
        raw_name = (filename or "").strip()
        raw_path = Path(raw_name)
        extension = raw_path.suffix
        raw_stem = raw_name[: -len(extension)] if extension else raw_name
        cleaned_stem = TelegramUploader._strip_leading_release_site_prefix(raw_stem)

        stem = cleaned_stem.replace("\n", " ").replace("\r", " ")
        stem = _INVALID_FILENAME_CHARS.sub(" ", stem)
        stem = re.sub(r"\s+", " ", stem).strip(" ._") or "file"

        base_budget = max(1, _TELEGRAM_FILENAME_MAX_CHARS - len(extension))
        truncated_stem = stem[:base_budget]
        truncated_stem = truncated_stem.rstrip(" ._-") or "file"
        final_name = f"{truncated_stem}{extension}"
        if len(final_name) <= _TELEGRAM_FILENAME_MAX_CHARS:
            return final_name
        return final_name[:_TELEGRAM_FILENAME_MAX_CHARS]

    @staticmethod
    def _strip_leading_release_site_prefix(stem: str) -> str:
        cleaned = _LEADING_1TAMILMV_PREFIX.sub("", stem).strip(" .")
        return cleaned if cleaned else stem

    @staticmethod
    def _truncate_utf8(text: str, max_bytes: int) -> str:
        if max_bytes <= 0:
            return ""
        encoded = text.encode("utf-8")
        if len(encoded) <= max_bytes:
            return text
        truncated = encoded[:max_bytes]
        while truncated:
            try:
                return truncated.decode("utf-8")
            except UnicodeDecodeError:
                truncated = truncated[:-1]
        return ""

    async def _get_client(self) -> Client:
        if self._client is not None and self._is_client_connected(self._client):
            return self._client
        stored_session = await self._repository.get_telegram_user_session()
        if stored_session is None:
            raise RuntimeError(
                "Telegram uploader session is not configured. "
                "Run /session_start <phone> in the admin chat."
            )
        self._client = await self._connect_client(stored_session.session_string)
        return self._client

    async def _connect_client(self, session_string: str) -> Client:
        client = self._create_client(session_string, name="seedr_tg_uploader")
        await client.connect()
        try:
            me = await client.get_me()
            setattr(client, "me", me)
        except Exception as exc:
            await client.disconnect()
            raise RuntimeError("Stored Telegram user session is no longer authorized.") from exc
        return client

    async def _promote_client_session(
        self,
        client: Client,
        phone_number: str | None,
    ) -> TelegramUserSession:
        session = await self._persist_authorized_session(client, phone_number=phone_number)
        await self._repository.clear_telegram_login_state()
        self._pending_login_client = None
        self._pending_login_phone_number = None
        if self._client is not None:
            await self._client.disconnect()
        self._client = client
        return session

    async def _persist_authorized_session(
        self,
        client: Client,
        *,
        phone_number: str | None,
    ) -> TelegramUserSession:
        me = await client.get_me()
        session_string = await client.export_session_string()
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

    @staticmethod
    def _is_client_connected(client: Client) -> bool:
        connected = getattr(client, "is_connected", None)
        if callable(connected):
            return bool(connected())
        return bool(connected)
