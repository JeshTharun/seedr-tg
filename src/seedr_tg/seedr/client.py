from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiofiles
import httpx
from seedrcc import AsyncSeedr, Token
from seedrcc.exceptions import APIError
from seedrcc.models import Folder, Torrent, TorrentProgress

from seedr_tg.config import Settings
from seedr_tg.db.models import SeedrDeviceCodeRecord
from seedr_tg.db.repository import JobRepository

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ResolvedTorrent:
    title: str | None
    total_size_bytes: int | None
    torrent: Torrent | None
    folder: Folder | None
    has_files: bool


@dataclass(slots=True)
class RemoteFile:
    name: str
    size: int
    download_url: str


class DownloadIntegrityError(RuntimeError):
    pass


class SeedrService:
    _DOWNLOAD_CHUNK_SIZE = 4 * 1024 * 1024

    def __init__(self, settings: Settings, repository: JobRepository) -> None:
        self._settings = settings
        self._repository = repository
        self._client: AsyncSeedr | None = None
        self._http_client: httpx.AsyncClient | None = None
        self._token_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._client is not None:
            return
        if self._http_client is None:
            timeout = httpx.Timeout(
                connect=self._settings.download_connect_timeout_seconds,
                read=self._settings.download_read_timeout_seconds,
                write=self._settings.download_write_timeout_seconds,
                pool=self._settings.download_pool_timeout_seconds,
            )
            self._http_client = httpx.AsyncClient(
                follow_redirects=True,
                timeout=timeout,
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            )
        token_json = await self._repository.get_seedr_token_json()
        if token_json is None and self._settings.seedr_token_json:
            token_json = self._settings.seedr_token_json
            await self._repository.set_seedr_token_json(token_json)
        if token_json is not None:
            self._client = await self._build_client_from_token(token_json)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def begin_device_authorization(self) -> SeedrDeviceCodeRecord:
        codes = await AsyncSeedr.get_device_code()
        return await self._repository.save_seedr_device_code(
            device_code=codes.device_code,
            user_code=codes.user_code,
            verification_url=codes.verification_url,
            expires_in=getattr(codes, "expires_in", None),
        )

    async def complete_device_authorization(self) -> str:
        pending = await self._repository.get_seedr_device_code()
        if pending is None:
            raise RuntimeError("No pending Seedr device authorization. Run /seedr_auth first.")
        client = await AsyncSeedr.from_device_code(
            pending.device_code,
            on_token_refresh=self._persist_token,
        )
        settings = await client.get_settings()
        await self._replace_client(client)
        await self._persist_token(client.token)
        await self._repository.clear_seedr_device_code()
        account = getattr(settings.account, "username", None) or "Seedr account"
        return str(account)

    async def add_magnet(self, magnet_link: str) -> int | None:
        client = await self._get_client()
        try:
            result = await client.add_torrent(magnet_link=magnet_link)
            return result.user_torrent_id
        except APIError as exc:
            if not self._is_storage_related_api_error(exc):
                raise
            LOGGER.warning(
                (
                    "Seedr add_torrent failed due to storage/quota limit. "
                    "Attempting automatic cleanup before retry. error=%s"
                ),
                exc,
            )
            deleted_count = await self._cleanup_seedr_storage(exclude_active_jobs=True)
            if deleted_count <= 0:
                LOGGER.warning(
                    "Seedr cleanup did not remove removable artifacts; "
                    "add_torrent retry skipped"
                )
                raise
            await asyncio.sleep(0.6)
            result = await client.add_torrent(magnet_link=magnet_link)
            LOGGER.info(
                "Seedr add_torrent succeeded after cleanup retry; removed_items=%s",
                deleted_count,
            )
            return result.user_torrent_id

    async def add_torrent_file(self, torrent_file_path: Path | str) -> int | None:
        client = await self._get_client()
        file_path = Path(torrent_file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Torrent file not found: {file_path}")
        try:
            result = await client.add_torrent(torrent_file=str(file_path))
            return result.user_torrent_id
        except APIError as exc:
            if not self._is_storage_related_api_error(exc):
                raise
            LOGGER.warning(
                (
                    "Seedr add_torrent(torrent_file) failed due to storage/quota limit. "
                    "Attempting automatic cleanup before retry. error=%s"
                ),
                exc,
            )
            deleted_count = await self._cleanup_seedr_storage(exclude_active_jobs=True)
            if deleted_count <= 0:
                LOGGER.warning(
                    "Seedr cleanup did not remove removable artifacts; "
                    "torrent file add retry skipped"
                )
                raise
            await asyncio.sleep(0.6)
            result = await client.add_torrent(torrent_file=str(file_path))
            LOGGER.info(
                "Seedr add_torrent(torrent_file) succeeded after cleanup retry; removed_items=%s",
                deleted_count,
            )
            return result.user_torrent_id

    async def resolve_torrent(
        self,
        torrent_id: int | None,
        known_folder_id: int | None = None,
    ) -> ResolvedTorrent:
        client = await self._get_client()
        contents = await client.list_contents()
        torrent = self._find_torrent(contents.torrents, torrent_id)
        folder = self._find_folder(
            contents.folders,
            (torrent.folder if torrent else None) or known_folder_id,
        )
        if (
            folder is None
            and torrent is None
            and known_folder_id is None
            and torrent_id is not None
        ):
            # Seedr can drop completed torrents before files/folder metadata is fully linked.
            # If exactly one folder remains at root, treat it as the completed torrent output.
            folder = self._single_folder_fallback(contents.folders)
        total_size_bytes = folder.size if folder else (torrent.size if torrent else None)
        title = folder.name if folder else (torrent.name if torrent else None)
        return ResolvedTorrent(
            title=title,
            total_size_bytes=total_size_bytes,
            torrent=torrent,
            folder=folder,
            has_files=bool(contents.files),
        )

    async def get_torrent_progress(self, progress_url: str | None) -> TorrentProgress | None:
        if not progress_url:
            return None
        client = await self._get_client()
        return await client.get_torrent_progress(progress_url)

    async def fetch_remote_files(self, folder_id: int | None = None) -> list[RemoteFile]:
        client = await self._get_client()
        remote_files: list[RemoteFile] = []
        visited: set[str] = set()

        async def walk(current_folder_id: int | None) -> None:
            folder_key = "root" if current_folder_id is None else str(current_folder_id)
            if folder_key in visited:
                return
            visited.add(folder_key)

            if current_folder_id is None:
                contents = await client.list_contents()
            else:
                contents = await client.list_contents(folder_id=str(current_folder_id))

            for item in contents.files:
                result = await client.fetch_file(str(item.folder_file_id))
                remote_files.append(
                    RemoteFile(name=item.name, size=item.size, download_url=result.url)
                )

            for child_folder in contents.folders:
                await walk(int(child_folder.id))

        await walk(folder_id)
        return remote_files

    async def delete_torrent(self, torrent_id: int | None) -> None:
        if torrent_id is None:
            return
        client = await self._get_client()
        await client.delete_torrent(str(torrent_id))

    async def delete_folder(self, folder_id: int | None) -> None:
        if folder_id is None:
            return
        client = await self._get_client()
        await client.delete_folder(str(folder_id))

    async def ensure_under_limit(self, total_size_bytes: int | None) -> None:
        if total_size_bytes is None:
            return
        if total_size_bytes > self._settings.max_seedr_file_size_bytes:
            raise ValueError(
                "Torrent size "
                f"{total_size_bytes} exceeds "
                f"{self._settings.max_seedr_file_size_bytes}"
            )

    async def download_file(
        self,
        url: str,
        destination: Path,
        progress_hook: Any | None = None,
    ) -> None:
        if self._settings.use_aria2_downloads:
            try:
                LOGGER.info("Using aria2 downloader for %s", destination.name)
                await self._download_file_via_aria2(
                    url,
                    destination,
                    progress_hook=progress_hook,
                )
                return
            except (RuntimeError, OSError, ValueError) as exc:
                self._cleanup_partial_download_artifacts(destination)
                LOGGER.warning(
                    "aria2 download failed for %s, falling back to httpx downloader: %s",
                    destination.name,
                    exc,
                )
        else:
            LOGGER.info("Using built-in httpx downloader for %s", destination.name)

        client = self._http_client
        if client is None:
            await self.start()
            client = self._http_client
        if client is None:
            raise RuntimeError("HTTP downloader is not initialized")

        destination.parent.mkdir(parents=True, exist_ok=True)
        max_attempts = max(1, int(self._settings.download_max_retries))
        for attempt in range(1, max_attempts + 1):
            try:
                downloaded = 0
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    total = int(response.headers.get("Content-Length", 0))
                    async with aiofiles.open(destination, "wb") as handle:
                        async for chunk in response.aiter_bytes(self._DOWNLOAD_CHUNK_SIZE):
                            await handle.write(chunk)
                            downloaded += len(chunk)
                            if progress_hook is not None:
                                await progress_hook(downloaded, total)
                if total > 0 and downloaded != total:
                    raise DownloadIntegrityError(
                        "size mismatch for "
                        f"{destination.name}: downloaded={downloaded} expected={total}"
                    )
                return
            except (
                httpx.TimeoutException,
                httpx.ConnectError,
                httpx.ReadError,
                httpx.HTTPStatusError,
                DownloadIntegrityError,
                OSError,
            ) as exc:
                self._cleanup_partial_download_artifacts(destination)
                is_retryable = self._is_retryable_download_error(exc)
                if not is_retryable or attempt >= max_attempts:
                    raise
                backoff = min(
                    self._settings.download_retry_base_delay_seconds * (2 ** (attempt - 1)),
                    self._settings.download_retry_max_delay_seconds,
                )
                jitter = random.uniform(0.0, self._settings.download_retry_base_delay_seconds)
                delay = backoff + jitter
                LOGGER.warning(
                    "Retrying Seedr file download (attempt %s/%s) in %.2fs due to %s",
                    attempt,
                    max_attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

    async def _download_file_via_aria2(
        self,
        url: str,
        destination: Path,
        progress_hook: Any | None = None,
    ) -> None:
        aria2_binary = self._settings.aria2_binary.strip() or "aria2c"
        aria2_path = shutil.which(aria2_binary)
        if aria2_path is None:
            raise RuntimeError(f"aria2 binary '{aria2_binary}' is not available in PATH")

        destination.parent.mkdir(parents=True, exist_ok=True)
        total_bytes = await self._probe_remote_size(url)
        if progress_hook is not None:
            await progress_hook(0, total_bytes)

        cmd = [
            aria2_path,
            "--allow-overwrite=true",
            "--continue=true",
            "--auto-file-renaming=false",
            "--summary-interval=0",
            "--console-log-level=warn",
            "--download-result=hide",
            f"--split={max(1, int(self._settings.aria2_split))}",
            (
                "--max-connection-per-server="
                f"{max(1, int(self._settings.aria2_max_connection_per_server))}"
            ),
            f"--min-split-size={self._settings.aria2_min_split_size}",
            f"--file-allocation={self._settings.aria2_file_allocation}",
            f"--max-tries={max(1, int(self._settings.download_max_retries))}",
            "--retry-wait=1",
            f"--timeout={max(5, int(self._settings.download_read_timeout_seconds))}",
            "--dir",
            str(destination.parent),
            "--out",
            destination.name,
            url,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            if progress_hook is not None:
                await self._track_aria2_progress(proc, destination, total_bytes, progress_hook)
            stdout, stderr = await proc.communicate()
        except asyncio.CancelledError:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            self._cleanup_partial_download_artifacts(destination)
            raise

        if proc.returncode != 0:
            stderr_text = stderr.decode("utf-8", errors="ignore").strip()
            stdout_text = stdout.decode("utf-8", errors="ignore").strip()
            details = stderr_text or stdout_text or "unknown error"
            self._cleanup_partial_download_artifacts(destination)
            raise RuntimeError(f"aria2 exited with code {proc.returncode}: {details}")

        final_size = destination.stat().st_size if destination.exists() else 0
        if total_bytes > 0 and final_size != total_bytes:
            self._cleanup_partial_download_artifacts(destination)
            raise DownloadIntegrityError(
                "aria2 size mismatch for "
                f"{destination.name}: downloaded={final_size} expected={total_bytes}"
            )
        if progress_hook is not None:
            await progress_hook(final_size, total_bytes or final_size)

    async def _track_aria2_progress(
        self,
        proc: asyncio.subprocess.Process,
        destination: Path,
        total_bytes: int,
        progress_hook: Any,
    ) -> None:
        wait_task = asyncio.create_task(proc.wait())
        last_reported = -1
        try:
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(wait_task), timeout=1.0)
                    break
                except TimeoutError:
                    current_size = destination.stat().st_size if destination.exists() else 0
                    if current_size != last_reported:
                        await progress_hook(current_size, total_bytes)
                        last_reported = current_size
        finally:
            if not wait_task.done():
                wait_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await wait_task

    async def _probe_remote_size(self, url: str) -> int:
        client = self._http_client
        if client is None:
            await self.start()
            client = self._http_client
        if client is None:
            return 0

        try:
            response = await client.head(url)
            if response.status_code < 400:
                value = response.headers.get("Content-Length")
                if value and value.isdigit():
                    return int(value)
        except (httpx.HTTPError, ValueError):
            return 0
        return 0

    @staticmethod
    def _is_retryable_download_error(exc: BaseException) -> bool:
        if isinstance(exc, httpx.TimeoutException | httpx.ConnectError | httpx.ReadError):
            return True
        if isinstance(exc, DownloadIntegrityError):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            return status == 429 or status >= 500
        return False

    @staticmethod
    def _cleanup_partial_download_artifacts(destination: Path) -> None:
        with contextlib.suppress(Exception):
            if destination.exists():
                destination.unlink()
        with contextlib.suppress(Exception):
            partial_state = destination.with_suffix(destination.suffix + ".aria2")
            if partial_state.exists():
                partial_state.unlink()

    async def _get_client(self) -> AsyncSeedr:
        if self._client is None:
            await self.start()
        if self._client is None:
            raise RuntimeError(
                "Seedr is not authenticated. "
                "Use /seedr_auth and /seedr_auth_done in the admin chat."
            )
        return self._client

    async def _build_client_from_token(self, token_json: str) -> AsyncSeedr:
        token = Token.from_json(token_json)
        return AsyncSeedr(token=token, on_token_refresh=self._persist_token)

    async def _replace_client(self, client: AsyncSeedr) -> None:
        old_client = self._client
        self._client = client
        if old_client is not None:
            await old_client.close()

    async def _persist_token(self, token: Token) -> None:
        async with self._token_lock:
            self._settings.seedr_token_json = token.to_json()
            await self._repository.set_seedr_token_json(token.to_json())
            LOGGER.info("Persisted refreshed Seedr token to MongoDB")

    @staticmethod
    def _find_torrent(torrents: list[Torrent], torrent_id: int | None) -> Torrent | None:
        if torrent_id is None:
            return torrents[0] if torrents else None
        for torrent in torrents:
            if int(torrent.id) == int(torrent_id):
                return torrent
        return None

    @staticmethod
    def _find_folder(folders: list[Folder], folder_id: int | str | None) -> Folder | None:
        if folder_id is None:
            return None
        for folder in folders:
            if str(folder.id) == str(folder_id):
                return folder
        return None

    @staticmethod
    def _single_folder_fallback(folders: list[Folder]) -> Folder | None:
        if len(folders) != 1:
            return None
        return folders[0]

    @staticmethod
    def _is_storage_related_api_error(exc: APIError) -> bool:
        message = str(exc).lower()
        if any(
            token in message
            for token in (
                "storage",
                "not enough",
                "insufficient",
                "quota",
                "full",
                "space",
                "limit",
            )
        ):
            return True
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code in {507, 509}:
            return True
        code = getattr(exc, "code", None)
        # Seedr API codes can vary; this catches the common storage-limit family.
        if isinstance(code, int) and code in {9, 11, 12, 13, 14, 15, 16, 17, 18, 19}:
            return True
        return False

    async def _cleanup_seedr_storage(self, *, exclude_active_jobs: bool) -> int:
        client = await self._get_client()
        protected_torrent_ids: set[str] = set()
        protected_folder_ids: set[str] = set()
        if exclude_active_jobs:
            active_jobs = await self._repository.list_jobs(include_final=False)
            protected_torrent_ids = {
                str(job.seedr_torrent_id)
                for job in active_jobs
                if job.seedr_torrent_id is not None
            }
            protected_folder_ids = {
                str(job.seedr_folder_id)
                for job in active_jobs
                if job.seedr_folder_id is not None
            }

        contents = await client.list_contents()
        torrents = sorted(
            [item for item in contents.torrents if str(item.id) not in protected_torrent_ids],
            key=lambda item: int(item.id),
        )
        folders = sorted(
            [item for item in contents.folders if str(item.id) not in protected_folder_ids],
            key=lambda item: int(item.id),
        )

        deleted = 0
        for torrent in torrents:
            try:
                await client.delete_torrent(str(torrent.id))
                deleted += 1
                LOGGER.info("Seedr cleanup removed torrent id=%s name=%s", torrent.id, torrent.name)
            except (APIError, OSError, RuntimeError) as cleanup_exc:
                LOGGER.warning(
                    "Seedr cleanup skipped torrent id=%s due to error: %s",
                    torrent.id,
                    cleanup_exc,
                )

        for folder in folders:
            try:
                await client.delete_folder(str(folder.id))
                deleted += 1
                LOGGER.info("Seedr cleanup removed folder id=%s name=%s", folder.id, folder.name)
            except (APIError, OSError, RuntimeError) as cleanup_exc:
                LOGGER.warning(
                    "Seedr cleanup skipped folder id=%s due to error: %s",
                    folder.id,
                    cleanup_exc,
                )

        LOGGER.info(
            "Seedr cleanup summary: deleted=%s protected_torrents=%s protected_folders=%s",
            deleted,
            len(protected_torrent_ids),
            len(protected_folder_ids),
        )
        return deleted
