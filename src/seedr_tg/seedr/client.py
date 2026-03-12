from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from seedrcc import AsyncSeedr, Token
from seedrcc.models import Folder, Torrent, TorrentProgress

from seedr_tg.config import Settings

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ResolvedTorrent:
    title: str | None
    total_size_bytes: int | None
    torrent: Torrent | None
    folder: Folder | None


@dataclass(slots=True)
class RemoteFile:
    name: str
    size: int
    download_url: str


class SeedrService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: AsyncSeedr | None = None
        self._token_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._client is not None:
            return
        self._client = await self._build_client()

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def add_magnet(self, magnet_link: str) -> int | None:
        client = await self._get_client()
        result = await client.add_torrent(magnet_link=magnet_link)
        return result.user_torrent_id

    async def resolve_torrent(self, torrent_id: int | None) -> ResolvedTorrent:
        client = await self._get_client()
        contents = await client.list_contents()
        torrent = self._find_torrent(contents.torrents, torrent_id)
        folder = self._find_folder(contents.folders, torrent.folder if torrent else None)
        total_size_bytes = folder.size if folder else (torrent.size if torrent else None)
        title = folder.name if folder else (torrent.name if torrent else None)
        return ResolvedTorrent(
            title=title,
            total_size_bytes=total_size_bytes,
            torrent=torrent,
            folder=folder,
        )

    async def get_torrent_progress(self, progress_url: str | None) -> TorrentProgress | None:
        if not progress_url:
            return None
        client = await self._get_client()
        return await client.get_torrent_progress(progress_url)

    async def fetch_remote_files(self, folder_id: int) -> list[RemoteFile]:
        client = await self._get_client()
        contents = await client.list_contents(folder_id=str(folder_id))
        remote_files: list[RemoteFile] = []
        for item in contents.files:
            result = await client.fetch_file(str(item.folder_file_id))
            remote_files.append(RemoteFile(name=item.name, size=item.size, download_url=result.url))
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
        async with httpx.AsyncClient(follow_redirects=True, timeout=None) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                total = int(response.headers.get("Content-Length", 0))
                downloaded = 0
                destination.parent.mkdir(parents=True, exist_ok=True)
                with destination.open("wb") as handle:
                    async for chunk in response.aiter_bytes(1024 * 1024):
                        handle.write(chunk)
                        downloaded += len(chunk)
                        if progress_hook is not None:
                            await progress_hook(downloaded, total)

    async def _get_client(self) -> AsyncSeedr:
        if self._client is None:
            await self.start()
        assert self._client is not None
        return self._client

    async def _build_client(self) -> AsyncSeedr:
        if self._settings.seedr_token_json:
            token = Token.from_json(self._settings.seedr_token_json)
            return AsyncSeedr(token=token, on_token_refresh=self._persist_token)
        return await AsyncSeedr.from_password(
            self._settings.seedr_email or "",
            self._settings.seedr_password or "",
            on_token_refresh=self._persist_token,
        )

    async def _persist_token(self, token: Token) -> None:
        async with self._token_lock:
            self._settings.seedr_token_json = token.to_json()
            token_path = self._settings.database_path.parent / "seedr_token.json"
            token_path.write_text(token.to_json(), encoding="utf-8")
            LOGGER.info("Persisted refreshed Seedr token to %s", token_path)

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
