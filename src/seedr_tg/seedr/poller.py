from __future__ import annotations

from dataclasses import dataclass

from seedr_tg.seedr.client import ResolvedTorrent, SeedrService


@dataclass(slots=True)
class TorrentSnapshot:
    title: str | None
    total_size_bytes: int | None
    progress_percent: float
    seedr_folder_id: int | None
    seedr_folder_name: str | None
    is_complete: bool


class SeedrPoller:
    def __init__(self, seedr_service: SeedrService) -> None:
        self._seedr_service = seedr_service

    async def poll(self, torrent_id: int | None) -> TorrentSnapshot:
        resolved: ResolvedTorrent = await self._seedr_service.resolve_torrent(torrent_id)
        progress_percent = 0.0
        is_complete = False
        if resolved.torrent and resolved.torrent.progress_url:
            progress = await self._seedr_service.get_torrent_progress(resolved.torrent.progress_url)
            if progress is not None:
                progress_percent = float(progress.progress)
                is_complete = progress_percent >= 100.0 or int(progress.stats.folder_created) == 1
        elif resolved.folder:
            progress_percent = 100.0
            is_complete = True
        return TorrentSnapshot(
            title=resolved.title,
            total_size_bytes=resolved.total_size_bytes,
            progress_percent=progress_percent,
            seedr_folder_id=resolved.folder.id if resolved.folder else None,
            seedr_folder_name=resolved.folder.name if resolved.folder else None,
            is_complete=is_complete,
        )
