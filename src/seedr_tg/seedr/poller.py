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


class SeedrTrackingLostError(RuntimeError):
    pass


class SeedrPoller:
    def __init__(self, seedr_service: SeedrService) -> None:
        self._seedr_service = seedr_service

    async def poll(self, torrent_id: int | None, known_folder_id: int | None = None) -> TorrentSnapshot:
        resolved: ResolvedTorrent = await self._seedr_service.resolve_torrent(torrent_id, known_folder_id=known_folder_id)
        progress_percent = 0.0
        is_complete = False
        torrent_folder_id = self._parse_folder_id(resolved.torrent.folder) if resolved.torrent else None
        if resolved.torrent and resolved.torrent.progress_url:
            progress = await self._seedr_service.get_torrent_progress(resolved.torrent.progress_url)
            if progress is not None:
                progress_percent = float(progress.progress)
                is_complete = progress_percent >= 100.0 or int(progress.stats.folder_created) == 1
        if resolved.torrent:
            progress_percent = max(progress_percent, self._parse_progress_percent(resolved.torrent.progress))
            is_complete = is_complete or progress_percent >= 100.0
            if torrent_folder_id is not None and (resolved.folder is not None or progress_percent >= 99.0):
                is_complete = True
        elif resolved.folder:
            progress_percent = 100.0
            is_complete = True
            
        if resolved.torrent is None and resolved.folder is None and not resolved.has_files:
            raise SeedrTrackingLostError(
                "Seedr stopped tracking this torrent. Common causes: source is larger than 4GB, "
                "magnet is invalid/dead, or Seedr removed it due to account/storage limits."
            )
            
        if resolved.has_files and (resolved.torrent is None or progress_percent >= 99.9):
            progress_percent = max(progress_percent, 100.0)
            is_complete = True
        snapshot_folder_id = resolved.folder.id if resolved.folder else torrent_folder_id
        return TorrentSnapshot(
            title=resolved.title,
            total_size_bytes=resolved.total_size_bytes,
            progress_percent=progress_percent,
            seedr_folder_id=snapshot_folder_id,
            seedr_folder_name=resolved.folder.name if resolved.folder else None,
            is_complete=is_complete,
        )

    @staticmethod
    def _parse_progress_percent(raw_progress: str | float | int | None) -> float:
        if raw_progress is None:
            return 0.0
        if isinstance(raw_progress, (int, float)):
            return float(raw_progress)
        cleaned = str(raw_progress).strip().rstrip("%")
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    @staticmethod
    def _parse_folder_id(raw_folder_id: str | int | None) -> int | None:
        if raw_folder_id in (None, "", "0", "-1", 0, -1):
            return None
        try:
            return int(raw_folder_id)
        except (TypeError, ValueError):
            return None
