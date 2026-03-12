from __future__ import annotations

from pathlib import Path

from seedr_tg.seedr.client import RemoteFile, SeedrService


class LocalDownloader:
    def __init__(self, seedr_service: SeedrService) -> None:
        self._seedr_service = seedr_service

    async def download_files(
        self,
        remote_files: list[RemoteFile],
        destination_root: Path,
        progress_hook=None,
    ) -> list[Path]:
        downloaded_paths: list[Path] = []
        total_size = sum(remote.size for remote in remote_files)
        aggregate_downloaded = 0
        for remote_file in remote_files:
            destination = destination_root / remote_file.name

            async def on_progress(
                downloaded: int,
                total: int,
                *,
                current_name: str = remote_file.name,
                aggregate_offset: int = aggregate_downloaded,
            ) -> None:
                if progress_hook is not None:
                    completed = aggregate_offset + downloaded
                    await progress_hook(completed, total_size or total, current_name)

            await self._seedr_service.download_file(
                remote_file.download_url,
                destination,
                progress_hook=on_progress,
            )
            downloaded_paths.append(destination)
            aggregate_downloaded += remote_file.size
        return downloaded_paths
