from __future__ import annotations

import asyncio
from pathlib import Path

from seedr_tg.seedr.client import RemoteFile, SeedrService


class LocalDownloader:
    def __init__(self, seedr_service: SeedrService) -> None:
        self._seedr_service = seedr_service

    async def download_files(
        self,
        remote_files: list[RemoteFile],
        destination_root: Path,
        concurrency: int = 1,
        progress_hook=None,
    ) -> list[Path]:
        downloaded_paths: list[Path] = []
        total_size = sum(remote.size for remote in remote_files)
        lock = asyncio.Lock()
        in_progress: dict[str, int] = {remote.name: 0 for remote in remote_files}
        semaphore = asyncio.Semaphore(max(1, int(concurrency)))

        async def download_one(remote_file: RemoteFile) -> Path:
            destination = destination_root / remote_file.name

            async def on_progress(
                downloaded: int,
                total: int,
                *,
                current_name: str = remote_file.name,
            ) -> None:
                if progress_hook is None:
                    return
                async with lock:
                    in_progress[current_name] = downloaded
                    aggregate_downloaded = sum(in_progress.values())
                await progress_hook(aggregate_downloaded, total_size or total, current_name)

            async with semaphore:
                await self._seedr_service.download_file(
                    remote_file.download_url,
                    destination,
                    progress_hook=on_progress,
                )
            return destination

        tasks = [asyncio.create_task(download_one(remote)) for remote in remote_files]
        results = await asyncio.gather(*tasks)
        downloaded_paths.extend(results)
        return downloaded_paths
