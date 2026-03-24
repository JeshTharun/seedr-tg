from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from seedr_tg.worker.queue_runner import QueueRunner


class _FakeSeedrService:
    def __init__(self) -> None:
        self.add_magnet_calls: list[str] = []
        self.add_torrent_file_calls: list[Path] = []

    async def add_magnet(self, magnet_link: str) -> int:
        self.add_magnet_calls.append(magnet_link)
        return 11

    async def add_torrent_file(self, torrent_file_path: Path | str) -> int:
        self.add_torrent_file_calls.append(Path(torrent_file_path))
        return 22


@pytest.mark.asyncio
async def test_add_seedr_torrent_for_job_uses_magnet_when_no_torrent_file_path():
    runner = QueueRunner.__new__(QueueRunner)
    fake_seedr = _FakeSeedrService()
    object.__setattr__(runner, "_seedr_service", fake_seedr)

    job = SimpleNamespace(magnet_link="magnet:?xt=urn:btih:123", torrent_file_path=None)

    add_for_job = getattr(runner, "_add_seedr_torrent_for_job")
    torrent_id = await add_for_job(job)

    assert torrent_id == 11
    assert fake_seedr.add_magnet_calls == ["magnet:?xt=urn:btih:123"]
    assert fake_seedr.add_torrent_file_calls == []


@pytest.mark.asyncio
async def test_add_seedr_torrent_for_job_uses_torrent_file_and_deletes_temp(tmp_path):
    runner = QueueRunner.__new__(QueueRunner)
    fake_seedr = _FakeSeedrService()
    object.__setattr__(runner, "_seedr_service", fake_seedr)

    torrent_file = tmp_path / "sample.torrent"
    torrent_file.write_bytes(b"d4:infod4:name4:teste")
    job = SimpleNamespace(magnet_link="torrent-file:abc", torrent_file_path=str(torrent_file))

    add_for_job = getattr(runner, "_add_seedr_torrent_for_job")
    torrent_id = await add_for_job(job)

    assert torrent_id == 22
    assert fake_seedr.add_magnet_calls == []
    assert fake_seedr.add_torrent_file_calls == [torrent_file]
    assert not torrent_file.exists()
