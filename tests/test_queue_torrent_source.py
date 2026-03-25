from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from seedr_tg.db.models import JobPhase
from seedr_tg.seedr.client import SeedrMaxTorrentSizeError, SeedrService
from seedr_tg.seedr.poller import SeedrTrackingLostError
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


def test_format_failure_reason_returns_plain_seedr_limit_warning():
    format_reason = getattr(QueueRunner, "_format_failure_reason")
    reason = format_reason(
        SeedrMaxTorrentSizeError(
            "Seedr accepts torrents/magnets up to 4GB only. Please use a source <= 4GB."
        )
    )

    assert reason == "Seedr accepts torrents/magnets up to 4GB only. Please use a source <= 4GB."


def test_format_failure_reason_returns_plain_seedr_tracking_lost_reason():
    format_reason = getattr(QueueRunner, "_format_failure_reason")
    reason = format_reason(
        SeedrTrackingLostError(
            "Seedr stopped tracking this torrent. Common causes: source is larger than 4GB, magnet is invalid/dead, or Seedr removed it due to account/storage limits."
        )
    )

    assert reason.startswith("Seedr stopped tracking this torrent")


def test_format_failure_reason_uses_plain_runtime_message_without_prefix():
    format_reason = getattr(QueueRunner, "_format_failure_reason")
    reason = format_reason(RuntimeError("Something failed clearly"))

    assert reason == "Something failed clearly"


@pytest.mark.asyncio
async def test_ensure_under_limit_raises_human_readable_4gb_warning():
    service = SeedrService.__new__(SeedrService)
    object.__setattr__(
        service,
        "_settings",
        SimpleNamespace(max_seedr_file_size_bytes=4 * 1024 * 1024 * 1024),
    )

    with pytest.raises(SeedrMaxTorrentSizeError, match="up to 4GB only"):
        await service.ensure_under_limit((4 * 1024 * 1024 * 1024) + 1)


@pytest.mark.asyncio
async def test_process_job_preserves_known_folder_id_for_download_and_cleanup(tmp_path):
    runner = QueueRunner.__new__(QueueRunner)

    downloaded_file = tmp_path / "video.mp4"
    downloaded_file.write_bytes(b"test")

    job = SimpleNamespace(
        id=1,
        phase=JobPhase.QUEUED,
        magnet_link="magnet:?xt=urn:btih:abc",
        torrent_file_path=None,
        seedr_torrent_id=None,
        seedr_folder_id=777,
        seedr_folder_name="known-folder",
        total_size_bytes=None,
        created_by_user_id=None,
        source_chat_id=123,
    )

    class _Repo:
        async def get_job(self, _job_id: int):
            return job

        async def get_upload_settings(self):
            return None

        async def get_user_settings(self, _user_id: int):
            return None

        async def renumber_queue(self):
            return None

    download_folder_args: list[int | None] = []
    cleanup_jobs: list[SimpleNamespace] = []

    async def _noop(*_args, **_kwargs):
        return None

    async def fake_transition(_job_id: int, **updates):
        for key, value in updates.items():
            setattr(job, key, value)
        return job

    async def fake_wait_for_seedr(_job_id: int, _torrent_id: int | None):
        return SimpleNamespace(
            title="Example",
            total_size_bytes=downloaded_file.stat().st_size,
            seedr_folder_id=None,
            seedr_folder_name=None,
            is_complete=True,
        )

    async def fake_fetch_remote_files_with_retry(folder_id: int | None):
        download_folder_args.append(folder_id)
        return [SimpleNamespace(name="video.mp4", size=downloaded_file.stat().st_size)]

    async def fake_download_files(*_args, **_kwargs):
        return [downloaded_file]

    async def fake_cleanup_seedr_artifacts(cleanup_job):
        cleanup_jobs.append(cleanup_job)

    async def fake_add_seedr_torrent_for_job(_job):
        return 55

    object.__setattr__(runner, "_settings", SimpleNamespace(
        download_root=tmp_path,
        download_concurrency=1,
        upload_concurrency=1,
        upload_part_size_kb=512,
        upload_max_retries=1,
    ))
    object.__setattr__(runner, "_repository", _Repo())
    object.__setattr__(runner, "_seedr_service", SimpleNamespace(ensure_under_limit=_noop))
    object.__setattr__(runner, "_uploader", SimpleNamespace(upload_files=_noop))
    object.__setattr__(runner, "_downloader", SimpleNamespace(download_files=fake_download_files))
    object.__setattr__(runner, "_seedr_stage_semaphore", asyncio.Semaphore(1))
    object.__setattr__(runner, "_cancel_processed_jobs", set())

    object.__setattr__(runner, "_transition", fake_transition)
    object.__setattr__(runner, "_sync_admin_message", _noop)
    object.__setattr__(runner, "_add_seedr_torrent_for_job", fake_add_seedr_torrent_for_job)
    object.__setattr__(runner, "_wait_for_seedr", fake_wait_for_seedr)
    object.__setattr__(runner, "_fetch_remote_files_with_retry", fake_fetch_remote_files_with_retry)
    object.__setattr__(runner, "_cleanup_seedr_artifacts", fake_cleanup_seedr_artifacts)
    object.__setattr__(runner, "_post_task_outcome", _noop)

    await runner._process_job(1)

    assert download_folder_args == [777]
    assert len(cleanup_jobs) == 1
    assert cleanup_jobs[0].seedr_folder_id == 777
