from __future__ import annotations

import asyncio

import httpx
import pytest

from seedr_tg.seedr.client import DownloadIntegrityError, RemoteFile, SeedrService
from seedr_tg.worker.downloads import LocalDownloader
from seedr_tg.worker.queue_runner import QueueRunner


class _FakeSeedrService:
    def __init__(self) -> None:
        self.cancelled_slow_task = asyncio.Event()

    async def download_file(self, url: str, destination, progress_hook=None) -> None:
        if url.endswith("fast-fail"):
            raise RuntimeError("boom")
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            self.cancelled_slow_task.set()
            raise


@pytest.mark.asyncio
async def test_download_files_cancels_sibling_tasks_on_failure(tmp_path):
    service = _FakeSeedrService()
    downloader = LocalDownloader(service)  # type: ignore[arg-type]
    files = [
        RemoteFile(name="a.bin", size=1, download_url="https://x/fast-fail"),
        RemoteFile(name="b.bin", size=1, download_url="https://x/slow"),
    ]

    with pytest.raises(RuntimeError, match="boom"):
        await downloader.download_files(files, tmp_path, concurrency=2)

    assert service.cancelled_slow_task.is_set()


def test_download_retryable_error_classification():
    assert SeedrService._is_retryable_download_error(DownloadIntegrityError("mismatch"))

    request = httpx.Request("GET", "https://example.com/file")
    response = httpx.Response(500, request=request)
    status_error = httpx.HTTPStatusError("server error", request=request, response=response)
    assert SeedrService._is_retryable_download_error(status_error)

    assert not SeedrService._is_retryable_download_error(ValueError("bad"))


def test_cleanup_partial_download_artifacts_removes_target_and_aria2_state(tmp_path):
    target = tmp_path / "video.mp4"
    sidecar = tmp_path / "video.mp4.aria2"
    target.write_bytes(b"abc")
    sidecar.write_text("state")

    SeedrService._cleanup_partial_download_artifacts(target)

    assert not target.exists()
    assert not sidecar.exists()


def test_format_failure_reason_uses_exception_and_cause():
    root = OSError("socket reset")
    exc = RuntimeError("")
    exc.__cause__ = root

    reason = QueueRunner._format_failure_reason(exc)

    assert "RuntimeError" in reason
    assert "OSError" in reason
    assert "socket reset" in reason
