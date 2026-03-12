from __future__ import annotations

import asyncio
import contextlib
import logging
import shutil

from seedr_tg.config import Settings
from seedr_tg.db.models import FINAL_PHASES, JobPhase, JobRecord
from seedr_tg.db.repository import JobRepository
from seedr_tg.seedr.client import SeedrService
from seedr_tg.seedr.poller import SeedrPoller
from seedr_tg.telegram.bot_app import TelegramBotApp
from seedr_tg.telegram.uploader import TelegramUploader
from seedr_tg.worker.downloads import LocalDownloader
from seedr_tg.worker.progress import format_job_status

LOGGER = logging.getLogger(__name__)


class QueueRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        repository: JobRepository,
        bot_app: TelegramBotApp,
        seedr_service: SeedrService,
        uploader: TelegramUploader,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._bot_app = bot_app
        self._seedr_service = seedr_service
        self._uploader = uploader
        self._poller = SeedrPoller(seedr_service)
        self._downloader = LocalDownloader(seedr_service)
        self._stop_event = asyncio.Event()
        self._wake_event = asyncio.Event()

    async def enqueue_magnet(
        self,
        magnet_link: str,
        source_chat_id: int,
        source_message_id: int,
    ) -> JobRecord | None:
        if await self._repository.has_active_magnet(magnet_link):
            return None
        job = await self._repository.enqueue_job(
            magnet_link=magnet_link,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            target_chat_id=self._settings.telegram_target_chat_id,
        )
        self._wake_event.set()
        return job

    async def list_jobs(self) -> list[JobRecord]:
        await self._repository.renumber_queue()
        return await self._repository.list_jobs(include_final=False)

    async def request_cancel(self, job_id: int) -> JobRecord:
        job = await self._repository.request_cancel(job_id)
        self._wake_event.set()
        return job

    async def run(self) -> None:
        await self._recover_unfinished_jobs()
        while not self._stop_event.is_set():
            await self._repository.renumber_queue()
            job = await self._repository.get_next_job()
            if job is None:
                self._wake_event.clear()
                try:
                    await asyncio.wait_for(
                        self._wake_event.wait(),
                        timeout=self._settings.poll_interval_seconds,
                    )
                except TimeoutError:
                    continue
                continue
            try:
                await self._process_job(job.id)
            except asyncio.CancelledError:
                LOGGER.info("Job %s was canceled", job.id)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Job %s failed", job.id)
                await self._mark_failed(job.id, str(exc))

    async def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()

    async def _recover_unfinished_jobs(self) -> None:
        jobs = await self._repository.list_jobs(include_final=False)
        for job in jobs:
            if job.phase != JobPhase.QUEUED:
                await self._repository.update_job(
                    job.id,
                    phase=JobPhase.QUEUED,
                    current_step="Recovered after restart",
                )

    async def _process_job(self, job_id: int) -> None:
        job = await self._transition(
            job_id,
            phase=JobPhase.VALIDATING,
            current_step="Queued for Seedr",
        )
        await self._sync_admin_message(job)

        torrent_id = await self._seedr_service.add_magnet(job.magnet_link)
        job = await self._transition(
            job_id,
            phase=JobPhase.WAITING_METADATA,
            seedr_torrent_id=torrent_id,
            current_step="Waiting for torrent metadata",
        )
        await self._sync_admin_message(job)

        snapshot = await self._wait_for_seedr(job_id, torrent_id)
        await self._seedr_service.ensure_under_limit(snapshot.total_size_bytes)
        job = await self._transition(
            job_id,
            phase=JobPhase.DOWNLOADING_LOCAL,
            torrent_name=snapshot.title,
            total_size_bytes=snapshot.total_size_bytes,
            seedr_folder_id=snapshot.seedr_folder_id,
            seedr_folder_name=snapshot.seedr_folder_name,
            progress_percent=100.0,
            current_step="Downloading files from Seedr to local disk",
        )
        await self._sync_admin_message(job)

        if snapshot.seedr_folder_id is None:
            raise RuntimeError("Seedr did not create a folder for the finished torrent")
        remote_files = await self._seedr_service.fetch_remote_files(snapshot.seedr_folder_id)
        if not remote_files:
            raise RuntimeError("Seedr finished torrent without downloadable files")
        local_root = self._settings.download_root / f"job_{job.id}"
        file_paths = await self._downloader.download_files(
            remote_files,
            local_root,
            progress_hook=lambda current, total, name: self._update_progress(
                job_id,
                JobPhase.DOWNLOADING_LOCAL,
                current,
                total,
                f"Downloading {name}",
            ),
        )

        await self._seedr_service.delete_folder(snapshot.seedr_folder_id)
        job = await self._transition(
            job_id,
            phase=JobPhase.UPLOADING_TELEGRAM,
            local_path=str(local_root),
            upload_file_count=len(file_paths),
            uploaded_file_count=0,
            current_step="Uploading files to Telegram",
        )
        await self._sync_admin_message(job)

        await self._uploader.upload_files(
            file_paths,
            caption_prefix=snapshot.title or f"Job {job.id}",
            progress_hook=lambda index, total_files, detail: self._track_upload_progress(
                job_id,
                index,
                total_files,
                detail,
            ),
        )

        job = await self._transition(
            job_id,
            phase=JobPhase.CLEANING,
            progress_percent=100.0,
            uploaded_file_count=len(file_paths),
            current_step="Cleaning local files",
        )
        await self._sync_admin_message(job)
        shutil.rmtree(local_root, ignore_errors=True)

        job = await self._transition(
            job_id,
            phase=JobPhase.COMPLETED,
            current_step="Completed",
            progress_percent=100.0,
        )
        await self._sync_admin_message(job)
        await self._repository.renumber_queue()

    async def _wait_for_seedr(self, job_id: int, torrent_id: int | None):
        while True:
            await self._check_cancellation(job_id)
            snapshot = await self._poller.poll(torrent_id)
            job = await self._transition(
                job_id,
                phase=JobPhase.DOWNLOADING_SEEDR,
                torrent_name=snapshot.title,
                total_size_bytes=snapshot.total_size_bytes,
                seedr_folder_id=snapshot.seedr_folder_id,
                seedr_folder_name=snapshot.seedr_folder_name,
                progress_percent=snapshot.progress_percent,
                current_step="Seedr downloading torrent",
            )
            await self._sync_admin_message(job)
            if snapshot.total_size_bytes is not None:
                await self._seedr_service.ensure_under_limit(snapshot.total_size_bytes)
            if snapshot.is_complete:
                return snapshot
            await asyncio.sleep(self._settings.poll_interval_seconds)

    async def _update_progress(
        self,
        job_id: int,
        phase: JobPhase,
        current: int,
        total: int,
        step: str,
    ) -> None:
        percent = 0.0 if total == 0 else (current / total) * 100
        job = await self._transition(
            job_id,
            phase=phase,
            progress_percent=percent,
            current_step=step,
        )
        await self._sync_admin_message(job)
        await self._check_cancellation(job_id)

    async def _track_upload_progress(
        self,
        job_id: int,
        current_file_index: int,
        total_files: int,
        detail: str,
    ) -> None:
        percent = (current_file_index / total_files) * 100 if total_files else 100.0
        job = await self._transition(
            job_id,
            phase=JobPhase.UPLOADING_TELEGRAM,
            progress_percent=percent,
            uploaded_file_count=current_file_index - 1,
            upload_file_count=total_files,
            current_step=detail,
        )
        await self._sync_admin_message(job)
        await self._check_cancellation(job_id)

    async def _check_cancellation(self, job_id: int) -> None:
        job = await self._repository.get_job(job_id)
        if not job.cancel_requested:
            return
        await self._seedr_service.delete_torrent(job.seedr_torrent_id)
        await self._seedr_service.delete_folder(job.seedr_folder_id)
        if job.local_path:
            shutil.rmtree(job.local_path, ignore_errors=True)
        canceled = await self._transition(
            job_id,
            phase=JobPhase.CANCELED,
            current_step="Canceled by operator",
            failure_reason="Cancellation requested",
        )
        await self._sync_admin_message(canceled)
        raise asyncio.CancelledError(f"Job {job_id} canceled")

    async def _mark_failed(self, job_id: int, reason: str) -> None:
        job = await self._repository.get_job(job_id)
        with contextlib.suppress(Exception):
            await self._seedr_service.delete_torrent(job.seedr_torrent_id)
        with contextlib.suppress(Exception):
            await self._seedr_service.delete_folder(job.seedr_folder_id)
        if job.local_path:
            shutil.rmtree(job.local_path, ignore_errors=True)
        failed = await self._transition(
            job_id,
            phase=JobPhase.FAILED,
            failure_reason=reason,
            last_error=reason,
            current_step="Failed",
        )
        await self._sync_admin_message(failed)

    async def _sync_admin_message(self, job: JobRecord) -> None:
        text = format_job_status(job)
        if job.admin_message_id is None:
            post_job_id = job.id if job.phase not in FINAL_PHASES else None
            message_id = await self._bot_app.post_admin_message(text, post_job_id)
            await self._repository.update_job(job.id, admin_message_id=message_id)
            return
        await self._bot_app.update_admin_message(
            job.admin_message_id,
            text,
            None if job.phase in FINAL_PHASES else job.id,
        )

    async def _transition(self, job_id: int, **updates) -> JobRecord:
        return await self._repository.update_job(job_id, **updates)
