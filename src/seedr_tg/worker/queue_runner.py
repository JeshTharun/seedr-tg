from __future__ import annotations

import asyncio
import contextlib
import logging
import shutil
import time

from seedr_tg.config import Settings
from seedr_tg.db.models import FINAL_PHASES, JobPhase, JobRecord
from seedr_tg.db.repository import JobRepository
from seedr_tg.seedr.client import SeedrService
from seedr_tg.seedr.poller import SeedrPoller
from seedr_tg.status.template import collect_bot_stats
from seedr_tg.telegram.bot_app import TelegramBotApp
from seedr_tg.telegram.uploader import TelegramUploader
from seedr_tg.worker.downloads import LocalDownloader
from seedr_tg.worker.progress import format_job_status

LOGGER = logging.getLogger(__name__)
_ALLOWED_UPLOAD_EXTENSIONS = {".mp4", ".mkv", ".zip"}


class QueueRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        repository: JobRepository,
        bot_app: TelegramBotApp,
        seedr_service: SeedrService,
        uploader: TelegramUploader,
        bot_start_time: float,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._bot_app = bot_app
        self._seedr_service = seedr_service
        self._uploader = uploader
        self._bot_start_time = float(bot_start_time)
        self._poller = SeedrPoller(seedr_service)
        self._downloader = LocalDownloader(seedr_service)
        self._stop_event = asyncio.Event()
        self._wake_event = asyncio.Event()
        self._job_tasks: dict[int, asyncio.Task[None]] = {}
        self._queue_concurrency = max(1, int(settings.queue_concurrency))
        self._seedr_stage_semaphore = asyncio.Semaphore(
            max(1, int(settings.seedr_active_concurrency))
        )
        self._last_progress_sync_at: dict[tuple[int, str], float] = {}
        self._speed_samples: dict[tuple[int, str], tuple[float, int]] = {}
        self._cancel_processed_jobs: set[int] = set()

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
            self._collect_finished_tasks()
            await self._repository.renumber_queue()
            available_slots = self._queue_concurrency - len(self._job_tasks)
            claimed_any = False
            for _ in range(max(0, available_slots)):
                claimed = await self._repository.claim_next_queued_job()
                if claimed is None:
                    break
                claimed_any = True
                task = asyncio.create_task(self._run_job_task(claimed.id), name=f"job-{claimed.id}")
                self._job_tasks[claimed.id] = task

            if claimed_any:
                continue

            if self._job_tasks:
                done, _ = await asyncio.wait(
                    self._job_tasks.values(),
                    timeout=self._settings.poll_interval_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if done:
                    self._collect_finished_tasks()
                continue

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
            await asyncio.sleep(0.05)

        for task in self._job_tasks.values():
            task.cancel()
        if self._job_tasks:
            await asyncio.gather(*self._job_tasks.values(), return_exceptions=True)
        self._job_tasks.clear()

    async def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()

    def _collect_finished_tasks(self) -> None:
        for job_id, task in list(self._job_tasks.items()):
            if not task.done():
                continue
            self._job_tasks.pop(job_id, None)
            with contextlib.suppress(asyncio.CancelledError):
                exc = task.exception()
                if exc is not None:
                    LOGGER.exception("Job %s failed", job_id, exc_info=exc)
                    asyncio.create_task(
                        self._mark_failed(job_id, self._format_failure_reason(exc))
                    )

    async def _run_job_task(self, job_id: int) -> None:
        try:
            await self._process_job(job_id)
        except asyncio.CancelledError:
            LOGGER.info("Job %s was canceled", job_id)
            raise

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
        job = await self._repository.get_job(job_id)
        if job.phase == JobPhase.QUEUED:
            job = await self._transition(
                job_id,
                phase=JobPhase.VALIDATING,
                current_step="Queued for Seedr",
            )
        await self._sync_admin_message(job)

        async with self._seedr_stage_semaphore:
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
                download_speed_bps=0.0,
                upload_speed_bps=0.0,
                current_step="Downloading files from Seedr to local disk",
            )
            await self._sync_admin_message(job)

            remote_files = await self._fetch_remote_files_with_retry(snapshot.seedr_folder_id)
            if not remote_files:
                raise RuntimeError("Seedr finished torrent without downloadable files")
            local_root = self._settings.download_root / f"job_{job.id}"
            file_paths = await self._downloader.download_files(
                remote_files,
                local_root,
                concurrency=self._settings.download_concurrency,
                progress_hook=lambda current, total, name: self._update_progress(
                    job_id,
                    JobPhase.DOWNLOADING_LOCAL,
                    current,
                    total,
                    f"Downloading {name}",
                ),
            )

            if snapshot.seedr_folder_id is not None:
                await self._seedr_service.delete_folder(snapshot.seedr_folder_id)
            else:
                await self._seedr_service.delete_torrent(torrent_id)
        upload_file_paths = [
            path for path in file_paths if path.suffix.lower() in _ALLOWED_UPLOAD_EXTENSIONS
        ]
        skipped_file_names = [
            path.name
            for path in file_paths
            if path.suffix.lower() not in _ALLOWED_UPLOAD_EXTENSIONS
        ]
        if skipped_file_names:
            LOGGER.info(
                "Skipping %s file(s) due to extension filter: %s",
                len(skipped_file_names),
                ", ".join(skipped_file_names),
            )
        if not upload_file_paths:
            allowed = ", ".join(sorted(_ALLOWED_UPLOAD_EXTENSIONS))
            raise RuntimeError(
                f"No uploadable files found after extension filter. Allowed extensions: {allowed}"
            )

        job = await self._transition(
            job_id,
            phase=JobPhase.UPLOADING_TELEGRAM,
            local_path=str(local_root),
            upload_file_count=len(upload_file_paths),
            uploaded_file_count=0,
            download_speed_bps=0.0,
            upload_speed_bps=0.0,
            current_step="Uploading files to Telegram",
        )
        await self._sync_admin_message(job)

        async def upload_progress_hook(
            index: int,
            total_files: int,
            detail: str,
            current_bytes: int,
            total_bytes: int,
        ) -> None:
            await self._track_upload_progress(
                job_id,
                index,
                total_files,
                detail,
                current_bytes,
                total_bytes,
            )

        await self._uploader.upload_files(
            upload_file_paths,
            caption_prefix=snapshot.title or f"Job {job.id}",
            job_id=job.id,
            upload_settings=await self._repository.get_upload_settings(),
            max_concurrent_uploads=self._settings.upload_concurrency,
            upload_part_size_kb=self._settings.upload_part_size_kb,
            upload_max_retries=self._settings.upload_max_retries,
            progress_hook=upload_progress_hook,
        )

        job = await self._transition(
            job_id,
            phase=JobPhase.CLEANING,
            progress_percent=100.0,
            uploaded_file_count=len(upload_file_paths),
            download_speed_bps=0.0,
            upload_speed_bps=0.0,
            current_step="Cleaning local files",
        )
        await self._sync_admin_message(job)
        await asyncio.to_thread(shutil.rmtree, local_root, True)

        job = await self._transition(
            job_id,
            phase=JobPhase.COMPLETED,
            current_step="Completed",
            progress_percent=100.0,
            download_speed_bps=0.0,
            upload_speed_bps=0.0,
        )
        await self._sync_admin_message(job)
        await self._repository.renumber_queue()

    async def _wait_for_seedr(self, job_id: int, torrent_id: int | None):
        while True:
            await self._check_cancellation(job_id)
            job = await self._repository.get_job(job_id)
            known_folder_id = job.seedr_folder_id if job else None
            snapshot = await self._poller.poll(torrent_id, known_folder_id=known_folder_id)
            job = await self._transition(
                job_id,
                phase=JobPhase.DOWNLOADING_SEEDR,
                torrent_name=snapshot.title,
                total_size_bytes=snapshot.total_size_bytes,
                seedr_folder_id=snapshot.seedr_folder_id or known_folder_id,
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
        download_speed_bps = self._compute_speed(job_id, "download", current)
        job = await self._transition(
            job_id,
            phase=phase,
            progress_percent=percent,
            download_speed_bps=download_speed_bps,
            current_step=step,
        )
        if self._should_sync_progress(job_id, phase, percent):
            await self._sync_admin_message_best_effort(job)
        await self._check_cancellation(job_id)

    async def _track_upload_progress(
        self,
        job_id: int,
        completed_files: int,
        total_files: int,
        detail: str,
        current_bytes: int,
        total_bytes: int,
    ) -> None:
        file_fraction = 0.0
        if total_bytes > 0:
            file_fraction = min(1.0, max(0.0, current_bytes / total_bytes))
        overall_units = completed_files + file_fraction
        percent = (overall_units / total_files) * 100 if total_files else 100.0
        upload_speed_bps = self._compute_speed(
            job_id,
            "upload",
            current_bytes,
        )
        job = await self._transition(
            job_id,
            phase=JobPhase.UPLOADING_TELEGRAM,
            progress_percent=min(percent, 100.0),
            uploaded_file_count=min(completed_files, total_files),
            upload_file_count=total_files,
            upload_speed_bps=upload_speed_bps,
            current_step=detail,
        )
        if self._should_sync_progress(job_id, JobPhase.UPLOADING_TELEGRAM, percent):
            await self._sync_admin_message_best_effort(job)
        await self._check_cancellation(job_id)

    async def _fetch_remote_files_with_retry(self, folder_id: int | None):
        # Seedr can report completion slightly before folder/file listings fully propagate.
        max_attempts = 10
        retry_delay_seconds = 3
        for attempt in range(1, max_attempts + 1):
            files = await self._seedr_service.fetch_remote_files(folder_id)
            if files:
                return files
            if folder_id is not None:
                root_files = await self._seedr_service.fetch_remote_files(None)
                if root_files:
                    LOGGER.info(
                        (
                            "Recovered downloadable files from Seedr root "
                            "on attempt %s for folder_id=%s"
                        ),
                        attempt,
                        folder_id,
                    )
                    return root_files
            if attempt < max_attempts:
                LOGGER.info(
                    "Seedr completed but files not visible yet (attempt %s/%s). Retrying in %ss",
                    attempt,
                    max_attempts,
                    retry_delay_seconds,
                )
                await asyncio.sleep(retry_delay_seconds)
        return []

    async def _check_cancellation(self, job_id: int) -> None:
        job = await self._repository.get_job(job_id)
        if not job.cancel_requested:
            return
        if job_id in self._cancel_processed_jobs or job.phase == JobPhase.CANCELED:
            raise asyncio.CancelledError(f"Job {job_id} canceled")

        self._cancel_processed_jobs.add(job_id)
        await self._cleanup_seedr_artifacts(job)
        await self._cleanup_local_artifacts(job)
        canceled = await self._transition(
            job_id,
            phase=JobPhase.CANCELED,
            current_step="Canceled by operator",
            failure_reason="Cancellation requested",
        )
        await self._sync_admin_message(canceled)
        await self._repository.delete_job(job_id)
        await self._repository.renumber_queue()
        raise asyncio.CancelledError(f"Job {job_id} canceled")

    async def _mark_failed(self, job_id: int, reason: str) -> None:
        try:
            job = await self._repository.get_job(job_id)
        except LookupError:
            return
        await self._cleanup_seedr_artifacts(job)
        await self._cleanup_local_artifacts(job)
        failed = await self._transition(
            job_id,
            phase=JobPhase.FAILED,
            failure_reason=reason,
            last_error=reason,
            current_step="Failed",
        )
        self._speed_samples.pop((job_id, "download"), None)
        self._speed_samples.pop((job_id, "upload"), None)
        await self._sync_admin_message(failed)

    @staticmethod
    def _format_failure_reason(exc: Exception) -> str:
        message = str(exc).strip()
        if message:
            return f"{exc.__class__.__name__}: {message}"
        cause = exc.__cause__
        if cause is None:
            return exc.__class__.__name__
        cause_message = str(cause).strip()
        if cause_message:
            return (
                f"{exc.__class__.__name__} "
                f"(caused by {cause.__class__.__name__}: {cause_message})"
            )
        return f"{exc.__class__.__name__} (caused by {cause.__class__.__name__})"

    async def _sync_admin_message(self, job: JobRecord) -> None:
        active_jobs = await self._repository.list_jobs(include_final=False)
        aggregate_dl = sum(float(item.download_speed_bps or 0.0) for item in active_jobs)
        aggregate_ul = sum(float(item.upload_speed_bps or 0.0) for item in active_jobs)
        bot_stats = collect_bot_stats(
            download_dir=self._settings.download_root,
            bot_start_time=self._bot_start_time,
            tasks_count=len(active_jobs),
            download_bps=aggregate_dl,
            upload_bps=aggregate_ul,
        )
        text = format_job_status(job, bot_stats=bot_stats)
        if job.admin_message_id is None:
            # Avoid duplicate status messages when bot message creation and worker updates race.
            latest = await self._repository.get_job(job.id)
            if latest.admin_message_id is None:
                await asyncio.sleep(0.25)
                latest = await self._repository.get_job(job.id)
            if latest.admin_message_id is not None:
                await self._bot_app.update_admin_message(
                    latest.admin_message_id,
                    text,
                    None if job.phase in FINAL_PHASES else job.id,
                )
                return
            post_job_id = job.id if job.phase not in FINAL_PHASES else None
            message_id = await self._bot_app.post_admin_message(text, post_job_id)
            await self._repository.update_job(job.id, admin_message_id=message_id)
            return
        await self._bot_app.update_admin_message(
            job.admin_message_id,
            text,
            None if job.phase in FINAL_PHASES else job.id,
        )

    async def _sync_admin_message_best_effort(self, job: JobRecord) -> None:
        try:
            await self._sync_admin_message(job)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Skipped admin message update due to transient error: %s", exc)

    def _should_sync_progress(self, job_id: int, phase: JobPhase, percent: float) -> bool:
        if percent >= 100.0:
            return True
        key = (job_id, phase.value)
        now = time.monotonic()
        last = self._last_progress_sync_at.get(key)
        if last is not None and (now - last) < self._settings.progress_update_interval_seconds:
            return False
        self._last_progress_sync_at[key] = now
        return True

    def _compute_speed(self, job_id: int, channel: str, current_bytes: int) -> float:
        key = (job_id, channel)
        now = time.monotonic()
        previous = self._speed_samples.get(key)
        self._speed_samples[key] = (now, int(current_bytes))
        if previous is None:
            return 0.0
        prev_time, prev_bytes = previous
        if current_bytes < prev_bytes:
            return 0.0
        elapsed = now - prev_time
        if elapsed <= 0:
            return 0.0
        return float(current_bytes - prev_bytes) / elapsed

    async def _transition(self, job_id: int, **updates) -> JobRecord:
        try:
            job = await self._repository.update_job(job_id, **updates)
        except LookupError as exc:
            if job_id in self._cancel_processed_jobs:
                raise asyncio.CancelledError(f"Job {job_id} canceled") from exc
            raise
        if job.phase in FINAL_PHASES:
            self._speed_samples.pop((job_id, "download"), None)
            self._speed_samples.pop((job_id, "upload"), None)
            self._last_progress_sync_at = {
                key: value
                for key, value in self._last_progress_sync_at.items()
                if key[0] != job_id
            }
        return job

    async def _cleanup_seedr_artifacts(self, job: JobRecord) -> None:
        folder_id = job.seedr_folder_id
        if folder_id is None and job.seedr_torrent_id is not None:
            with contextlib.suppress(Exception):
                resolved = await self._seedr_service.resolve_torrent(
                    job.seedr_torrent_id,
                    known_folder_id=None,
                )
                if resolved.folder is not None:
                    folder_id = int(resolved.folder.id)

        with contextlib.suppress(Exception):
            await self._seedr_service.delete_folder(folder_id)
        with contextlib.suppress(Exception):
            await self._seedr_service.delete_torrent(job.seedr_torrent_id)

    async def _cleanup_local_artifacts(self, job: JobRecord) -> None:
        if job.local_path:
            await asyncio.to_thread(shutil.rmtree, job.local_path, True)
        local_root = self._settings.download_root / f"job_{job.id}"
        await asyncio.to_thread(shutil.rmtree, local_root, True)
