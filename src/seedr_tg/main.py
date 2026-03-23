from __future__ import annotations

import asyncio
import contextlib
import importlib
import signal
import time

from seedr_tg.config import load_settings
from seedr_tg.db.models import CaptionParseMode, UploadMediaType
from seedr_tg.db.repository import JobRepository
from seedr_tg.direct.downloader import DirectDownloader
from seedr_tg.direct.handler import DirectDownloadCommandHandler
from seedr_tg.direct.renamer import FilenameRenamer
from seedr_tg.direct.telegram_uploader import DirectTelegramUploader
from seedr_tg.logging import configure_logging
from seedr_tg.seedr.client import SeedrService
from seedr_tg.telegram.bot_app import TelegramBotApp
from seedr_tg.telegram.media_rename import TelegramMediaRenameHandler
from seedr_tg.telegram.uploader import TelegramUploader
from seedr_tg.web.api import WebApiConfig, WebApiServer
from seedr_tg.worker.queue_runner import QueueRunner


async def run() -> None:
    bot_start_time = time.time()
    settings = load_settings()
    configure_logging(settings.log_level)

    repository = JobRepository(settings.mongodb_uri, settings.mongodb_database)
    await repository.initialize()

    seedr_service = SeedrService(settings, repository)
    await seedr_service.start()

    uploader = TelegramUploader(
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash,
        bot_token=settings.telegram_bot_token,
        target_chat_id=settings.telegram_target_chat_id,
        repository=repository,
        bootstrap_session_string=settings.telegram_user_session_string,
        upload_retry_base_delay_seconds=settings.upload_retry_base_delay_seconds,
        upload_retry_max_delay_seconds=settings.upload_retry_max_delay_seconds,
        upload_governor_enabled=settings.upload_governor_enabled,
        upload_governor_min_concurrency=settings.upload_governor_min_concurrency,
        upload_governor_scale_up_after_stable_files=settings.upload_governor_scale_up_after_stable_files,
    )
    await uploader.start()

    queue_runner: QueueRunner | None = None

    async def enqueue_callback(
        magnet: str,
        chat_id: int,
        message_id: int,
        user_id: int | None,
        username: str | None,
        display_name: str | None,
    ):
        assert queue_runner is not None
        return await queue_runner.enqueue_magnet(
            magnet,
            chat_id,
            message_id,
            user_id,
            username,
            display_name,
        )

    async def list_jobs_callback():
        assert queue_runner is not None
        return await queue_runner.list_jobs()

    async def cancel_callback(job_id: int):
        assert queue_runner is not None
        return await queue_runner.request_cancel(job_id)

    async def set_admin_message_id_callback(job_id: int, admin_message_id: int):
        return await repository.update_job(job_id, admin_message_id=admin_message_id)

    async def start_seedr_auth_callback():
        return await seedr_service.begin_device_authorization()

    async def complete_seedr_auth_callback():
        return await seedr_service.complete_device_authorization()

    async def start_user_session_callback(phone_number: str):
        return await uploader.begin_login(phone_number)

    async def submit_user_session_code_callback(code: str):
        return await uploader.complete_login_with_code(code)

    async def submit_user_session_password_callback(password: str):
        return await uploader.complete_login_with_password(password)

    async def get_upload_settings_callback():
        return await repository.get_upload_settings()

    async def update_upload_settings_callback(**updates):
        normalized = dict(updates)
        if "media_type" in normalized and normalized["media_type"] is not None:
            normalized["media_type"] = UploadMediaType(str(normalized["media_type"]))
        if "caption_parse_mode" in normalized and normalized["caption_parse_mode"] is not None:
            normalized["caption_parse_mode"] = CaptionParseMode(
                str(normalized["caption_parse_mode"])
            )
        return await repository.update_upload_settings(**normalized)

    async def reset_upload_settings_callback():
        return await repository.reset_upload_settings()

    direct_handler: DirectDownloadCommandHandler | None = None
    media_rename_handler: TelegramMediaRenameHandler | None = None

    async def direct_download_handler(update, context):
        assert direct_handler is not None
        await direct_handler.handle(update, context)

    async def telegram_media_rename_handler(update, context):
        assert media_rename_handler is not None
        await media_rename_handler.handle(update, context)

    direct_downloader = DirectDownloader(
        connect_timeout_seconds=settings.download_connect_timeout_seconds,
        read_timeout_seconds=settings.download_read_timeout_seconds,
        write_timeout_seconds=settings.download_write_timeout_seconds,
        pool_timeout_seconds=settings.download_pool_timeout_seconds,
        chunk_size_bytes=settings.direct_download_chunk_size_bytes,
        max_retries=settings.download_max_retries,
        retry_base_delay_seconds=settings.download_retry_base_delay_seconds,
        retry_max_delay_seconds=settings.download_retry_max_delay_seconds,
    )
    direct_renamer = FilenameRenamer(max_filename_bytes=settings.direct_filename_max_bytes)
    direct_uploader = DirectTelegramUploader()
    bot_app = TelegramBotApp(
        token=settings.telegram_bot_token,
        source_chat_id=settings.telegram_source_chat_id,
        admin_chat_id=settings.telegram_admin_chat_id,
        enqueue_callback=enqueue_callback,
        list_jobs_callback=list_jobs_callback,
        cancel_callback=cancel_callback,
        set_admin_message_id_callback=set_admin_message_id_callback,
        start_seedr_auth_callback=start_seedr_auth_callback,
        complete_seedr_auth_callback=complete_seedr_auth_callback,
        start_user_session_callback=start_user_session_callback,
        submit_user_session_code_callback=submit_user_session_code_callback,
        submit_user_session_password_callback=submit_user_session_password_callback,
        get_upload_settings_callback=get_upload_settings_callback,
        update_upload_settings_callback=update_upload_settings_callback,
        reset_upload_settings_callback=reset_upload_settings_callback,
        direct_download_handler=direct_download_handler,
        telegram_media_rename_handler=telegram_media_rename_handler,
        status_download_dir=settings.download_root,
        bot_start_time=bot_start_time,
    )
    direct_handler = DirectDownloadCommandHandler(
        downloader=direct_downloader,
        renamer=direct_renamer,
        uploader=direct_uploader,
        repository=repository,
        download_root=settings.download_root,
        allowed_chat_ids={settings.telegram_source_chat_id, settings.telegram_admin_chat_id},
        bot_start_time=bot_start_time,
        register_active_task_callback=bot_app.register_active_task,
        update_active_task_callback=bot_app.update_active_task,
        unregister_active_task_callback=bot_app.unregister_active_task,
    )
    media_rename_handler = TelegramMediaRenameHandler(
        uploader=uploader,
        repository=repository,
        renamer=direct_renamer,
        download_root=settings.download_root,
        allowed_chat_ids={settings.telegram_source_chat_id, settings.telegram_admin_chat_id},
        bot_start_time=bot_start_time,
        max_concurrent_tasks=settings.rename_concurrency,
        register_active_task_callback=bot_app.register_active_task,
        update_active_task_callback=bot_app.update_active_task,
        unregister_active_task_callback=bot_app.unregister_active_task,
    )
    queue_runner = QueueRunner(
        settings=settings,
        repository=repository,
        bot_app=bot_app,
        seedr_service=seedr_service,
        uploader=uploader,
        bot_start_time=bot_start_time,
    )
    web_api = WebApiServer(
        enqueue_callback=queue_runner.enqueue_magnet,
        list_jobs_callback=queue_runner.list_jobs,
        config=WebApiConfig(allowed_origins=settings.web_api_allowed_origins),
    )

    stop_event = asyncio.Event()

    def handle_stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_stop)

    await bot_app.start()
    await web_api.start()
    worker_task = asyncio.create_task(queue_runner.run())
    try:
        await stop_event.wait()
    finally:
        with contextlib.suppress(Exception):
            await queue_runner.stop()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        with contextlib.suppress(Exception):
            await web_api.stop()
        with contextlib.suppress(Exception):
            await bot_app.stop()
        with contextlib.suppress(Exception):
            await uploader.stop()
        with contextlib.suppress(Exception):
            await seedr_service.stop()
        with contextlib.suppress(Exception):
            await repository.close()


def main() -> None:
    settings = load_settings()
    use_uvloop = settings.use_uvloop
    if use_uvloop:
        with contextlib.suppress(Exception):
            uvloop = importlib.import_module("uvloop")
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(run())


if __name__ == "__main__":
    main()
