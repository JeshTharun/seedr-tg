from __future__ import annotations

import asyncio
import contextlib
import importlib
import signal

from seedr_tg.config import load_settings
from seedr_tg.db.models import CaptionParseMode, UploadMediaType
from seedr_tg.db.repository import JobRepository
from seedr_tg.logging import configure_logging
from seedr_tg.seedr.client import SeedrService
from seedr_tg.telegram.bot_app import TelegramBotApp
from seedr_tg.telegram.uploader import TelegramUploader
from seedr_tg.web.api import WebApiConfig, WebApiServer
from seedr_tg.worker.queue_runner import QueueRunner


async def run() -> None:
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
    )
    await uploader.start()

    queue_runner: QueueRunner | None = None

    async def enqueue_callback(magnet: str, chat_id: int, message_id: int):
        assert queue_runner is not None
        return await queue_runner.enqueue_magnet(magnet, chat_id, message_id)

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
    )
    queue_runner = QueueRunner(
        settings=settings,
        repository=repository,
        bot_app=bot_app,
        seedr_service=seedr_service,
        uploader=uploader,
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
        await queue_runner.stop()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await web_api.stop()
        await bot_app.stop()
        await uploader.stop()
        await seedr_service.stop()
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
