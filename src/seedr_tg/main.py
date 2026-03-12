from __future__ import annotations

import asyncio
import contextlib
import signal

from seedr_tg.config import load_settings
from seedr_tg.db.repository import JobRepository
from seedr_tg.logging import configure_logging
from seedr_tg.seedr.client import SeedrService
from seedr_tg.telegram.bot_app import TelegramBotApp
from seedr_tg.telegram.uploader import TelegramUploader
from seedr_tg.worker.queue_runner import QueueRunner


async def run() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)

    repository = JobRepository(settings.database_path)
    await repository.initialize()

    seedr_service = SeedrService(settings)
    await seedr_service.start()

    uploader = TelegramUploader(
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash,
        phone_number=settings.telegram_phone_number,
        session_name=settings.telegram_session_name,
        target_chat_id=settings.telegram_target_chat_id,
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

    bot_app = TelegramBotApp(
        token=settings.telegram_bot_token,
        source_chat_id=settings.telegram_source_chat_id,
        admin_chat_id=settings.telegram_admin_chat_id,
        enqueue_callback=enqueue_callback,
        list_jobs_callback=list_jobs_callback,
        cancel_callback=cancel_callback,
        set_admin_message_id_callback=set_admin_message_id_callback,
    )
    queue_runner = QueueRunner(
        settings=settings,
        repository=repository,
        bot_app=bot_app,
        seedr_service=seedr_service,
        uploader=uploader,
    )

    stop_event = asyncio.Event()

    def handle_stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_stop)

    await bot_app.start()
    worker_task = asyncio.create_task(queue_runner.run())
    try:
        await stop_event.wait()
    finally:
        await queue_runner.stop()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await bot_app.stop()
        await uploader.stop()
        await seedr_service.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
