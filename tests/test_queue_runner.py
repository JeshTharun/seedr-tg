from pathlib import Path

import pytest

from seedr_tg.config import Settings
from seedr_tg.db.models import JobPhase
from seedr_tg.db.repository import JobRepository


@pytest.mark.asyncio
async def test_repository_enqueues_in_order(tmp_path: Path) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="token",
        TELEGRAM_API_ID=1,
        TELEGRAM_API_HASH="hash",
        TELEGRAM_PHONE_NUMBER="+1000000",
        TELEGRAM_SOURCE_CHAT_ID=1,
        TELEGRAM_TARGET_CHAT_ID=2,
        TELEGRAM_ADMIN_CHAT_ID=3,
        SEEDR_TOKEN_JSON='{"access_token":"x"}',
        DATABASE_PATH=tmp_path / "jobs.sqlite3",
    )
    repository = JobRepository(settings.database_path)
    await repository.initialize()

    first = await repository.enqueue_job(
        magnet_link="magnet:?xt=urn:btih:1",
        source_chat_id=1,
        source_message_id=10,
        target_chat_id=2,
    )
    second = await repository.enqueue_job(
        magnet_link="magnet:?xt=urn:btih:2",
        source_chat_id=1,
        source_message_id=11,
        target_chat_id=2,
    )

    assert first.queue_position == 1
    assert second.queue_position == 2

    await repository.update_job(first.id, phase=JobPhase.COMPLETED)
    await repository.renumber_queue()
    jobs = await repository.list_jobs(include_final=False)
    assert jobs[0].id == second.id
    assert jobs[0].queue_position == 1
