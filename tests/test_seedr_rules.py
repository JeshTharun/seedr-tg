import pytest

from seedr_tg.config import Settings
from seedr_tg.seedr.client import SeedrService


@pytest.mark.asyncio
async def test_ensure_under_limit_raises() -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="token",
        TELEGRAM_API_ID=1,
        TELEGRAM_API_HASH="hash",
        TELEGRAM_PHONE_NUMBER="+1000000",
        TELEGRAM_SOURCE_CHAT_ID=1,
        TELEGRAM_TARGET_CHAT_ID=2,
        TELEGRAM_ADMIN_CHAT_ID=3,
        SEEDR_TOKEN_JSON='{"access_token":"x"}',
    )
    service = SeedrService(settings)
    with pytest.raises(ValueError):
        await service.ensure_under_limit(settings.max_seedr_file_size_bytes + 1)
