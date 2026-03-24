from __future__ import annotations

from seedr_tg.telegram.uploader import TelegramUploader


def test_build_telegram_filename_strips_leading_1tamilmv_prefix():
    result = TelegramUploader._build_telegram_filename(
        "www.1TamilMV.immo - Sabdham (2025) Tamil HQ HDRip - x264 - AAC - 400MB - ESub.mkv"
    )

    assert result == "Sabdham (2025) Tamil HQ HDRip - x264 - AAC - 400MB - ESub.mkv"


def test_build_telegram_filename_strips_leading_1tamilmv_for_other_domains():
    io_result = TelegramUploader._build_telegram_filename(
        "www.1TamilMV.io - Movie Title (2026).mp4"
    )
    com_result = TelegramUploader._build_telegram_filename(
        "www.1TamilMV.com - Another Title (2024).mkv"
    )

    assert io_result == "Movie Title (2026).mp4"
    assert com_result == "Another Title (2024).mkv"
