from __future__ import annotations

from seedr_tg.telegram.uploader import TelegramUploader


def test_build_telegram_filename_strips_leading_1tamilmv_prefix():
    result = TelegramUploader._build_telegram_filename(
        "www.1TamilMV.immo - Sabdham (2025) Tamil HQ HDRip - x264 - AAC - 400MB - ESub.mkv"
    )

    assert result == "Sabdham (2025) Tamil HQ HDRip - x264 - AAC - 400MB - ESu.mkv"


def test_build_telegram_filename_strips_leading_1tamilmv_for_other_domains():
    io_result = TelegramUploader._build_telegram_filename(
        "www.1TamilMV.io - Movie Title (2026).mp4"
    )
    com_result = TelegramUploader._build_telegram_filename(
        "www.1TamilMV.com - Another Title (2024).mkv"
    )

    assert io_result == "Movie Title (2026).mp4"
    assert com_result == "Another Title (2024).mkv"


def test_build_telegram_filename_replaces_invalid_chars_without_forcing_underscores():
    result = TelegramUploader._build_telegram_filename("My cool*movie:2026! final cut?.mp4")

    assert result == "My cool movie 2026! final cut.mp4"


def test_build_telegram_filename_truncates_stem_to_fit_60_chars_including_extension():
    result = TelegramUploader._build_telegram_filename("A" * 80 + ".mkv")

    assert result.endswith(".mkv")
    assert len(result) == 60
    assert result == ("A" * 56) + ".mkv"
