from __future__ import annotations

from seedr_tg.telegram.uploader import TelegramUploader


def test_build_telegram_filename_strips_leading_1tamilmv_prefix():
    result = TelegramUploader._build_telegram_filename(
        "www.1TamilMV.immo - Sabdham (2025) Tamil HQ HDRip - x264 - AAC - 400MB - ESub.mkv"
    )

    assert result == "Sabdham_2025_Tamil_HQ_HDRip_x264_AAC_400MB_ESub.mkv"


def test_build_telegram_filename_strips_leading_1tamilmv_for_other_domains():
    io_result = TelegramUploader._build_telegram_filename(
        "www.1TamilMV.io - Movie Title (2026).mp4"
    )
    com_result = TelegramUploader._build_telegram_filename(
        "www.1TamilMV.com - Another Title (2024).mkv"
    )

    assert io_result == "Movie_Title_2026.mp4"
    assert com_result == "Another_Title_2024.mkv"


def test_build_telegram_filename_replaces_spaces_and_special_chars_with_underscores():
    result = TelegramUploader._build_telegram_filename("My cool*movie:2026! final cut?.mp4")

    assert result == "My_cool_movie_2026_final_cut.mp4"


def test_build_telegram_filename_truncates_stem_to_fit_60_chars_including_extension():
    result = TelegramUploader._build_telegram_filename("A" * 80 + ".mkv")

    assert result.endswith(".mkv")
    assert len(result) == 60
    assert result == ("A" * 56) + ".mkv"
