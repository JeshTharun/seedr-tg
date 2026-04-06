from __future__ import annotations

from pathlib import Path

from seedr_tg.db.models import CaptionParseMode, UploadMediaType, UploadSettings, UserSettings
from seedr_tg.telegram.uploader import TelegramUploader


def _upload_settings() -> UploadSettings:
    return UploadSettings(
        media_type=UploadMediaType.MEDIA,
        caption_template=None,
        caption_parse_mode=CaptionParseMode.HTML,
        thumbnail_file_id=None,
        thumbnail_base64=None,
        created_at="2026-03-25T00:00:00+00:00",
        updated_at="2026-03-25T00:00:00+00:00",
    )


def _user_settings(caption_template: str | None) -> UserSettings:
    return UserSettings(
        user_id=123,
        caption_template=caption_template,
        thumbnail_file_id=None,
        thumbnail_base64=None,
        created_at="2026-03-25T00:00:00+00:00",
        updated_at="2026-03-25T00:00:00+00:00",
    )


def test_render_caption_prefers_user_template_from_mysettings():
    render_caption = getattr(TelegramUploader, "_render_caption")
    caption, parse_mode = render_caption(
        file_path=Path("Movie.mkv"),
        caption_prefix="Torrent Name",
        job_id=12,
        upload_settings=_upload_settings(),
        user_settings=_user_settings("User: {filename} | Job: {job_id}"),
    )

    assert parse_mode == "html"
    assert caption == "User: Movie.mkv | Job: 12"


def test_render_caption_accepts_common_alias_placeholders():
    render_caption = getattr(TelegramUploader, "_render_caption")
    caption, parse_mode = render_caption(
        file_path=Path("Episode 01.mkv"),
        caption_prefix="Show S01",
        job_id=7,
        upload_settings=_upload_settings(),
        user_settings=_user_settings("{file_name} - {title} - #{job}"),
    )

    assert parse_mode == "html"
    assert caption == "Episode 01.mkv - Show S01 - #7"


def test_render_caption_keeps_unknown_placeholders_without_fallback():
    render_caption = getattr(TelegramUploader, "_render_caption")
    caption, parse_mode = render_caption(
        file_path=Path("Track.flac"),
        caption_prefix="Album",
        job_id=3,
        upload_settings=_upload_settings(),
        user_settings=_user_settings("{filename} | {unknown_field}"),
    )

    assert parse_mode == "html"
    assert caption == "Track.flac | {unknown_field}"


def test_render_caption_strips_leading_1tamilmv_prefix_from_display_filename():
    render_caption = getattr(TelegramUploader, "_render_caption")
    caption, parse_mode = render_caption(
        file_path=Path(
            "www.1TamilMV.cymru - Repu Udayam 10 Gantalaku (2026) Tamil HQ HDRip - x264 - AAC - 250MB - ESub.mkv"
        ),
        caption_prefix="Torrent Name",
        job_id=42,
        upload_settings=_upload_settings(),
        user_settings=None,
    )

    assert parse_mode is None
    assert (
        caption
        == "Torrent Name\nRepu Udayam 10 Gantalaku (2026) Tamil HQ HDRip - x264 - AAC - 250MB - ESub.mkv"
    )
