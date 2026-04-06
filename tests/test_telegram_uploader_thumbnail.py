from __future__ import annotations

import base64

from seedr_tg.db.models import CaptionParseMode, UploadMediaType, UploadSettings, UserSettings
from seedr_tg.telegram.uploader import TelegramUploader


def _upload_settings(*, thumbnail_base64: str | None) -> UploadSettings:
    return UploadSettings(
        media_type=UploadMediaType.MEDIA,
        caption_template=None,
        caption_parse_mode=CaptionParseMode.HTML,
        thumbnail_file_id=None,
        thumbnail_base64=thumbnail_base64,
        created_at="2026-04-06T00:00:00+00:00",
        updated_at="2026-04-06T00:00:00+00:00",
    )


def _user_settings(*, user_id: int, thumbnail_base64: str | None) -> UserSettings:
    return UserSettings(
        user_id=user_id,
        caption_template=None,
        thumbnail_file_id=None,
        thumbnail_base64=thumbnail_base64,
        created_at="2026-04-06T00:00:00+00:00",
        updated_at="2026-04-06T00:00:00+00:00",
    )


def test_resolve_thumbnail_path_refreshes_global_cache_when_base64_changes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    first = base64.b64encode(b"first-thumb").decode("ascii")
    second = base64.b64encode(b"second-thumb").decode("ascii")

    first_path = TelegramUploader._resolve_thumbnail_path(
        _upload_settings(thumbnail_base64=first),
        None,
    )
    assert first_path is not None
    assert first_path.read_bytes() == b"first-thumb"

    second_path = TelegramUploader._resolve_thumbnail_path(
        _upload_settings(thumbnail_base64=second),
        None,
    )
    assert second_path == first_path
    assert second_path.read_bytes() == b"second-thumb"


def test_resolve_thumbnail_path_prefers_user_thumbnail_over_global(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    global_thumb = base64.b64encode(b"global-thumb").decode("ascii")
    user_thumb = base64.b64encode(b"user-thumb").decode("ascii")

    selected_path = TelegramUploader._resolve_thumbnail_path(
        _upload_settings(thumbnail_base64=global_thumb),
        _user_settings(user_id=42, thumbnail_base64=user_thumb),
    )

    assert selected_path is not None
    assert selected_path.name == "user_42.jpg"
    assert selected_path.read_bytes() == b"user-thumb"