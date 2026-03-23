from __future__ import annotations

from seedr_tg.telegram.media_rename import TelegramMediaRenameHandler


def test_rename_parser_accepts_unquoted_multiword_explicit_name():
    options = TelegramMediaRenameHandler._parse_options(
        "/rename Small Soldiers 1998 REMASTERED 1080p BluRay.mkv"
    )

    assert options.explicit_name == "Small Soldiers 1998 REMASTERED 1080p BluRay.mkv"
    assert options.prefix is None
    assert options.substitutions == []


def test_rename_parser_accepts_multiword_rename_value_before_flags():
    options = TelegramMediaRenameHandler._parse_options(
        "/rename --rename Small Soldiers 1998.mkv --prefix [TAG]"
    )

    assert options.explicit_name == "Small Soldiers 1998.mkv"
    assert options.prefix == "[TAG]"
