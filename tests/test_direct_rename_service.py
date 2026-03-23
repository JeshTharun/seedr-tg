from __future__ import annotations

from seedr_tg.direct.renamer import FilenameRenamer, RegexSubstitutionRule, RenameRequest


def test_extension_preserved_when_renaming(tmp_path):
    renamer = FilenameRenamer(max_filename_bytes=255)

    result = renamer.build_name(
        original_name="movie.mkv",
        request=RenameRequest(explicit_name="my custom upload"),
        target_directory=tmp_path,
    )

    assert result == "my custom upload.mkv"


def test_regex_substitution_case_insensitive_by_default(tmp_path):
    renamer = FilenameRenamer(max_filename_bytes=255)

    result = renamer.build_name(
        original_name="My.Show.S01E01.mkv",
        request=RenameRequest(
            substitutions=[
                RegexSubstitutionRule(pattern="show", replacement="series"),
                RegexSubstitutionRule(pattern="\\.", replacement=" "),
            ]
        ),
        target_directory=tmp_path,
    )

    assert result == "My series S01E01.mkv"


def test_long_name_truncated_to_byte_limit(tmp_path):
    renamer = FilenameRenamer(max_filename_bytes=40)

    result = renamer.build_name(
        original_name="a" * 80 + ".mp4",
        request=RenameRequest(prefix="[Tag] "),
        target_directory=tmp_path,
    )

    assert len(result.encode("utf-8")) <= 40
    assert result.endswith(".mp4")


def test_duplicate_filename_gets_numeric_suffix(tmp_path):
    renamer = FilenameRenamer(max_filename_bytes=255)
    existing = tmp_path / "sample.mp4"
    existing.write_bytes(b"x")

    result = renamer.build_name(
        original_name="sample.mp4",
        request=RenameRequest(),
        target_directory=tmp_path,
    )

    assert result == "sample (1).mp4"
