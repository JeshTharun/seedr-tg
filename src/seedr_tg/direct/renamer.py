from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

_INVALID_FILENAME_CHARS = re.compile(r"[\\/:*?\"<>|\x00-\x1f]")
_WHITESPACE = re.compile(r"\s+")


@dataclass(slots=True)
class RegexSubstitutionRule:
    """A regex substitution operation applied to a filename stem."""

    pattern: str
    replacement: str
    case_sensitive: bool = False


@dataclass(slots=True)
class RenameRequest:
    """Input options for deterministic filename renaming."""

    explicit_name: str | None = None
    prefix: str | None = None
    substitutions: list[RegexSubstitutionRule] = field(default_factory=list)


class FilenameRenamer:
    """Sanitizes and renames file names while preserving extensions and uniqueness."""

    def __init__(self, *, max_filename_bytes: int = 255) -> None:
        self._max_filename_bytes = max(32, int(max_filename_bytes))

    def build_name(
        self,
        *,
        original_name: str,
        request: RenameRequest,
        target_directory: Path,
    ) -> str:
        """Build a Telegram-safe filename and resolve duplicate names in target_directory."""
        safe_original = self.sanitize_filename(original_name)
        original_stem, extension = self._split_name(safe_original)

        stem = original_stem
        if request.explicit_name:
            explicit_safe = self.sanitize_filename(request.explicit_name, fallback="")
            explicit_stem, _ = self._split_name(explicit_safe)
            if explicit_stem:
                stem = explicit_stem

        if request.prefix:
            prefix = self._sanitize_fragment(request.prefix)
            if prefix:
                stem = f"{prefix}{stem}"

        for rule in request.substitutions:
            try:
                flags = 0 if rule.case_sensitive else re.IGNORECASE
                stem = re.sub(rule.pattern, rule.replacement, stem, flags=flags)
            except re.error:
                continue

        stem = self._sanitize_fragment(stem)
        if not stem:
            stem = original_stem

        normalized_name = self._limit_bytes(f"{stem}{extension}")
        normalized_stem, _ = self._split_name(normalized_name)
        if not normalized_stem:
            normalized_name = self._limit_bytes(safe_original)
            normalized_stem, _ = self._split_name(normalized_name)
            if not normalized_stem:
                normalized_name = self._limit_bytes(f"file{extension}")

        return self._ensure_unique_name(normalized_name, extension, target_directory)

    @staticmethod
    def sanitize_filename(value: str, fallback: str = "file") -> str:
        candidate = (value or "").strip()
        candidate = _INVALID_FILENAME_CHARS.sub("_", candidate)
        candidate = candidate.replace("\n", " ").replace("\r", " ")
        candidate = _WHITESPACE.sub(" ", candidate)
        candidate = candidate.strip(" .")
        return candidate or fallback

    @staticmethod
    def _split_name(filename: str) -> tuple[str, str]:
        path = Path(filename)
        extension = path.suffix
        if extension:
            stem = filename[: -len(extension)]
        else:
            stem = filename
        stem = stem.strip(" .")
        return stem, extension

    @staticmethod
    def _sanitize_fragment(value: str) -> str:
        cleaned = FilenameRenamer.sanitize_filename(value, fallback="")
        return cleaned.strip(" .")

    def _ensure_unique_name(
        self,
        candidate_name: str,
        extension: str,
        target_directory: Path,
    ) -> str:
        if not (target_directory / candidate_name).exists():
            return candidate_name

        stem, _ = self._split_name(candidate_name)
        counter = 1
        while True:
            suffix = f" ({counter})"
            suffix_budget = self._max_filename_bytes - len(extension.encode("utf-8"))
            base_budget = max(1, suffix_budget - len(suffix.encode("utf-8")))
            truncated_stem = self._truncate_utf8(stem, base_budget)
            deduped = f"{truncated_stem}{suffix}{extension}"
            if not (target_directory / deduped).exists():
                return deduped
            counter += 1

    def _limit_bytes(self, value: str) -> str:
        raw = value.encode("utf-8")
        if len(raw) <= self._max_filename_bytes:
            return value

        stem, extension = self._split_name(value)
        extension_bytes = len(extension.encode("utf-8"))
        stem_budget = max(1, self._max_filename_bytes - extension_bytes)
        truncated_stem = self._truncate_utf8(stem, stem_budget)
        limited = f"{truncated_stem}{extension}"
        if len(limited.encode("utf-8")) <= self._max_filename_bytes:
            return limited
        return self._truncate_utf8(value, self._max_filename_bytes)

    @staticmethod
    def _truncate_utf8(text: str, max_bytes: int) -> str:
        if max_bytes <= 0:
            return ""
        encoded = text.encode("utf-8")
        if len(encoded) <= max_bytes:
            return text
        truncated = encoded[:max_bytes]
        while truncated:
            try:
                return truncated.decode("utf-8")
            except UnicodeDecodeError:
                truncated = truncated[:-1]
        return ""
