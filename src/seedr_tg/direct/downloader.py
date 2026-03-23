from __future__ import annotations

import asyncio
import logging
import random
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final
from urllib.parse import unquote, urlparse

import aiofiles
import httpx

LOGGER = logging.getLogger(__name__)
_CONTENT_DISPOSITION_FILENAME = re.compile(r'filename\*=UTF-8\'\'([^;]+)|filename="?([^";]+)"?')
_DEFAULT_DOWNLOAD_NAME: Final[str] = "downloaded_file"


class DirectDownloadError(RuntimeError):
    """Base error for direct URL downloads."""


class InvalidDirectUrlError(DirectDownloadError):
    """Raised when a URL is malformed or unsupported."""


class DirectDownloadIntegrityError(DirectDownloadError):
    """Raised when downloaded bytes do not match expected content length."""


@dataclass(slots=True)
class DownloadedFile:
    """Metadata for a successfully downloaded file."""

    original_name: str
    file_path: str
    size_bytes: int
    content_length: int | None


class DirectDownloader:
    """Streams a direct URL into local storage with retries and timeout control."""

    def __init__(
        self,
        *,
        connect_timeout_seconds: float,
        read_timeout_seconds: float,
        write_timeout_seconds: float,
        pool_timeout_seconds: float,
        chunk_size_bytes: int,
        max_retries: int,
        retry_base_delay_seconds: float,
        retry_max_delay_seconds: float,
    ) -> None:
        self._timeout = httpx.Timeout(
            connect=float(connect_timeout_seconds),
            read=float(read_timeout_seconds),
            write=float(write_timeout_seconds),
            pool=float(pool_timeout_seconds),
        )
        self._chunk_size_bytes = max(64 * 1024, int(chunk_size_bytes))
        self._max_retries = max(1, int(max_retries))
        self._retry_base_delay_seconds = max(0.1, float(retry_base_delay_seconds))
        self._retry_max_delay_seconds = max(
            self._retry_base_delay_seconds,
            float(retry_max_delay_seconds),
        )

    async def download_to_path(self, url: str, destination_path: str) -> DownloadedFile:
        """Download a URL to destination_path using streaming writes and retries."""
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise InvalidDirectUrlError("Only absolute http(s) URLs are supported.")

        LOGGER.info("Starting direct download url=%s destination=%s", url, destination_path)
        expected_from_head = await self._probe_content_length(url)

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=self._timeout,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        ) as client:
            for attempt in range(1, self._max_retries + 1):
                downloaded = 0
                try:
                    async with client.stream("GET", url) as response:
                        response.raise_for_status()
                        name_from_response = self._name_from_headers_or_url(url, response)
                        content_length = self._parse_content_length(
                            response.headers.get("Content-Length")
                        )
                        expected_length = content_length or expected_from_head

                        async with aiofiles.open(destination_path, "wb") as handle:
                            async for chunk in response.aiter_bytes(self._chunk_size_bytes):
                                if not chunk:
                                    continue
                                await handle.write(chunk)
                                downloaded += len(chunk)

                        if expected_length is not None and downloaded != expected_length:
                            raise DirectDownloadIntegrityError(
                                "Downloaded content size mismatch "
                                f"(got {downloaded}, expected {expected_length})."
                            )

                        LOGGER.info(
                            "Direct download completed url=%s bytes=%s name=%s",
                            url,
                            downloaded,
                            name_from_response,
                        )
                        return DownloadedFile(
                            original_name=name_from_response,
                            file_path=destination_path,
                            size_bytes=downloaded,
                            content_length=expected_length,
                        )
                except (
                    httpx.TimeoutException,
                    httpx.ConnectError,
                    httpx.ReadError,
                    httpx.HTTPStatusError,
                    DirectDownloadIntegrityError,
                    OSError,
                ) as exc:
                    LOGGER.warning(
                        "Direct download attempt failed attempt=%s/%s url=%s error=%s",
                        attempt,
                        self._max_retries,
                        url,
                        exc,
                    )
                    await self._remove_partial_file(destination_path)
                    if attempt >= self._max_retries or not self._is_retryable(exc):
                        raise DirectDownloadError(f"Failed to download URL: {exc}") from exc
                    backoff = min(
                        self._retry_base_delay_seconds * (2 ** (attempt - 1)),
                        self._retry_max_delay_seconds,
                    )
                    jitter = random.uniform(0.0, self._retry_base_delay_seconds)
                    await asyncio.sleep(backoff + jitter)

        raise DirectDownloadError("Failed to download URL due to unknown error.")

    async def _probe_content_length(self, url: str) -> int | None:
        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=self._timeout,
                limits=httpx.Limits(max_connections=5, max_keepalive_connections=2),
            ) as client:
                response = await client.head(url)
                if response.status_code >= 400:
                    return None
                size = self._parse_content_length(response.headers.get("Content-Length"))
                if size is not None:
                    LOGGER.info("HEAD probe content-length=%s for url=%s", size, url)
                return size
        except (httpx.HTTPError, OSError):
            LOGGER.debug("Skipping HEAD probe for url=%s", url)
            return None

    @staticmethod
    def _parse_content_length(value: str | None) -> int | None:
        if value is None:
            return None
        if not value.isdigit():
            return None
        size = int(value)
        return size if size >= 0 else None

    @staticmethod
    async def _remove_partial_file(path: str) -> None:
        try:
            await asyncio.to_thread(Path(path).unlink, True)
        except Exception:
            LOGGER.debug(
                "Could not remove partial direct download file path=%s",
                path,
                exc_info=True,
            )

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        if isinstance(exc, DirectDownloadIntegrityError):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            code = exc.response.status_code
            return code in {408, 409, 425, 429, 500, 502, 503, 504}
        retryable_types = httpx.TimeoutException | httpx.ConnectError | httpx.ReadError | OSError
        return isinstance(exc, retryable_types)

    @staticmethod
    def _name_from_headers_or_url(url: str, response: httpx.Response) -> str:
        content_disposition = response.headers.get("Content-Disposition", "")
        match = _CONTENT_DISPOSITION_FILENAME.search(content_disposition)
        if match:
            encoded = match.group(1)
            plain = match.group(2)
            if encoded:
                decoded = unquote(encoded).strip()
                if decoded:
                    return decoded
            if plain:
                candidate = plain.strip()
                if candidate:
                    return candidate

        parsed = urlparse(url)
        candidate = unquote(PurePosixPath(parsed.path).name).strip()
        return candidate or _DEFAULT_DOWNLOAD_NAME
