from __future__ import annotations

import asyncio
from dataclasses import fields
from pathlib import Path
from typing import Any

import aiosqlite

from seedr_tg.db.models import FINAL_PHASES, JobPhase, JobRecord, utc_now


class JobRepository:
    def __init__(self, database_path: Path) -> None:
        self._database_path = database_path
        self._write_lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with aiosqlite.connect(self._database_path) as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    magnet_link TEXT NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    admin_message_id INTEGER,
                    target_chat_id INTEGER NOT NULL,
                    phase TEXT NOT NULL,
                    queue_position INTEGER NOT NULL,
                    torrent_name TEXT,
                    total_size_bytes INTEGER,
                    seedr_torrent_id INTEGER,
                    seedr_folder_id INTEGER,
                    seedr_folder_name TEXT,
                    progress_percent REAL NOT NULL DEFAULT 0,
                    current_step TEXT,
                    local_path TEXT,
                    upload_file_count INTEGER NOT NULL DEFAULT 0,
                    uploaded_file_count INTEGER NOT NULL DEFAULT 0,
                    failure_reason TEXT,
                    last_error TEXT,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_phase_queue ON jobs(phase, queue_position)"
            )
            await conn.commit()

    async def enqueue_job(
        self,
        *,
        magnet_link: str,
        source_chat_id: int,
        source_message_id: int,
        target_chat_id: int,
    ) -> JobRecord:
        async with self._write_lock:
            queue_position = await self._next_queue_position()
            now = utc_now()
            async with aiosqlite.connect(self._database_path) as conn:
                cursor = await conn.execute(
                    """
                    INSERT INTO jobs (
                        magnet_link, source_chat_id, source_message_id, admin_message_id,
                        target_chat_id, phase, queue_position, progress_percent,
                        upload_file_count, uploaded_file_count, cancel_requested,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, NULL, ?, ?, ?, 0, 0, 0, 0, ?, ?)
                    """,
                    (
                        magnet_link,
                        source_chat_id,
                        source_message_id,
                        target_chat_id,
                        JobPhase.QUEUED,
                        queue_position,
                        now,
                        now,
                    ),
                )
                await conn.commit()
                return await self.get_job(cursor.lastrowid)

    async def has_active_magnet(self, magnet_link: str) -> bool:
        placeholders = ", ".join("?" for _ in FINAL_PHASES)
        query = f"SELECT COUNT(*) FROM jobs WHERE magnet_link = ? AND phase NOT IN ({placeholders})"
        async with aiosqlite.connect(self._database_path) as conn:
            cursor = await conn.execute(query, (magnet_link, *FINAL_PHASES))
            row = await cursor.fetchone()
            return bool(row[0])

    async def get_job(self, job_id: int) -> JobRecord:
        async with aiosqlite.connect(self._database_path) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = await cursor.fetchone()
            if row is None:
                raise LookupError(f"Job {job_id} not found")
            return self._to_record(row)

    async def list_jobs(self, include_final: bool = False) -> list[JobRecord]:
        async with aiosqlite.connect(self._database_path) as conn:
            conn.row_factory = aiosqlite.Row
            if include_final:
                cursor = await conn.execute("SELECT * FROM jobs ORDER BY id ASC")
            else:
                placeholders = ", ".join("?" for _ in FINAL_PHASES)
                query = (
                    f"SELECT * FROM jobs WHERE phase NOT IN ({placeholders}) "
                    "ORDER BY queue_position ASC"
                )
                cursor = await conn.execute(query, tuple(FINAL_PHASES))
            rows = await cursor.fetchall()
            return [self._to_record(row) for row in rows]

    async def get_next_job(self) -> JobRecord | None:
        placeholders = ", ".join("?" for _ in FINAL_PHASES)
        query = (
            f"SELECT * FROM jobs WHERE phase NOT IN ({placeholders}) "
            "ORDER BY queue_position ASC, id ASC LIMIT 1"
        )
        async with aiosqlite.connect(self._database_path) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(query, tuple(FINAL_PHASES))
            row = await cursor.fetchone()
            return self._to_record(row) if row else None

    async def update_job(self, job_id: int, **updates: Any) -> JobRecord:
        if not updates:
            return await self.get_job(job_id)
        async with self._write_lock:
            updates["updated_at"] = utc_now()
            assignments = ", ".join(f"{key} = ?" for key in updates)
            values = [self._serialize_update(key, value) for key, value in updates.items()]
            values.append(job_id)
            async with aiosqlite.connect(self._database_path) as conn:
                await conn.execute(f"UPDATE jobs SET {assignments} WHERE id = ?", tuple(values))
                await conn.commit()
            return await self.get_job(job_id)

    async def request_cancel(self, job_id: int) -> JobRecord:
        return await self.update_job(job_id, cancel_requested=True)

    async def renumber_queue(self) -> None:
        async with self._write_lock:
            active_jobs = await self.list_jobs(include_final=False)
            async with aiosqlite.connect(self._database_path) as conn:
                for index, job in enumerate(active_jobs, start=1):
                    await conn.execute(
                        "UPDATE jobs SET queue_position = ?, updated_at = ? WHERE id = ?",
                        (index, utc_now(), job.id),
                    )
                await conn.commit()

    async def _next_queue_position(self) -> int:
        placeholders = ", ".join("?" for _ in FINAL_PHASES)
        query = (
            "SELECT COALESCE(MAX(queue_position), 0) + 1 FROM jobs "
            f"WHERE phase NOT IN ({placeholders})"
        )
        async with aiosqlite.connect(self._database_path) as conn:
            cursor = await conn.execute(query, tuple(FINAL_PHASES))
            row = await cursor.fetchone()
            return int(row[0])

    @staticmethod
    def _serialize_update(key: str, value: Any) -> Any:
        if key == "phase" and isinstance(value, JobPhase):
            return value.value
        if key == "cancel_requested":
            return int(bool(value))
        return value

    @staticmethod
    def _to_record(row: aiosqlite.Row) -> JobRecord:
        values = {field.name: row[field.name] for field in fields(JobRecord)}
        values["phase"] = JobPhase(values["phase"])
        values["cancel_requested"] = bool(values["cancel_requested"])
        return JobRecord(**values)
