from __future__ import annotations

import asyncio
from dataclasses import asdict, fields
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, ReturnDocument

from seedr_tg.db.models import (
    FINAL_PHASES,
    JobPhase,
    JobRecord,
    SeedrDeviceCodeRecord,
    TelegramLoginState,
    TelegramUserSession,
    utc_now,
)

FINAL_PHASE_VALUES = [phase.value for phase in FINAL_PHASES]


def _serialize_job_updates(updates: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in updates.items():
        if key == "phase" and isinstance(value, JobPhase):
            serialized[key] = value.value
        elif key == "cancel_requested":
            serialized[key] = bool(value)
        else:
            serialized[key] = value
    return serialized


class JobRepository:
    def __init__(
        self,
        mongodb_uri: str,
        database_name: str,
        *,
        client: AsyncIOMotorClient | None = None,
    ) -> None:
        self._write_lock = asyncio.Lock()
        self._owns_client = client is None
        self._client = client or AsyncIOMotorClient(mongodb_uri)
        self._database: AsyncIOMotorDatabase = self._client[database_name]
        self._jobs = self._database.jobs
        self._state = self._database.app_state
        self._counters = self._database.counters

    async def initialize(self) -> None:
        await self._jobs.create_index([("phase", ASCENDING), ("queue_position", ASCENDING)])
        await self._jobs.create_index("magnet_link")

    async def close(self) -> None:
        if self._owns_client:
            self._client.close()

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
            job_id = await self._next_job_id()
            now = utc_now()
            await self._jobs.insert_one(
                {
                    "_id": job_id,
                    "magnet_link": magnet_link,
                    "source_chat_id": source_chat_id,
                    "source_message_id": source_message_id,
                    "admin_message_id": None,
                    "target_chat_id": target_chat_id,
                    "phase": JobPhase.QUEUED.value,
                    "queue_position": queue_position,
                    "torrent_name": None,
                    "total_size_bytes": None,
                    "seedr_torrent_id": None,
                    "seedr_folder_id": None,
                    "seedr_folder_name": None,
                    "progress_percent": 0.0,
                    "current_step": None,
                    "local_path": None,
                    "upload_file_count": 0,
                    "uploaded_file_count": 0,
                    "failure_reason": None,
                    "last_error": None,
                    "cancel_requested": False,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            return await self.get_job(job_id)

    async def has_active_magnet(self, magnet_link: str) -> bool:
        count = await self._jobs.count_documents(
            {"magnet_link": magnet_link, "phase": {"$nin": FINAL_PHASE_VALUES}}
        )
        return bool(count)

    async def get_job(self, job_id: int) -> JobRecord:
        row = await self._jobs.find_one({"_id": job_id})
        if row is None:
            raise LookupError(f"Job {job_id} not found")
        return self._to_record(row)

    async def list_jobs(self, include_final: bool = False) -> list[JobRecord]:
        query = {} if include_final else {"phase": {"$nin": FINAL_PHASE_VALUES}}
        cursor = self._jobs.find(query).sort(
            [
                ("queue_position", ASCENDING),
                ("_id", ASCENDING),
            ]
        )
        rows = await cursor.to_list(length=None)
        return [self._to_record(row) for row in rows]

    async def get_next_job(self) -> JobRecord | None:
        row = await self._jobs.find_one(
            {"phase": {"$nin": FINAL_PHASE_VALUES}},
            sort=[("queue_position", ASCENDING), ("_id", ASCENDING)],
        )
        return self._to_record(row) if row else None

    async def update_job(self, job_id: int, **updates: Any) -> JobRecord:
        if not updates:
            return await self.get_job(job_id)
        async with self._write_lock:
            updates["updated_at"] = utc_now()
            await self._jobs.update_one({"_id": job_id}, {"$set": _serialize_job_updates(updates)})
            return await self.get_job(job_id)

    async def request_cancel(self, job_id: int) -> JobRecord:
        return await self.update_job(job_id, cancel_requested=True)

    async def renumber_queue(self) -> None:
        async with self._write_lock:
            active_jobs = await self.list_jobs(include_final=False)
            for index, job in enumerate(active_jobs, start=1):
                await self._jobs.update_one(
                    {"_id": job.id},
                    {"$set": {"queue_position": index, "updated_at": utc_now()}},
                )

    async def _next_queue_position(self) -> int:
        row = await self._jobs.find_one(
            {"phase": {"$nin": FINAL_PHASE_VALUES}},
            sort=[("queue_position", -1)],
            projection={"queue_position": True},
        )
        return int(row["queue_position"] + 1) if row else 1

    async def _next_job_id(self) -> int:
        row = await self._counters.find_one_and_update(
            {"_id": "job_id"},
            {"$inc": {"value": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int(row["value"])

    async def save_seedr_device_code(
        self,
        *,
        device_code: str,
        user_code: str,
        verification_url: str,
        expires_in: int | None,
    ) -> SeedrDeviceCodeRecord:
        record = SeedrDeviceCodeRecord(
            device_code=device_code,
            user_code=user_code,
            verification_url=verification_url,
            expires_in=expires_in,
            created_at=utc_now(),
        )
        await self._state.replace_one(
            {"_id": "seedr_device_code"},
            {"_id": "seedr_device_code", **asdict(record)},
            upsert=True,
        )
        return record

    async def get_seedr_device_code(self) -> SeedrDeviceCodeRecord | None:
        row = await self._state.find_one({"_id": "seedr_device_code"})
        if row is None:
            return None
        row.pop("_id", None)
        return SeedrDeviceCodeRecord(**row)

    async def clear_seedr_device_code(self) -> None:
        await self._state.delete_one({"_id": "seedr_device_code"})

    async def set_seedr_token_json(self, token_json: str) -> None:
        await self._state.update_one(
            {"_id": "seedr_token"},
            {"$set": {"token_json": token_json, "updated_at": utc_now()}},
            upsert=True,
        )

    async def get_seedr_token_json(self) -> str | None:
        row = await self._state.find_one({"_id": "seedr_token"}, projection={"token_json": True})
        return None if row is None else row.get("token_json")

    async def save_telegram_login_state(
        self,
        *,
        phone_number: str,
        phone_code_hash: str,
        session_string: str,
        password_required: bool,
    ) -> TelegramLoginState:
        existing = await self.get_telegram_login_state()
        created_at = existing.created_at if existing else utc_now()
        record = TelegramLoginState(
            phone_number=phone_number,
            phone_code_hash=phone_code_hash,
            session_string=session_string,
            password_required=password_required,
            created_at=created_at,
            updated_at=utc_now(),
        )
        await self._state.replace_one(
            {"_id": "telegram_login_state"},
            {"_id": "telegram_login_state", **asdict(record)},
            upsert=True,
        )
        return record

    async def get_telegram_login_state(self) -> TelegramLoginState | None:
        row = await self._state.find_one({"_id": "telegram_login_state"})
        if row is None:
            return None
        row.pop("_id", None)
        return TelegramLoginState(**row)

    async def clear_telegram_login_state(self) -> None:
        await self._state.delete_one({"_id": "telegram_login_state"})

    async def save_telegram_user_session(
        self,
        *,
        session_string: str,
        phone_number: str | None,
        user_id: int | None,
        username: str | None,
        display_name: str | None,
    ) -> TelegramUserSession:
        existing = await self.get_telegram_user_session()
        created_at = existing.created_at if existing else utc_now()
        record = TelegramUserSession(
            session_string=session_string,
            phone_number=phone_number,
            user_id=user_id,
            username=username,
            display_name=display_name,
            created_at=created_at,
            updated_at=utc_now(),
        )
        await self._state.replace_one(
            {"_id": "telegram_user_session"},
            {"_id": "telegram_user_session", **asdict(record)},
            upsert=True,
        )
        return record

    async def get_telegram_user_session(self) -> TelegramUserSession | None:
        row = await self._state.find_one({"_id": "telegram_user_session"})
        if row is None:
            return None
        row.pop("_id", None)
        return TelegramUserSession(**row)

    @staticmethod
    def _to_record(row: dict[str, Any]) -> JobRecord:
        values = {field.name: row.get(field.name) for field in fields(JobRecord)}
        values["id"] = row["_id"]
        values["phase"] = JobPhase(values["phase"])
        values["cancel_requested"] = bool(values["cancel_requested"])
        return JobRecord(**values)
