"""
JobRepository — DB access for the sessions table.

Services call these methods; they never touch SQLAlchemy directly.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.Entities.Job import Job


class JobRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    # ── Reads ────────────────────────────────────────

    async def find_by_id(self, job_id: str | uuid.UUID) -> Optional[Job]:
        uid = _as_uuid(job_id)
        result = await self.db.execute(select(Job).where(Job.id == uid))
        return result.scalar_one_or_none()

    async def list_all(self, limit: int = 100) -> list[Job]:
        result = await self.db.execute(
            select(Job).order_by(Job.created_at.desc()).limit(limit)
        )
        return list(result.scalars().all())

    # ── Writes ───────────────────────────────────────

    async def create(
        self,
        mode: str,
        metadata: Optional[dict] = None,
        source_s3_key: Optional[str] = None,
    ) -> Job:
        job = Job(
            mode=mode,
            metadata_=metadata or {},
            source_s3_key=source_s3_key,
        )
        self.db.add(job)
        await self.db.commit()
        await self.db.refresh(job)
        return job

    async def update_status(self, job_id: str | uuid.UUID, status: str) -> None:
        uid = _as_uuid(job_id)
        await self.db.execute(
            update(Job).where(Job.id == uid).values(status=status)
        )
        await self.db.commit()

    async def set_source_s3_key(
        self, job_id: str | uuid.UUID, source_s3_key: str
    ) -> None:
        """
        Persist the real S3 key once the browser finishes uploading.
        Required so the burner can later fetch the source file by key.
        """
        uid = _as_uuid(job_id)
        await self.db.execute(
            update(Job).where(Job.id == uid).values(source_s3_key=source_s3_key)
        )
        await self.db.commit()

    async def mark_complete(
        self,
        job_id: str | uuid.UUID,
        burned_s3_key: Optional[str] = None,
    ) -> None:
        """
        Mark the job complete and (optionally) record where the burned
        output landed in S3 so the frontend can download it.

        Previously the condition was `if burned_s3_key is None` — inverted,
        which dropped real keys and only stored None. Fixed below.
        """
        uid = _as_uuid(job_id)
        values: dict = {
            "status": "complete",
            "completed_at": datetime.now(timezone.utc),
        }
        if burned_s3_key is not None:
            values["burned_s3_key"] = burned_s3_key
        await self.db.execute(update(Job).where(Job.id == uid).values(**values))
        await self.db.commit()

    async def increment_counters(
        self,
        job_id: str | uuid.UUID,
        frames: int = 0,
        faces: int = 0,
    ) -> None:
        uid = _as_uuid(job_id)
        await self.db.execute(
            update(Job)
            .where(Job.id == uid)
            .values(
                total_frames=Job.total_frames + frames,
                total_faces=Job.total_faces + faces,
            )
        )
        await self.db.commit()


def _as_uuid(value: str | uuid.UUID) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))



