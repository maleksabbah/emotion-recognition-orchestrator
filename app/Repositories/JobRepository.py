"""
JobRepository — DB access for the sessions table.

Services call these methods; they never touch SQLAlchemy directly.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import and_, func, select, update
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

    async def list_all(self, limit: int = 100, user_id: str | None = None) -> list[Job]:
        stmt = select(Job)
        if user_id:
            stmt = stmt.where(Job.metadata_["user_id"].astext == user_id)
        stmt = stmt.order_by(Job.created_at.desc()).limit(limit)
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    # ── Usage (plan limits) ──────────────────────────

    async def count_usage(
        self, user_id: str, month_start: datetime, active_since: datetime
    ) -> dict[str, int]:
        """
        One query, three counts for a user:
          photos / videos  created since month_start, source uploaded, not failed
          active_jobs      photo/video in processing|burning, created since active_since
        """
        owner = Job.metadata_["user_id"].astext == user_id
        billable = and_(
            Job.created_at >= month_start,
            Job.source_s3_key.isnot(None),
            Job.status != "failed",
        )
        active = and_(
            Job.mode.in_(("photo", "video")),
            Job.status.in_(("processing", "burning")),
            Job.created_at >= active_since,
        )
        stmt = select(
            func.count().filter(and_(Job.mode == "photo", billable)),
            func.count().filter(and_(Job.mode == "video", billable)),
            func.count().filter(active),
        ).where(owner, Job.created_at >= min(month_start, active_since))
        photos, videos, active_jobs = (await self.db.execute(stmt)).one()
        return {
            "photos": int(photos or 0),
            "videos": int(videos or 0),
            "active_jobs": int(active_jobs or 0),
        }

    async def sum_live_seconds(
        self, user_id: str, month_start: datetime, cap_seconds: int
    ) -> int:
        """
        Sum over this month's live rows of (completed_at or now) - created_at,
        each clamped to [0, cap_seconds] so a row left open can't eat the quota.
        """
        ended = func.coalesce(Job.completed_at, func.now())
        duration = func.extract("epoch", ended - Job.created_at)
        clamped = func.least(func.greatest(duration, 0), cap_seconds)
        stmt = select(func.coalesce(func.sum(clamped), 0)).where(
            Job.metadata_["user_id"].astext == user_id,
            Job.mode == "live",
            Job.created_at >= month_start,
        )
        total = (await self.db.execute(stmt)).scalar_one()
        return int(float(total or 0))

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
        await self.db.flush()
        await self.db.refresh(job)
        return job

    async def update_status(self, job_id: str | uuid.UUID, status: str) -> None:
        uid = _as_uuid(job_id)
        await self.db.execute(
            update(Job).where(Job.id == uid).values(status=status)
        )

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

    async def set_metadata(self, job_id: str | uuid.UUID, metadata: dict) -> None:
        """Replace the metadata JSONB. Callers merge first (read → merge → write)."""
        uid = _as_uuid(job_id)
        await self.db.execute(
            update(Job).where(Job.id == uid).values(metadata_=metadata)
        )

    async def mark_failed(
        self,
        job_id: str | uuid.UUID,
        metadata: Optional[dict] = None,
    ) -> None:
        """status='failed' + completed_at; optionally replace metadata (e.g. with an error)."""
        uid = _as_uuid(job_id)
        values: dict = {
            "status": "failed",
            "completed_at": datetime.now(timezone.utc),
        }
        if metadata is not None:
            values["metadata_"] = metadata
        await self.db.execute(update(Job).where(Job.id == uid).values(**values))

    async def try_set_burning(self, job_id: str | uuid.UUID) -> bool:
        """
        Atomically set status='burning' only if not already 'burning'.
        Returns True if the row was updated (we won the race), False otherwise.
        """
        uid = _as_uuid(job_id)
        result = await self.db.execute(
            update(Job)
            .where(Job.id == uid, Job.status != "burning")
            .values(status="burning")
        )
        return result.rowcount > 0

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


def _as_uuid(value: str | uuid.UUID) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))



