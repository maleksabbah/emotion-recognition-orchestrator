"""
DetectionRepository — DB access for the detections table.

Includes a batch insert so the pipeline can flush N detections in one
round-trip rather than one INSERT per face.
"""
from __future__ import annotations

import uuid
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.Entities.Detection import Detection


class DetectionRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def find_by_frame_and_face(
        self,
        session_id: str | uuid.UUID,
        frame_number: int,
        face_index: int,
    ) -> Detection | None:
        uid = _as_uuid(session_id)
        result = await self.db.execute(
            select(Detection).where(
                Detection.session_id == uid,
                Detection.frame_number == frame_number,
                Detection.face_index == face_index,
            )
        )
        return result.scalar_one_or_none()

    async def list_for_session(self, session_id: str | uuid.UUID) -> list[Detection]:
        uid = _as_uuid(session_id)
        result = await self.db.execute(
            select(Detection)
            .where(Detection.session_id == uid)
            .order_by(Detection.frame_number, Detection.face_index)
        )
        return list(result.scalars().all())

    async def create(self, detection: Detection) -> Detection:
        self.db.add(detection)
        await self.db.flush()
        await self.db.refresh(detection)
        return detection

    async def create_many(self, detections: Iterable[Detection]) -> None:
        self.db.add_all(list(detections))
        await self.db.flush()


def _as_uuid(value: str | uuid.UUID) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))