"""
CropRecordRepository — DB access for the crop_records table.

Five rows per detection (face + 4 regions). Batch insert is the only
sane write path.
"""
from __future__ import annotations

import uuid
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.Entities.CropRecord import CropRecord


class CropRecordRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def list_for_detection(
        self, detection_id: str | uuid.UUID
    ) -> list[CropRecord]:
        uid = _as_uuid(detection_id)
        result = await self.db.execute(
            select(CropRecord).where(CropRecord.detection_id == uid)
        )
        return list(result.scalars().all())

    async def create_many(self, records: Iterable[CropRecord]) -> None:
        self.db.add_all(list(records))
        await self.db.commit()


def _as_uuid(value: str | uuid.UUID) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))