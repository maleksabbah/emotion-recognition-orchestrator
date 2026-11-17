"""
PredictionRepository — DB access for the predictions table.

One row per detection. Batch insert matches DetectionRepository for the
same reason — the pipeline flushes predictions in groups.
"""
from __future__ import annotations

import uuid
from typing import Iterable, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.Entities.Prediction import Prediction


class PredictionRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def find_for_detection(
        self, detection_id: str | uuid.UUID
    ) -> Optional[Prediction]:
        uid = _as_uuid(detection_id)
        result = await self.db.execute(
            select(Prediction).where(Prediction.detection_id == uid)
        )
        return result.scalar_one_or_none()

    async def find_for_detections(
        self, detection_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, Prediction]:
        """Batch-fetch predictions for multiple detection IDs in one query."""
        if not detection_ids:
            return {}
        result = await self.db.execute(
            select(Prediction).where(Prediction.detection_id.in_(detection_ids))
        )
        return {p.detection_id: p for p in result.scalars().all()}

    async def create(self, prediction: Prediction) -> Prediction:
        self.db.add(prediction)
        await self.db.flush()
        await self.db.refresh(prediction)
        return prediction

    async def create_many(self, predictions: Iterable[Prediction]) -> None:
        self.db.add_all(list(predictions))
        await self.db.flush()


def _as_uuid(value: str | uuid.UUID) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))