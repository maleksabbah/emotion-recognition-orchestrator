"""
Session manager — PostgreSQL CRUD for sessions.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.ORM_Models import Session
logger = logging.getLogger(__name__)


class SessionManager:

    async def create_session(
        self, db: AsyncSession, mode: str, source_s3_key: Optional[str] = None, metadata: Optional[dict] = None,
    ) -> Session:
        session = Session(
            id=uuid.uuid4(),
            mode=mode,
            status="active" if mode == "live" else "processing",
            source_s3_key=source_s3_key,
            metadata_=metadata or {},
        )
        db.add(session)
        await db.commit()
        await db.refresh(session)
        logger.info(f"Created session {session.id} mode={mode}")
        return session

    async def get_session(self, db: AsyncSession, session_id: str) -> Optional[Session]:
        result = await db.execute(select(Session).where(Session.id == uuid.UUID(session_id)))
        return result.scalar_one_or_none()

    async def update_status(self, db: AsyncSession, session_id: str, status: str, **kwargs) -> None:
        values = {"status": status}
        if status in ("complete", "failed"):
            values["completed_at"] = datetime.now(timezone.utc)
        values.update(kwargs)
        await db.execute(update(Session).where(Session.id == uuid.UUID(session_id)).values(**values))
        await db.commit()
        logger.info(f"Session {session_id} → {status}")

    async def increment_counters(self, db: AsyncSession, session_id: str, frames: int = 0, faces: int = 0) -> None:
        session = await self.get_session(db, session_id)
        if session:
            session.total_frames += frames
            session.total_faces += faces
            await db.commit()

    async def set_burned_key(self, db: AsyncSession, session_id: str, burned_s3_key: str) -> None:
        await db.execute(
            update(Session).where(Session.id == uuid.UUID(session_id))
            .values(burned_s3_key=burned_s3_key, status="complete", completed_at=datetime.now(timezone.utc))
        )
        await db.commit()
        logger.info(f"Session {session_id} burn complete → {burned_s3_key}")

    async def list_sessions(
        self, db: AsyncSession, limit: int = 50, offset: int = 0,
        mode: Optional[str] = None, status: Optional[str] = None,
    ) -> list[Session]:
        query = select(Session).order_by(Session.created_at.desc()).limit(limit).offset(offset)
        if mode:
            query = query.where(Session.mode == mode)
        if status:
            query = query.where(Session.status == status)
        result = await db.execute(query)
        return list(result.scalars().all())