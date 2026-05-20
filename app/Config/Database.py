"""
Async SQLAlchemy engine and session factory for orchestrator_db.
"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.Config.Config import ORCHESTRATOR_DB_URL


engine = create_async_engine(
    ORCHESTRATOR_DB_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

SessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)