"""
Async database engine and session factory for orchestrator_db.
"""
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.Config import ORCHESTRATOR_DB_URL
engine = create_async_engine(
    ORCHESTRATOR_DB_URL,
    echo=False,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,
)

async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncSession:
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()