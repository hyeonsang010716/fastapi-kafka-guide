"""
SQLAlchemy 2.0 비동기 엔진/세션.

엔진과 세션 팩토리를 모듈 레벨에 두고, 요청 단위로 AsyncSession 컨텍스트
매니저를 받아 쓰는 구조를 사용한다.
"""

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import DATABASE_URL

engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=10,
    pool_pre_ping=True,
    echo=False,
)

SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autoflush=False,
)
