from contextlib import asynccontextmanager, contextmanager
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from .database import engine_sync, engine_async


@asynccontextmanager
async def get_async_session() -> AsyncSession:
    async with AsyncSession(engine_async) as conn:
        yield conn


@contextmanager
def get_sync_session() -> Session:
    with Session(engine_sync) as conn:
        yield conn


async def get_async_session_fastapi() -> AsyncSession:
    async with AsyncSession(engine_async) as conn:
        yield conn


SessionFastAPIDep = Annotated[AsyncSession, Depends(get_async_session_fastapi)]
