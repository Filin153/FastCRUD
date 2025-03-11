from datetime import datetime

from sqlalchemy import create_engine, DateTime, func
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, declared_attr, mapped_column, Mapped

from config import settings

engine_async = create_async_engine(settings.PG_ASYNC_URL)
engine_sync = create_engine(settings.PG_SYNC_URL)


class Base(DeclarativeBase):
    __abstract__ = True

    @declared_attr
    def create_at(cls) -> Mapped[datetime]:
        return mapped_column(DateTime(timezone=True), server_default=func.now())

    @declared_attr
    def update_at(cls) -> Mapped[datetime]:
        return mapped_column(DateTime(timezone=True), onupdate=func.now(), nullable=True)

    @declared_attr
    def delete_at(cls) -> Mapped[datetime]:
        return mapped_column(DateTime(timezone=True), nullable=True)
