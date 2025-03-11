from datetime import datetime

from sqlalchemy import create_engine, DateTime, func
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, declared_attr, mapped_column, Mapped

from config import settings


def get_db_url_async() -> str:
    return f"postgresql+asyncpg://{settings.pg_user}:{settings.pg_pass}@{settings.pg_host}:{settings.pg_port}/{settings.pg_db_name}"


def get_db_url_sync() -> str:
    return f"postgresql://{settings.pg_user}:{settings.pg_pass}@{settings.pg_host}:{settings.pg_port}/{settings.pg_db_name}"


engine_async = create_async_engine(get_db_url_async())
engine_sync = create_engine(get_db_url_sync())


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
