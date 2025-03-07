from datetime import datetime
from typing import Union

from sqlalchemy import DateTime, func, BIGINT
from sqlalchemy.orm import Mapped, mapped_column

from database.database import Base


class UserModel(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    tg_id: Mapped[int] = mapped_column(BIGINT, nullable=False)
    fio: Mapped[str] = mapped_column()
    group: Mapped[str] = mapped_column()

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    update_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    delete_at: Mapped[Union[datetime, None]] = mapped_column(DateTime(timezone=True), nullable=True)
