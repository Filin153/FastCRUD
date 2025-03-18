from sqlalchemy import BIGINT
from sqlalchemy.orm import Mapped, mapped_column

from database.database import Base


class UserModel(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    tg_id: Mapped[int] = mapped_column(BIGINT, nullable=False)
    fio: Mapped[str] = mapped_column()
    group: Mapped[str] = mapped_column()
    allow: Mapped[bool] = mapped_column()
