from typing import Union

from aredis_om import Field
from pydantic import BaseModel

from database.interfaces.redis_json import BaseRedisModel


class UserCreate(BaseModel):
    tg_id: int = Field(index=True)
    fio: str = Field(index=True, full_text_search=True)
    group: str = Field(index=True, full_text_search=True)
    allow: Union[bool, int] = Field(index=True)


class UserUpdate(BaseModel):
    tg_id: int | None = None
    fio: str | None = None
    group: str | None = None
    allow: bool | None = None


class UserSchemas(UserCreate, BaseRedisModel):
    id: int = Field(index=True)


class UserFilters(UserUpdate):
    id: int | None = None
