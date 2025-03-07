from pydantic import BaseModel


class UserCreate(BaseModel):
    tg_id: int
    fio: str
    group: str


class UserUpdate(BaseModel):
    tg_id: int | None = None
    fio: str | None = None
    group: str | None = None


class UserSchemas(UserCreate):
    id: int


class UserFilters(UserUpdate):
    pass
