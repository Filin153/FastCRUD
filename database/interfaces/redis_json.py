import warnings
from abc import ABC
from typing import Any, Union, Optional

from aredis_om import NotFoundError, Migrator, get_redis_connection, HashModel

from config import settings
from .base_interface import BaseDBInterface


class BaseRedisModel(HashModel, ABC):
    class Meta:
        global_key_prefix = "db_cache"
        database = get_redis_connection(url=settings.REDIS_URL)


class BaseRedisInterface(BaseDBInterface):
    _base_schemas = None

    def __init__(self,
                 base_schemas: Union[HashModel, Any]
                 ):
        warnings.filterwarnings(
            "ignore",
            category=UserWarning,
            message="Pydantic serializer warnings:.*"
        )
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
        )

        self._base_schemas = base_schemas

    async def migrate(self):
        await Migrator().run()
        return self

    async def _create(self, create_object: _base_schemas | list[_base_schemas]) -> bool:
        if isinstance(create_object, list):
            for item in create_object:
                await item.save()
        else:
            await create_object.save()

        return True

    async def _get_one_or_none(self, where_filter: Any) -> Optional[_base_schemas]:
        try:
            return await self._base_schemas.find(where_filter).first()
        except NotFoundError:
            return None

    async def _get_all(self,
                       where_filter: Any = None,
                       limit: int = 10000,
                       offset: int = 0) -> Any:
        try:
            if where_filter:
                query = self._base_schemas.find(where_filter)
            else:
                query = self._base_schemas.find()

            query.limit = limit
            query.offset = offset

            return await query.all()
        except NotFoundError:
            return None

    async def _delete(self, where_filter: Any) -> bool:
        models = await self._get_all(where_filter)
        await self._base_schemas.delete_many(models)
        return True
