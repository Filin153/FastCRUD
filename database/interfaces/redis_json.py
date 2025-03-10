import warnings
from abc import ABC
from typing import Any, Optional

from aredis_om import NotFoundError, get_redis_connection, HashModel, Migrator
from pydantic import BaseModel

from config import settings
from .base_interface import BaseDBInterface, SchemasValidator


class BaseRedisModel(HashModel, ABC):
    class Meta:
        global_key_prefix = "db_cache"
        database = get_redis_connection(url=settings.REDIS_URL)


class BaseRedisInterface(BaseDBInterface, SchemasValidator):
    _base_schemas = None
    _filter_schemas = None

    def __init__(self,
                 base_schemas: HashModel.model_json_schema,
                 filter_schemas: BaseModel.model_json_schema
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
        self._filter_schemas = filter_schemas

    async def migrate(self):
        await Migrator().run()
        return self

    async def __connect_filter_with_kwargs(self, where_filter: Any = None,
                                           error: bool = True, **kwargs):
        if error:
            if where_filter is None and not kwargs:
                raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        kwargs = await self.valid_schema(self._filter_schemas, **kwargs)

        for key, value in kwargs.items():
            model_item = getattr(self._base_schemas, key)
            if not where_filter:
                where_filter = model_item == value
            else:
                where_filter = where_filter & (model_item == value)

        return where_filter

    async def __create(self, create_object: _base_schemas):
        try:
            await self._delete(self._base_schemas.id == create_object.id)
        except:
            pass
        await create_object.save()
        return True

    async def _create(self, create_object: _base_schemas | list[_base_schemas]) -> bool:
        if isinstance(create_object, list):
            for item in create_object:
                await self.__create(item)
        else:
            await self.__create(create_object)

        return True

    async def _get_one_or_none(self, where_filter: Any = None, **kwargs) -> Optional[_base_schemas]:
        try:
            where_filter = await self.__connect_filter_with_kwargs(where_filter, **kwargs)
            return await self._base_schemas.find(where_filter).first()
        except NotFoundError:
            return None

    async def _get_all(self,
                       where_filter: Any = None,
                       limit: int = 10,
                       offset: int = 0,
                       **kwargs) -> list[_base_schemas]:
        try:
            where_filter = await self.__connect_filter_with_kwargs(where_filter,
                                                                   error=False,
                                                                   **kwargs)

            if where_filter:
                where_filter = where_filter & (self._base_schemas.id >= offset)
            else:
                where_filter = self._base_schemas.id >= offset

            if where_filter:
                query = self._base_schemas.find(where_filter)
            else:
                query = self._base_schemas.find()

            query.limit = limit
            query.offset = 0

            return await query.all()
        except NotFoundError:
            return []

    async def __update(self, update_object: _base_schemas):
        del_res = await self._delete(self._base_schemas.id == update_object.id)
        if del_res == False:
            return False
        await self._create(update_object)
        return True

    async def _update(self, update_object: _base_schemas | list[_base_schemas]) -> bool:
        if isinstance(update_object, list):
            for item in update_object:
                await self.__update(item)
        else:
            await self.__update(update_object)

    async def _delete(self, where_filter: Any) -> bool:
        models = await self._get_all(where_filter)
        if not models:
            return False
        await self._base_schemas.delete_many(models)
        return True
