import warnings
from abc import ABC
from typing import Any, Optional, Union

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
        self.bool_filed = []
        self.find_bool_filed()

    async def __bool_convert(self, val: bool | int):
        if isinstance(val, bool):
            return int(val)
        else:
            return bool(val)

    def find_bool_filed(self):
        for key, type_value in self._base_schemas.__annotations__.items():
            try:
                if bool in type_value.__args__:
                    self._base_schemas.__annotations__[key] = Union[bool, int]
                    self.bool_filed.append(key)
            except:
                pass

    async def migrate(self):
        await Migrator().run()
        return self

    async def __bool_convert_for_object(self, val_obj: BaseModel | list[BaseModel]):
        if isinstance(val_obj, list):
            res = []
            for val in val_obj:
                for key in val.model_dump().keys():
                    if key in self.bool_filed:
                        setattr(val, key, await self.__bool_convert(getattr(val, key)))
                        res.append(val)
            return res
        else:
            for key in val_obj.model_dump().keys():
                if key in self.bool_filed:
                    setattr(val_obj, key, await self.__bool_convert(getattr(val_obj, key)))
            return val_obj

    async def __connect_filter_with_kwargs(self, where_filter: Any = None,
                                           error: bool = True, **kwargs):
        if error:
            if where_filter is None and not kwargs:
                raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        kwargs = await self.valid_schema(self._filter_schemas, **kwargs)

        for key, value in kwargs.items():
            if key in self.bool_filed:
                value = self.__bool_convert(value)

            model_item = getattr(self._base_schemas, key)
            if not where_filter:
                where_filter = model_item == value
            else:
                where_filter = where_filter & (model_item == value)

        return where_filter

    async def __create(self, create_object: _base_schemas):
        create_object = await self.__bool_convert_for_object(create_object)
        print(create_object.model_dump())
        try:
            await self.delete(self._base_schemas.id == create_object.id)
        except:
            pass
        await create_object.save()
        return True

    async def create(self, create_object: _base_schemas | list[_base_schemas]) -> bool:
        if isinstance(create_object, list):
            for item in create_object:
                await self.__create(item)
        else:
            await self.__create(create_object)
        return True

    async def get_one_or_none(self, where_filter: Any = None, **kwargs) -> Optional[_base_schemas]:
        try:
            where_filter = await self.__connect_filter_with_kwargs(where_filter, **kwargs)
            res = await self._base_schemas.find(where_filter).first()
            res = await self.__bool_convert_for_object(res)
            return res
        except NotFoundError:
            return None

    async def get_all(self,
                      where_filter: Any = None,
                      limit: int = 10,
                      offset: int = 0,
                      **kwargs) -> list[_base_schemas]:
        try:
            where_filter = await self.__connect_filter_with_kwargs(where_filter,
                                                                   error=False,
                                                                   **kwargs)

            if where_filter:
                where_filter = where_filter & (
                        (self._base_schemas.id >= offset) & (self._base_schemas.id <= limit + offset - 1))
            else:
                where_filter = (self._base_schemas.id >= offset) & (self._base_schemas.id < limit + offset - 1)

            res = await self._base_schemas.find(where_filter).all()
            res = await self.__bool_convert_for_object(res)

            return res
        except NotFoundError:
            return []

    async def __update(self, update_object: _base_schemas):
        await self.delete(self._base_schemas.id == update_object.id)
        await self.create(update_object)
        return True

    async def update(self, update_object: _base_schemas | list[_base_schemas]) -> bool:
        if isinstance(update_object, list):
            for item in update_object:
                await self.__update(item)
        else:
            await self.__update(update_object)

    async def delete(self, where_filter: Any) -> bool:
        models = await self.get_all(where_filter)
        if not models:
            return False
        await self._base_schemas.delete_many(models)
        return True
