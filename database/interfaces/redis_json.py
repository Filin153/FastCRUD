from typing import Any, Union

from aredis_om import NotFoundError
from redis_om import HashModel, RedisModel

from base_interface import BaseDBInterface


class SetRedisUrl:
    def __init__(self, redis_url: str):
        HashModel.database = redis_url


class BaseRedisInterface(BaseDBInterface):
    _base_schemas = None

    def __init__(self,
                 base_schemas: Union[HashModel, Any]
                 ):
        self._base_schemas = base_schemas

    async def _create(self, create_object: _base_schemas | list[_base_schemas],
                      default_primary_key: bool = False) -> bool:
        if isinstance(create_object, list):
            for item in create_object:
                if not default_primary_key:
                    item.primary_key = item.id
                item.save()
        else:
            if not default_primary_key:
                create_object.primary_key = create_object.id
            create_object.save()
        return True

    async def _get_one_or_none(self, where_filter: Any) -> _base_schemas | None:
        try:
            return self._base_schemas.find(where_filter).first()
        except NotFoundError:
            return None

    async def _get_all(self,
                       where_filter: Any,
                       limit: int = 10,
                       offset: int = 0,
                       no_limit: bool = False) -> Any:
        try:
            query = self._base_schemas.find(where_filter)
            if no_limit:
                query.limit = float("inf")
            else:
                query.limit = limit
            query.offset = offset
            return query.all()
        except NotFoundError:
            return None

    async def _delete(self, obj: int | _base_schemas) -> bool:
        if isinstance(obj, int):
            RedisModel.delete(obj)
        else:
            RedisModel.delete_many(obj)

        return True
