import asyncio
from typing import Union, Any, Optional

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from .base_interface import BaseDBInterface
from .redis_json import BaseRedisInterface
from .sql import BaseSQLInterface


class MainCRUDInterface(BaseDBInterface):
    _db_model = None
    _base_schemas = None
    _create_schemas = None
    _update_schemas = None
    _filters_schemas = None

    def __init__(self, db_model: Union[DeclarativeBase, Any],
                 base_schemas: Union[BaseModel, Any],
                 create_schemas: Union[BaseModel, Any],
                 update_schemas: Union[BaseModel, Any],
                 filters_schemas: Union[BaseModel, Any]):
        self._db_model = db_model
        self._base_schemas = base_schemas
        self._create_schemas = create_schemas
        self.update_schemas = update_schemas
        self._filters_schemas = filters_schemas

        self.__redis = BaseRedisInterface(self._base_schemas,
                                          self._filters_schemas)
        self.__sql = BaseSQLInterface(self.session,
                                      self._db_model,
                                      self._base_schemas,
                                      self._create_schemas,
                                      self.update_schemas,
                                      self._filters_schemas)

    @property
    def sql(self):
        return self.__sql

    @property
    def redis(self):
        return self.__redis

    async def __migrate(self):
        await self.__redis.migrate()

    @classmethod
    async def init(cls, session: AsyncSession, db_model, base_schemas, create_schemas, update_schemas, filters_schemas):
        cls.session = session
        instance = cls(db_model, base_schemas, create_schemas, update_schemas, filters_schemas)
        await instance.__migrate()
        return instance

    async def get_one_or_none(self,
                              where_filter_sql: Any = None,
                              where_filter_redis: Any = None,
                              **kwargs
                              ) -> Optional[_base_schemas]:
        res = None

        if where_filter_redis is not None or kwargs is not None:
            res = await self.__redis.get_one_or_none(where_filter=where_filter_redis,
                                                     **kwargs)
            if res:
                return res

        if res is None and (where_filter_redis is not None or kwargs is not None):
            res = await self.__sql.get_one_or_none(where_filter=where_filter_sql,
                                                   **kwargs)
            if res:
                asyncio.create_task(self.__redis.create(res))

        return res

    async def get_all(self,
                      where_filter_sql: Any = None,
                      where_filter_redis: Any = None,
                      no_limit: bool = False,
                      limit: int = 10,
                      offset: int = 0,
                      **kwargs
                      ) -> list[_base_schemas]:
        if limit > 10000 and no_limit == False:
            raise ValueError("limit must be less than 10000")

        res = []

        if (where_filter_redis is not None or kwargs is not None) and no_limit == False:
            redis_res = await self.__redis.get_all(where_filter=where_filter_redis,
                                                   limit=limit,
                                                   offset=offset,
                                                   **kwargs)
            if len(redis_res) >= limit:
                return redis_res[:limit + 1]
            else:
                res += redis_res

        all_res_id = [item.id for item in res]
        while True:
            if where_filter_sql is not None:
                where_filter_sql = where_filter_sql & (self._db_model.id.not_in(all_res_id))
            else:
                where_filter_sql = self._db_model.id.not_in(all_res_id)
            sql_res = await self.__sql.get_all(where_filter=where_filter_sql,
                                               limit=limit,
                                               offset=offset,
                                               no_limit=no_limit,
                                               **kwargs)

            if no_limit:
                return sql_res

            if not sql_res:
                break

            asyncio.create_task(self.__redis.create(sql_res))
            all_res_id += [item.id for item in sql_res]
            res += sql_res
            if len(set(all_res_id)) >= limit:
                return res[:limit + 1]

            offset += limit
            limit -= len(res)

        return res

    async def create(self,
                     create_object: _create_schemas | list[_create_schemas]) -> bool:
        return await self.__sql.create(create_object)

    async def update(self,
                     update_object: dict,
                     where_filter_sql: Any = None,
                     **kwargs) -> bool:
        await self.__sql.update(update_object, where_filter_sql, **kwargs)
        obj = await self.__sql.get_all(where_filter_sql, **kwargs)
        await self.__redis.update(obj)
        return True

    async def delete(self,
                     where_filter: Any = None,
                     soft: bool = True,
                     **kwargs) -> bool:
        res = await self.__sql.get_all(where_filter, **kwargs)
        for item in res:
            await self.__redis.delete(self._base_schemas.id == item.id)
        if soft:
            await self.__sql.soft_delete(where_filter, **kwargs)
        else:
            await self.__sql.delete(where_filter, **kwargs)
        return True

    async def uniq_col_value(self, col_name: str) -> list[Any]:
        return await self.__sql.uniq_col_value(col_name)
