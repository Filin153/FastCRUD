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
        self._update_schemas = update_schemas
        self._filters_schemas = filters_schemas

        self.__redis = BaseRedisInterface(self._base_schemas,
                                          self._filters_schemas)
        self.__sql = BaseSQLInterface(self._db_model,
                                      self._base_schemas,
                                      self._create_schemas,
                                      self._update_schemas,
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
    async def init(cls, db_model, base_schemas, create_schemas, update_schemas, filters_schemas):
        instance = cls(db_model, base_schemas, create_schemas, update_schemas, filters_schemas)
        await instance.__migrate()
        return instance

    async def _get_one_or_none(self,
                               session: AsyncSession,
                               where_filter_sql: Any = None,
                               where_filter_redis: Any = None,
                               **kwargs
                               ) -> Optional[_base_schemas]:
        res = None

        if where_filter_redis or kwargs:
            res = await self.__redis._get_one_or_none(where_filter=where_filter_redis,
                                                      **kwargs)
            print(f"Redis {res=}")

        if res is None and (where_filter_sql is not None or kwargs):
            res = await self.__sql._get_one_or_none(session=session,
                                                    where_filter=where_filter_sql,
                                                    **kwargs)
            if res:
                await self.__redis._create(res)

        return res

    async def _get_all(self,
                       session: AsyncSession,
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

        if where_filter_redis or kwargs:
            redis_res = await self.__redis._get_all(where_filter=where_filter_redis,
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
            sql_res = await self.__sql._get_all(session=session,
                                                where_filter=where_filter_sql,
                                                limit=limit,
                                                offset=offset,
                                                no_limit=no_limit,
                                                **kwargs)
            if not sql_res:
                break

            await self.__redis._create(sql_res)
            all_res_id += [item.id for item in sql_res]
            res += sql_res
            if len(set(all_res_id)) >= limit:
                return res[:limit + 1]

            offset += limit
            limit -= len(res)

        return res

    async def _create(self,
                      session: AsyncSession,
                      create_object: _create_schemas | list[_create_schemas]) -> bool:
        return await self.__sql._create(session, create_object)

    async def _update(self,  ## TODO: Перенести __valid_XXX в главные классы!
                      session: AsyncSession,
                      update_object: dict,
                      where_filter_sql: Any = None,
                      **kwargs) -> bool:
        await self.__sql._update(session, update_object, where_filter_sql, **kwargs)
        obj = await self.__sql._get_all(session, where_filter_sql, **kwargs)
        await self.__redis._update(obj)
        return True

    async def _delete(self,
                      session: AsyncSession,
                      where_filter: Any = None,
                      soft: bool = True,
                      **kwargs) -> bool:
        res = await self.__sql._get_one_or_none(session, where_filter, **kwargs)
        print(f"Delete {res=}")
        await self.__redis._delete(self._base_schemas.id == res.id)
        if soft:
            await self.__sql._soft_delete(session, where_filter, **kwargs)
        else:
            await self.__sql._delete(session, where_filter, **kwargs)
        return True
