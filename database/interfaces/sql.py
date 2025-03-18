import logging
from typing import Any, Optional

from pydantic import BaseModel
from sqlalchemy import select, update, delete, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from .base_interface import BaseDBInterface, SchemasValidator


class BaseSQLInterface(BaseDBInterface, SchemasValidator):
    _db_model = None
    _base_schemas = None
    _create_schemas = None
    _update_schemas = None

    def __init__(self, session: AsyncSession,
                 db_model: DeclarativeBase,
                 base_schemas: BaseModel.model_json_schema,
                 create_schemas: BaseModel.model_json_schema,
                 update_schemas: BaseModel.model_json_schema,
                 filter_schemas: BaseModel.model_json_schema):
        self.session = session
        self._db_model = db_model
        self._base_schemas = base_schemas
        self._create_schemas = create_schemas
        self._update_schemas = update_schemas
        self._filter_schemas = filter_schemas

    async def __add_filter_to_query(self, query: Any, **kwargs) -> Any:
        if kwargs:
            filters = await self.valid_schema(self._filter_schemas, **kwargs)
            return query.filter_by(**filters)
        return query

    async def __set_delete_at_limit(self, query: Any) -> Any:
        return query.where(self._db_model.delete_at.is_(None))

    async def query_execute(self,
                            query: Any = None) -> Any:
        try:
            return await self.session.execute(query)
        except Exception as e:
            await self.session.rollback()
            logging.error(e)
            raise e

    async def get_one_or_none(self,
                              where_filter: Any = None,
                              **kwargs) -> Optional[_base_schemas]:

        if where_filter is None and not kwargs:
            raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        query = select(self._db_model)

        query = await self.__add_filter_to_query(query, **kwargs)
        query = await self.__set_delete_at_limit(query)

        if where_filter is not None:
            query = query.where(where_filter)

        res = await self.query_execute(query)
        response_object = res.scalars().one_or_none()

        if not response_object:
            return None
        return self._base_schemas.model_validate(response_object, from_attributes=True)

    async def get_all(self,
                      where_filter: Any = None,
                      limit: int = 10,
                      offset: int = 0,
                      no_limit: bool = False,
                      **kwargs) -> list[_base_schemas]:

        if no_limit:
            query = select(self._db_model).offset(offset)
        else:
            query = select(self._db_model).offset(offset).limit(limit)

        query = await self.__add_filter_to_query(query, **kwargs)
        query = await self.__set_delete_at_limit(query)

        if where_filter is not None:
            query = query.where(where_filter)

        res = await self.query_execute(query)
        response_object = res.scalars().all()

        if not response_object:
            return []
        return [self._base_schemas.model_validate(resp_obj, from_attributes=True) for resp_obj in response_object]

    async def delete(self,
                     where_filter: Any = None,
                     **kwargs) -> bool:
        if where_filter is None and not kwargs:
            raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        query = delete(self._db_model)

        query = await self.__add_filter_to_query(query, **kwargs)
        query = await self.__set_delete_at_limit(query)

        if where_filter is not None:
            query = query.where(where_filter)

        await self.query_execute(query)
        return True

    async def soft_delete(self,
                          where_filter: Any = None,
                          **kwargs) -> bool:
        if where_filter is None and not kwargs:
            raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        query = update(self._db_model)

        query = await self.__add_filter_to_query(query, **kwargs)
        query = await self.__set_delete_at_limit(query)

        if where_filter is not None:
            query = query.where(where_filter)

        query = query.values(delete_at=func.now())

        await self.query_execute(query)
        return True

    async def update(self,
                     update_object: dict,
                     where_filter: Any = None,
                     **kwargs) -> bool:

        if where_filter is None and not kwargs:
            raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        update_object = await self.valid_schema(self._update_schemas, **update_object)

        query = update(self._db_model)

        query = await self.__add_filter_to_query(query, **kwargs)
        query = await self.__set_delete_at_limit(query)

        if where_filter is not None:
            query = query.where(where_filter)

        query = query.values(**update_object)

        await self.query_execute(query)
        return True

    async def create(self,
                     create_object: _create_schemas | list[_create_schemas]) -> bool:
        if isinstance(create_object, list):
            add_object = [self._db_model(**obj.model_dump()) for obj in create_object]
            self.session.add_all(add_object)
        else:
            add_object = self._db_model(**create_object.model_dump())
            self.session.add(add_object)

        return add_object

    async def uniq_col_value(self, col_name: str):
        model_item = getattr(self._db_model, col_name)
        result = await self.session.execute(select(distinct(model_item)))
        return result.scalars().all()
