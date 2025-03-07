from typing import Union, Any, Optional

from pydantic import BaseModel
from sqlalchemy import select, update, delete, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from .base_interface import BaseDBInterface


class BaseSQLInterface(BaseDBInterface):
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

    async def _query_execute(self,
                             session: AsyncSession,
                             query: Any = None) -> Any:
        try:
            return await session.execute(query)
        except Exception as e:
            await session.rollback()
            raise e

    async def _get_one_or_none(self,
                               session: AsyncSession,
                               where_filter: Any = None,
                               **kwargs) -> Optional[_base_schemas]:

        if where_filter is None and not kwargs:
            raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        query = select(self._db_model)

        if kwargs:
            query = query.filter_by(**kwargs)

        if where_filter is not None:
            query = query.where(where_filter)

        res = await self._query_execute(session, query)
        response_object = res.scalars().one_or_none()

        if not response_object:
            return None
        return self._base_schemas.model_validate(response_object, from_attributes=True)

    async def _get_all(self,
                       session: AsyncSession,
                       where_filter: Any = None,
                       limit: int = 10,
                       offset: int = 0,
                       no_limit: bool = False,
                       **kwargs) -> list[_base_schemas]:

        if no_limit:
            query = select(self._db_model).offset(offset)
        else:
            query = select(self._db_model).offset(offset).limit(limit)

        if kwargs:
            query = query.filter_by(**kwargs)

        if where_filter is not None:
            query = query.where(where_filter)

        res = await self._query_execute(session, query)
        response_object = res.scalars().all()

        if not response_object:
            return []
        return [self._base_schemas.model_validate(resp_obj, from_attributes=True) for resp_obj in response_object]

    async def _delete(self,
                      session: AsyncSession,
                      where_filter: Any = None,
                      **kwargs) -> bool:
        if where_filter is None and not kwargs:
            raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        query = delete(self._db_model)

        if kwargs:
            query = query.filter_by(**kwargs)

        if where_filter is not None:
            query = query.where(where_filter)

        await self._query_execute(session, query)
        return True

    async def _soft_delete(self,
                           session: AsyncSession,
                           where_filter: Any = None,
                           **kwargs) -> bool:
        if where_filter is None and not kwargs:
            raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        query = update(self._db_model)

        if kwargs:
            query = query.filter_by(**kwargs)

        if where_filter is not None:
            query = query.where(where_filter)

        query = query.values({"delete_at": func.now()})

        await self._query_execute(session, query)
        return True

    async def _update(self,
                      session: AsyncSession,
                      update_object: _update_schemas,
                      where_filter: Any = None,
                      **kwargs) -> bool:

        if where_filter is None and not kwargs:
            raise ValueError('At least one of `where_filter` or `kwargs` must be set')

        true_update_object = update_object.model_dump(exclude_none=True, exclude_unset=True)

        query = update(self._db_model)

        if kwargs:
            query = query.filter_by(**kwargs)

        if where_filter is not None:
            query = query.where(where_filter)

        query = query.values(**true_update_object)

        await self._query_execute(session, query)
        return True

    async def _create(self,
                      session: AsyncSession,
                      create_object: _create_schemas | list[_create_schemas]) -> bool:
        if isinstance(create_object, list):
            add_object = [self._db_model(**obj.model_dump()) for obj in create_object]
            session.add_all(add_object)
        else:
            add_object = self._db_model(**create_object.model_dump())
            session.add(add_object)

        return True

    async def uniq_col_value(self, session: AsyncSession, col_name: str):
        model_item = getattr(self._db_model, col_name)
        result = await session.execute(select(distinct(model_item)))
        return result.scalars().all()
