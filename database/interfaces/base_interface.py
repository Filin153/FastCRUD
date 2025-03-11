import logging
from abc import abstractmethod
from typing import Any, get_args

from pydantic import BaseModel


class BaseDBInterface:
    @abstractmethod
    async def get_one_or_none(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    async def get_all(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    async def delete(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    async def soft_delete(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    async def update(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    async def create(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    async def uniq_col_value(self, *args, **kwargs) -> Any:
        pass


class SchemasValidator:
    @staticmethod
    async def valid_schema(schema: BaseModel.model_json_schema, **kwargs) -> dict:
        for key, val in kwargs.items():
            if key not in schema.model_fields.keys():
                logging.error(f"{schema} has not filed {key}")
                raise KeyError(f"{schema} has not filed {key}")

            if type(val) != get_args(schema.model_fields[key].annotation)[0]:
                logging.error(f"{schema} {key=} has invalid type {type(val)}")
                raise ValueError(f"{schema} {key=} has invalid type {type(val)}")

        if kwargs:
            return schema(**kwargs).model_dump(exclude_none=True, exclude_unset=True)
        else:
            return {}
