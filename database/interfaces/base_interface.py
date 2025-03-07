from abc import abstractmethod
from typing import Any


class BaseDBInterface:
    @abstractmethod
    async def _query_execute(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    async def _get_one_or_none(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    async def _get_all(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    async def _delete(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    async def _soft_delete(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    async def _update(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    async def _create(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    async def uniq_col_value(self, *args, **kwargs) -> Any:
        pass
