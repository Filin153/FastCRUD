import pytest
from sqlalchemy import and_

from database.interfaces.sql import BaseSQLInterface
from database.models.user import UserModel
from database.session import get_async_session
from .conftest import clear_all
from .schemas.user import *


@pytest.mark.run(order=1)
class TestSQLInterface:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_init(self):
        clear_all()
        async with get_async_session() as session:
            BaseSQLInterface(session,
                             UserModel,
                             UserSchemas,
                             UserCreate,
                             UserUpdate,
                             UserFilters)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create(self):
        async with get_async_session() as session:
            interface = BaseSQLInterface(session,
                                         UserModel,
                                         UserSchemas,
                                         UserCreate,
                                         UserUpdate,
                                         UserFilters)
            
            await interface.create(UserCreate(**{
                    "tg_id": 54,
                    "fio": "Aboba_54",
                    "group": "XD_54",
                "allow": True
                })
            )

            await interface.create(

                create_object=[
                    UserCreate(**{
                        "tg_id": 0,
                        "fio": "Aboba_0",
                        "group": "XD_0",
                        "allow": True
                    }),
                    UserCreate(**{
                        "tg_id": 1,
                        "fio": "Aboba_1",
                        "group": "XD_1",
                        "allow": True
                    })
                ]
            )
            await session.commit()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_one(self):
        async with get_async_session() as session:
            interface = BaseSQLInterface(session,
                                         UserModel,
                                         UserSchemas,
                                         UserCreate,
                                         UserUpdate,
                                         UserFilters)

            res_1 = await interface.get_one_or_none(

                tg_id=1,
            )
            assert res_1 != None
            assert res_1.tg_id == 1
            assert res_1.fio == "Aboba_1"
            assert res_1.group == "XD_1"

            res_2 = await interface.get_one_or_none(

                where_filter=and_(UserModel.tg_id >= 10, UserModel.tg_id <= 60)
            )
            assert res_2 != None
            assert res_2.tg_id == 54
            assert res_2.fio == "Aboba_54"
            assert res_2.group == "XD_54"

            res_3 = await interface.get_one_or_none(

                where_filter=and_(UserModel.tg_id >= 10, UserModel.tg_id <= 60, UserModel.fio != "Aboba_54")
            )
            assert res_3 == None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_some(self):
        async with get_async_session() as session:
            interface = BaseSQLInterface(session,
                                         UserModel,
                                         UserSchemas,
                                         UserCreate,
                                         UserUpdate,
                                         UserFilters)

            res_1 = await interface.get_all(

                tg_id=1,
            )
            assert res_1 != None
            for item in res_1:
                assert item.tg_id == 1
                assert item.fio == "Aboba_1"
                assert item.group == "XD_1"

            res_2 = await interface.get_all(

                where_filter=and_(UserModel.tg_id >= 10, UserModel.tg_id <= 60)
            )
            assert res_2 != None
            for item in res_2:
                assert item.tg_id == 54
                assert item.fio == "Aboba_54"
                assert item.group == "XD_54"

            res_3 = await interface.get_all(

                where_filter=and_(UserModel.tg_id >= 10, UserModel.tg_id <= 60, UserModel.fio != "Aboba_54")
            )
            assert res_3 == []

            res_4 = await interface.get_all(

                limit=1
            )
            assert len(res_4) == 1

            res_5 = await interface.get_all(

                limit=2
            )
            assert len(res_5) == 2

            res_6 = await interface.get_all(

                offset=1,
                limit=2
            )
            assert len(res_6) == 2
            assert res_6[0].tg_id == 0
            assert res_6[0].fio == "Aboba_0"
            assert res_6[0].group == "XD_0"
            assert res_6[1].tg_id == 1
            assert res_6[1].fio == "Aboba_1"
            assert res_6[1].group == "XD_1"

            res_7 = await interface.get_all(

                no_limit=True
            )
            assert len(res_7) == 3

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update(self):
        async with get_async_session() as session:
            interface = BaseSQLInterface(session,
                                         UserModel,
                                         UserSchemas,
                                         UserCreate,
                                         UserUpdate,
                                         UserFilters)

            await interface.update(

                update_object={
                    "fio": "XXX"
                },
                tg_id=0,
            )
            await session.flush()
            res_1 = await interface.get_one_or_none(

                tg_id=0,
            )
            assert res_1 != None
            assert res_1.fio == "XXX"

            await interface.update(

                update_object={
                    "fio": "qwerty"
                },
                where_filter=UserModel.tg_id >= 1,
            )
            await session.flush()
            res_2 = await interface.get_all(

                where_filter=UserModel.tg_id >= 1,
            )
            assert res_2 != []
            for item in res_2:
                assert item.fio == "qwerty"

            await session.commit()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_uniq_col_value(self):
        async with get_async_session() as session:
            interface = BaseSQLInterface(session,
                                         UserModel,
                                         UserSchemas,
                                         UserCreate,
                                         UserUpdate,
                                         UserFilters)

            all_uniq_val = await interface.uniq_col_value(

                col_name="tg_id"
            )
            assert len(all_uniq_val) == 3
            assert sorted(all_uniq_val) == sorted([54, 1, 0])

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete(self):
        async with get_async_session() as session:
            interface = BaseSQLInterface(session,
                                         UserModel,
                                         UserSchemas,
                                         UserCreate,
                                         UserUpdate,
                                         UserFilters)

            await interface.delete(

                tg_id=1
            )

            await session.flush()

            res_1 = await interface.get_one_or_none(

                tg_id=1,
            )
            assert res_1 == None

            await interface.soft_delete(

                tg_id=0
            )

            await session.flush()

            res_2 = await interface.get_one_or_none(

                tg_id=0,
            )
            assert res_2 == None

            await session.commit()
