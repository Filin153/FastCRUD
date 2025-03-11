import pytest
from sqlalchemy import and_

from database.interfaces.main_interface import MainCRUDInterface
from database.models.user import UserModel
from database.session import get_async_session
from .conftest import clear_all
from .schemas.user import *


@pytest.mark.run(order=3)
class TestMainInterface:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_init(self):
        clear_all()
        await MainCRUDInterface.init(UserModel,
                                     UserSchemas,
                                     UserCreate,
                                     UserUpdate,
                                     UserFilters)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create(self):
        interface = await MainCRUDInterface.init(UserModel,
                                                 UserSchemas,
                                                 UserCreate,
                                                 UserUpdate,
                                                 UserFilters)

        async with get_async_session() as session:
            await interface.create(
                session=session,
                create_object=UserCreate(**{
                    "tg_id": 54,
                    "fio": "Aboba_54",
                    "group": "XD_54"
                })
            )

            await interface.create(
                session=session,
                create_object=[
                    UserCreate(**{
                        "tg_id": 0,
                        "fio": "Aboba_0",
                        "group": "XD_0"
                    }),
                    UserCreate(**{
                        "tg_id": 1,
                        "fio": "Aboba_1",
                        "group": "XD_1"
                    })
                ]
            )
            await session.commit()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_one(self):
        interface = await MainCRUDInterface.init(UserModel,
                                                 UserSchemas,
                                                 UserCreate,
                                                 UserUpdate,
                                                 UserFilters
                                                 )
        async with get_async_session() as session:
            res_1 = await interface.get_one_or_none(
                session=session,
                tg_id=1,
            )
            assert res_1 != None
            assert res_1.tg_id == 1
            assert res_1.fio == "Aboba_1"
            assert res_1.group == "XD_1"

            res_2 = await interface.get_one_or_none(
                session=session,
                where_filter_sql=UserModel.tg_id >= 10,
                where_filter_redis=UserSchemas.tg_id >= 10
            )
            assert res_2 != None
            assert res_2.tg_id == 54
            assert res_2.fio == "Aboba_54"
            assert res_2.group == "XD_54"

            res_3 = await interface.get_one_or_none(
                session=session,
                where_filter_sql=and_(UserModel.tg_id >= 10, UserModel.fio != "Aboba_54"),
                where_filter_redis=(UserSchemas.tg_id >= 10) & (UserSchemas.fio != "Aboba_54")
            )
            assert res_3 == None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_some(self):
        interface = await MainCRUDInterface.init(UserModel,
                                                 UserSchemas,
                                                 UserCreate,
                                                 UserUpdate,
                                                 UserFilters)

        async with get_async_session() as session:
            res_1 = await interface.get_all(
                session=session,
                tg_id=1,
            )
            assert res_1 != None
            for item in res_1:
                assert item.tg_id == 1
                assert item.fio == "Aboba_1"
                assert item.group == "XD_1"

            res_2 = await interface.get_all(
                session=session,
                where_filter_sql=UserModel.tg_id >= 10,
                where_filter_redis=UserSchemas.tg_id >= 10
            )
            assert res_2 != None
            for item in res_2:
                assert item.tg_id == 54
                assert item.fio == "Aboba_54"
                assert item.group == "XD_54"

            res_3 = await interface.get_all(
                session=session,
                where_filter_sql=and_(UserModel.tg_id >= 10, UserModel.fio != "Aboba_54"),
                where_filter_redis=(UserSchemas.tg_id >= 10) & (UserSchemas.fio != "Aboba_54")
            )
            assert res_3 == []

            res_4 = await interface.get_all(
                session=session,
                limit=1
            )
            assert len(res_4) == 1

            res_5 = await interface.get_all(
                session=session,
                limit=2
            )
            assert len(res_5) == 2

            res_6 = await interface.get_all(
                session=session,
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
                session=session,
                no_limit=True
            )
            assert len(res_7) == 3

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update(self):
        interface = await MainCRUDInterface.init(UserModel,
                                                 UserSchemas,
                                                 UserCreate,
                                                 UserUpdate,
                                                 UserFilters)

        async with get_async_session() as session:
            await interface.update(
                session=session,
                update_object={
                    "fio": "XXX"
                },
                tg_id=0,
            )
            await session.flush()
            res_1 = await interface.get_one_or_none(
                session=session,
                tg_id=0,
            )
            assert res_1 != None
            assert res_1.fio == "XXX"

            await interface.update(
                session=session,
                update_object={
                    "fio": "qwerty"
                },
                where_filter_sql=UserModel.tg_id >= 1,
            )
            await session.flush()
            res_2 = await interface.get_all(
                session=session,
                where_filter_sql=UserModel.tg_id >= 1,
                where_filter_redis=UserSchemas.tg_id >= 1,
            )
            assert res_2 != []
            for item in res_2:
                assert item.fio == "qwerty"

            await session.commit()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_uniq_col_value(self):
        interface = await MainCRUDInterface.init(UserModel,
                                                 UserSchemas,
                                                 UserCreate,
                                                 UserUpdate,
                                                 UserFilters)

        async with get_async_session() as session:
            all_uniq_val = await interface.sql.uniq_col_value(
                session=session,
                col_name="tg_id"
            )
            assert len(all_uniq_val) == 3
            assert sorted(all_uniq_val) == sorted([54, 0, 1])

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete(self):
        interface = await MainCRUDInterface.init(UserModel,
                                                 UserSchemas,
                                                 UserCreate,
                                                 UserUpdate,
                                                 UserFilters)

        async with get_async_session() as session:
            # For set to Redis
            await interface.get_one_or_none(
                session=session,
                tg_id=0,
            )

            await interface.get_one_or_none(
                session=session,
                tg_id=1,
            )
            # --------

            await interface.delete(
                session=session,
                soft=False,
                tg_id=1
            )

            await session.flush()

            res_1 = await interface.get_one_or_none(
                session=session,
                tg_id=1,
            )
            assert res_1 == None

            await interface.delete(
                session=session,
                soft=True,
                tg_id=0
            )

            await session.flush()

            res_2 = await interface.get_one_or_none(
                session=session,
                tg_id=0,
            )
            assert res_2 == None

            await session.commit()
