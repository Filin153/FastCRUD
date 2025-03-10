import pytest
from sqlalchemy import and_

from database.interfaces.sql import BaseSQLInterface
from database.models.user import UserModel
from database.session import get_async_session
from .schemas.user import *


class TestSQLInterface:
    def test_init(self):
        BaseSQLInterface(UserModel,
                         UserSchemas,
                         UserCreate,
                         UserUpdate,
                         UserFilters)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create(self):
        interface = BaseSQLInterface(UserModel,
                                     UserSchemas,
                                     UserCreate,
                                     UserUpdate,
                                     UserFilters)

        async with get_async_session() as session:
            await interface._create(
                session=session,
                create_object=UserCreate(**{
                    "tg_id": 54,
                    "fio": "Aboba_54",
                    "group": "XD_54"
                })
            )

            await interface._create(
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
        interface = BaseSQLInterface(UserModel,
                                     UserSchemas,
                                     UserCreate,
                                     UserUpdate,
                                     UserFilters)
        async with get_async_session() as session:
            res_1 = await interface._get_one_or_none(
                session=session,
                tg_id=1,
            )
            assert res_1 != None
            assert res_1.tg_id == 1
            assert res_1.fio == "Aboba_1"
            assert res_1.group == "XD_1"

            res_2 = await interface._get_one_or_none(
                session=session,
                where_filter=UserModel.tg_id >= 10
            )
            assert res_2 != None
            assert res_2.tg_id == 54
            assert res_2.fio == "Aboba_54"
            assert res_2.group == "XD_54"

            res_3 = await interface._get_one_or_none(
                session=session,
                where_filter=and_(UserModel.tg_id >= 10, UserModel.fio != "Aboba_54")
            )
            assert res_3 == None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_some(self):
        interface = BaseSQLInterface(UserModel,
                                     UserSchemas,
                                     UserCreate,
                                     UserUpdate,
                                     UserFilters)

        async with get_async_session() as session:
            res_1 = await interface._get_all(
                session=session,
                tg_id=1,
            )
            assert res_1 != None
            for item in res_1:
                assert item.tg_id == 1
                assert item.fio == "Aboba_1"
                assert item.group == "XD_1"

            res_2 = await interface._get_all(
                session=session,
                where_filter=UserModel.tg_id >= 10
            )
            assert res_2 != None
            for item in res_2:
                assert item.tg_id == 54
                assert item.fio == "Aboba_54"
                assert item.group == "XD_54"

            res_3 = await interface._get_all(
                session=session,
                where_filter=and_(UserModel.tg_id >= 10, UserModel.fio != "Aboba_54")
            )
            assert res_3 == []

            res_4 = await interface._get_all(
                session=session,
                limit=1
            )
            assert len(res_4) == 1

            res_5 = await interface._get_all(
                session=session,
                limit=2
            )
            assert len(res_5) == 2

            res_6 = await interface._get_all(
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

            res_7 = await interface._get_all(
                session=session,
                no_limit=True
            )
            assert len(res_7) == 3

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update(self):
        interface = BaseSQLInterface(UserModel,
                                     UserSchemas,
                                     UserCreate,
                                     UserUpdate,
                                     UserFilters)

        async with get_async_session() as session:
            await interface._update(
                session=session,
                update_object=UserUpdate(**{
                    "fio": "XXX"
                }),
                tg_id=0,
            )
            await session.flush()
            res_1 = await interface._get_one_or_none(
                session=session,
                tg_id=0,
            )
            assert res_1 != None
            assert res_1.fio == "XXX"

            await interface._update(
                session=session,
                update_object=UserUpdate(**{
                    "fio": "qwerty"
                }),
                where_filter=UserModel.tg_id >= 1,
            )
            await session.flush()
            res_2 = await interface._get_all(
                session=session,
                where_filter=UserModel.tg_id >= 1,
            )
            assert res_2 != []
            for item in res_2:
                assert item.fio == "qwerty"

            await session.commit()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_uniq_col_value(self):
        interface = BaseSQLInterface(UserModel,
                                     UserSchemas,
                                     UserCreate,
                                     UserUpdate,
                                     UserFilters)

        async with get_async_session() as session:
            all_uniq_val = await interface.uniq_col_value(
                session=session,
                col_name="tg_id"
            )
            assert len(all_uniq_val) == 3
            assert all_uniq_val == [54, 0, 1]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete(self):
        interface = BaseSQLInterface(UserModel,
                                     UserSchemas,
                                     UserCreate,
                                     UserUpdate,
                                     UserFilters)

        async with get_async_session() as session:
            await interface._delete(
                session=session,
                tg_id=1
            )

            await session.flush()

            res_1 = await interface._get_one_or_none(
                session=session,
                tg_id=1,
            )
            assert res_1 == None

            await interface._soft_delete(
                session=session,
                tg_id=0
            )

            await session.flush()

            res_2 = await interface._get_one_or_none(
                session=session,
                where_filter=UserModel.delete_at == None,
                tg_id=0,
            )
            assert res_2 == None

            await session.commit()
