import pytest

from database.interfaces.redis_json import BaseRedisInterface
from tests.conftest import clear_all
from tests.schemas.user import UserSchemas, UserFilters


@pytest.mark.run(order=2)
class TestRedisDB:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_init(self):
        clear_all()
        await BaseRedisInterface(UserSchemas,
                                 UserFilters).migrate()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create(self):
        interface = await BaseRedisInterface(UserSchemas,
                                             UserFilters).migrate()

        await interface._create(UserSchemas(**{
            "id": 0,
            "tg_id": 0,
            "fio": "Aboba 0",
            "group": "XD 0"
        }))

        await interface._create([
            UserSchemas(**{
                "id": 1,
                "tg_id": 1,
                "fio": "Aboba 1",
                "group": "XD 1"
            }),
            UserSchemas(**{
                "id": 2,
                "tg_id": 2,
                "fio": "Aboba 2",
                "group": "XD 2"
            })
        ])

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_one(self):
        interface = await BaseRedisInterface(UserSchemas,
                                             UserFilters).migrate()

        res_1 = await interface._get_one_or_none(id=0)
        assert res_1 != None
        assert res_1.tg_id == 0

        res_2 = await interface._get_one_or_none(UserSchemas.fio == "Aboba 2")
        assert res_2 != None
        assert res_2.id == 2
        assert res_2.tg_id == 2
        assert res_2.group == "XD 2"

        res_3 = await interface._get_one_or_none(UserSchemas.tg_id >= 1,
                                                 fio="Aboba 2")
        assert res_3 != None
        assert res_3.id == 2
        assert res_3.tg_id == 2
        assert res_3.group == "XD 2"

        res_4 = await interface._get_one_or_none(UserSchemas.fio % "2")
        assert res_4 != None
        assert res_4.id == 2
        assert res_4.tg_id == 2
        assert res_4.group == "XD 2"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_some(self):
        interface = await BaseRedisInterface(UserSchemas,
                                             UserFilters).migrate()

        res_1 = await interface._get_all(UserSchemas.tg_id >= 1)
        assert res_1 != None
        assert res_1[0].id == 1
        assert res_1[1].id == 2
        assert res_1[0].group == "XD 1"
        assert res_1[1].group == "XD 2"

        res_2 = await interface._get_all(UserSchemas.group % "XD")
        assert res_2 != None
        assert len(res_2) == 3

        res_3 = await interface._get_all(UserSchemas.group % "XD", id=2)
        assert res_3 != None
        assert len(res_3) == 1
        assert res_3[0].group == "XD 2"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete(self):
        interface = await BaseRedisInterface(UserSchemas,
                                             UserFilters).migrate()

        res_1 = await interface._delete(UserSchemas.tg_id >= 1)
        assert res_1 == True

        res_2 = await interface._get_all()
        assert res_2 != None
        assert len(res_2) == 1
