from redis_om import get_redis_connection

from config import settings
from database.database import engine_sync
from database.models import Base


def clear_all():
    Base.metadata.drop_all(engine_sync)
    Base.metadata.create_all(engine_sync)

    redis = get_redis_connection(url=settings.REDIS_URL)
    redis.flushall()
