from database.database import engine_sync
from database.models import Base

Base.metadata.drop_all(engine_sync)
Base.metadata.create_all(engine_sync)
