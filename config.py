from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8',
                                      extra="ignore")

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_LOGIN: str
    REDIS_PASSWORD: str
    REDIS_URL: str = ""

    # database
    PG_USER: str
    PG_PASSWORD: str
    PG_HOST: str
    PG_PORT: int
    PG_DB_NAME: str
    PG_SYNC_URL: str = ""
    PG_ASYNC_URL: str = ""


def get_settings():
    settings = Settings()

    settings.REDIS_URL = f"redis://{settings.REDIS_LOGIN}:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}"
    settings.PG_ASYNC_URL = f"postgresql+asyncpg://{settings.PG_USER}:{settings.PG_PASSWORD}@{settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DB_NAME}"
    settings.PG_SYNC_URL = f"postgresql://{settings.PG_USER}:{settings.PG_PASSWORD}@{settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DB_NAME}"

    return settings


settings = get_settings()
