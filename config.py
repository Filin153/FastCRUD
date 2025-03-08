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
    pg_user: str
    pg_pass: str
    pg_host: str
    pg_port: int
    pg_db_name: str


def get_settings():
    settings = Settings()

    settings.REDIS_URL = f"redis://{settings.REDIS_LOGIN}:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}"

    return settings


settings = get_settings()
