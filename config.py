from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8',
                                      extra="ignore")

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_PASSWORD: str

    # database
    pg_user: str
    pg_pass: str
    pg_host: str
    pg_port: int
    pg_db_name: str


settings = Settings()



