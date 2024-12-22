from pydantic import ConfigDict, EmailStr
from pydantic_settings import BaseSettings

class Config(BaseSettings):
    JDBC_URL: str
    JDBC_USER: str
    JDBC_PASSWORD: str
    MYSQL_HOST: str
    MYSQL_PORT: int

    model_config = ConfigDict(
        extra="ignore", env_file=".venv", env_file_encoding="utf-8", case_sensitive=True
    )

config = Config()