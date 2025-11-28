from pydantic_settings import BaseSettings


class Frontend(BaseSettings):
    url: str = "http://localhost:3000"


class Server(BaseSettings):
    name: str = "Open Science Archive"
    version: str = "0.0.1"  # TODO: better type?
    description: str = "An open platform for depositing scientific data"


class DatabaseConfig(BaseSettings):
    url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/osa"
    echo: bool = False


class Config(BaseSettings):
    server: Server = Server()
    frontend: Frontend = Frontend()
    database: DatabaseConfig = DatabaseConfig()

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"  # Allows OSA_DATABASE__URL override
