from pydantic_settings import BaseSettings


class Frontend(BaseSettings):
    url: str = "http://localhost:3000"


class Server(BaseSettings):
    name: str = "Open Science Arvchive"
    version: str = "0.0.1"  # TODO: better type?
    description: str = "An open platform for depositing scientific data"


class Config(BaseSettings):
    server: Server = Server()
    frontend: Frontend = Frontend()

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
