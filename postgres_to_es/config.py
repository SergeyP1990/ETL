import logging
import typing
import toml
from pydantic import BaseModel

class PostgresConfig(BaseModel):
    dbname: str
    user: str
    password: str
    host: str
    port: int


class ElasticConfig(BaseModel):
    host: str
    port: int


class BackoffConfig(BaseModel):
    start_sleep_time: float
    factor: int
    border_sleep_time: float


class Config(BaseModel):
    pg_database: PostgresConfig
    elastic: ElasticConfig
    backoff: BackoffConfig

    @classmethod
    def parse_config(cls, file_path):
        conf = toml.load(file_path)
        return cls.parse_obj(conf)
