# Без этого подключения почему-то не работает аннотация
# именем класса в методах, возвращающих self.
# В данном случае - def parse_config в Config
# Моя версия python 3.8.10
from __future__ import annotations
import toml
from pydantic import BaseModel


class PostgresConfig(BaseModel):
    dbname: str
    host: str
    port: int


class SqlConfig(BaseModel):
    limit: int


class ElasticConfig(BaseModel):
    host: str
    port: int


class BackoffConfig(BaseModel):
    max_time: int


class EtlConfig(BaseModel):
    time_interval: int


class Config(BaseModel):
    pg_database: PostgresConfig
    elastic: ElasticConfig
    backoff: BackoffConfig
    sql_settings: SqlConfig
    etl: EtlConfig

    @classmethod
    def parse_config(cls, file_path: str) -> Config:
        conf = toml.load(file_path)
        return cls.parse_obj(conf)
