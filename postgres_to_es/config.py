import toml
from pydantic import BaseModel

class PostgresConfig(BaseModel):
    dbname: str
    user: str
    password: str
    host: str
    port: int

class SqlConfig(BaseModel):
    limit: int

class ElasticConfig(BaseModel):
    host: str
    port: int


class BackoffConfig(BaseModel):
    max_time: int


class Config(BaseModel):
    pg_database: PostgresConfig
    elastic: ElasticConfig
    backoff: BackoffConfig
    sql_settings: SqlConfig

    @classmethod
    def parse_config(cls, file_path):
        conf = toml.load(file_path)
        return cls.parse_obj(conf)
