import dataclasses

from pydantic import BaseModel, Field
from typing import Optional

# from etl import PostgresConnection
from psycopg2.sql import SQL


class LoaderSettings(BaseModel):
    sql_query: SQL
    sql_values: dict
    data_class: dataclasses.dataclass
    offset_by: Optional[str] = Field(default=None)
    produce_field: Optional[str] = Field(default=None)
