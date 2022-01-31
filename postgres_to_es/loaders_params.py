import dataclasses

from pydantic import BaseModel, Field
from typing import Optional, Any

# from etl import PostgresConnection
from psycopg2.sql import SQL


class LoaderSettings(BaseModel):
    sql_query: Any
    sql_values: dict
    data_class: object
    offset_by: Optional[str] = Field(default=None)
    produce_field: Optional[str] = Field(default=None)

class EnricherSettings(BaseModel):
    producer_settings: LoaderSettings
    enrich_by: str

