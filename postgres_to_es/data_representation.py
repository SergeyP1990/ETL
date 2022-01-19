import datetime
import uuid
from dataclasses import dataclass, field, InitVar
from typing import List

@dataclass
class NotNullChecker:
    def _check_not_null_(self, not_null_fields):
        for not_null_field in not_null_fields:
            if self.__dict__[not_null_field] is None:
                raise ValueError(f"Field '{not_null_field}' violates not null constraint")


@dataclass
class BaseRecord:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    # created_at: datetime.datetime = field(default=None)
    updated_at: datetime.datetime = field(default=None)


@dataclass
class Genre(BaseRecord, NotNullChecker):
    name: str = field(default=None)
    description: str = field(default=None)
    table_name: InitVar[str] = "genre"

    def __post_init__(self, table_name: str):
        self._check_not_null_(("name",))


@dataclass
class Person(BaseRecord, NotNullChecker):
    full_name: str = field(default=None)
    birth_date: datetime.date = field(default=None)
    updated_at: datetime.datetime = field(default=None)
    table_name: InitVar[str] = "person"

    def __post_init__(self, table_name: str):
        self._check_not_null_(("full_name",))


@dataclass
class FilmWork(BaseRecord, NotNullChecker):
    fw_id: uuid.UUID = field(default_factory=uuid.uuid4)
    imdb_rating: float = field(default=None)
    title: str = field(default=None)
    description: str = field(default=None)
    genres: List[Genre] = field(default_factory=list)
    director: list = field(default_factory=list)
    actors_names: List = field(default_factory=list)
    writers_names: List = field(default_factory=list)
    actors: List = field(default_factory=list)
    writers: List = field(default_factory=list)
    table_name: InitVar[str] = "film_work"