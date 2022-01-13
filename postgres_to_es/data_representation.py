import datetime
import uuid
from dataclasses import dataclass, field, InitVar

@dataclass
class NotNullChecker:
    def _check_not_null_(self, not_null_fields):
        for not_null_field in not_null_fields:
            if self.__dict__[not_null_field] is None:
                raise ValueError(f"Field '{not_null_field}' violates not null constraint")


@dataclass
class BaseRecord:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime.datetime = field(default=None)
    updated_at: datetime.datetime = field(default=None)


@dataclass
class FilmWork(BaseRecord, NotNullChecker):
    title: str = field(default=None)
    type: str = field(default=None)
    description: str = field(default=None)
    creation_date: datetime.date = field(default=None)
    certificate: str = field(default=None)
    file_path: str = field(default=None)
    rating: float = field(default=None)
    table_name: InitVar[str] = "film_work"

    def __post_init__(self, table_name: str):
        self._check_not_null_(("title", "type"))


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
class GenreFilmWork(NotNullChecker):
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime.datetime = field(default=None)
    table_name: InitVar[str] = "genre_film_work"

    def __post_init__(self, table_name: str):
        self._check_not_null_(("film_work_id", "genre_id"))

@dataclass
class PersonFilmWork(NotNullChecker):
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime.datetime = field(default=None)
    table_name: InitVar[str] = "person_film_work"

    def __post_init__(self, table_name: str):
        self._check_not_null_(("film_work_id", "person_id", "role"))


