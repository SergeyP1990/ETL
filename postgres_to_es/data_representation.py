import datetime
import uuid
from dataclasses import dataclass, field
from typing import List


@dataclass
class BaseRecord:
    id: uuid.UUID = field(default=None)
    updated_at: datetime.datetime = field(default=None)


@dataclass
class FilmWorkGenres:
    fw_id: uuid.UUID = field(default=None)
    genres: List = field(default_factory=list)


@dataclass
class FilmWorkPersons:
    fw_id: uuid.UUID = field(default=None)
    director: list = field(default_factory=list)
    actors_names: List = field(default_factory=list)
    writers_names: List = field(default_factory=list)
    actors: List = field(default_factory=list)
    writers: List = field(default_factory=list)


@dataclass
class FilmWork:
    fw_id: uuid.UUID = field(default=None)
    imdb_rating: float = field(default=None)
    title: str = field(default=None)
    description: str = field(default=None)
    genre: List = field(default_factory=list)
    director: List = field(default_factory=list)
    actors_names: List = field(default_factory=list)
    writers_names: List = field(default_factory=list)
    actors: List = field(default_factory=list)
    writers: List = field(default_factory=list)
    updated_at: datetime.datetime = field(default=None)
