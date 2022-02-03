import datetime
import uuid
from dataclasses import dataclass, field, fields
from typing import List


@dataclass
class BaseRecord:
    id: uuid.UUID = field(default=None)
    updated_at: datetime.datetime = field(default=None)


@dataclass
class FilmWorkGenres:
    fw_id: uuid.UUID = field(default=None)
    genres: List = field(default_factory=list)

    def elastic_format(self) -> dict:
        di = {}
        for model_field in fields(self):
            f_name = model_field.name
            if f_name == "fw_id":
                continue
            if f_name == "genres":
                f_name = "genre"
            di[f_name] = getattr(self, model_field.name)
        return di


@dataclass
class FilmWorkPersons:
    fw_id: uuid.UUID = field(default=None)
    director: List = field(default_factory=list)
    actors_names: List = field(default_factory=list)
    writers_names: List = field(default_factory=list)
    actors: List = field(default_factory=list)
    writers: List = field(default_factory=list)

    def elastic_format(self) -> dict:
        di = {}
        for model_field in fields(self):
            f_name = model_field.name
            if f_name == "fw_id":
                continue
            di[f_name] = getattr(self, model_field.name)
        return di


@dataclass
class FilmWork:
    fw_id: uuid.UUID = field(default=None)
    imdb_rating: float = field(default=None)
    title: str = field(default=None)
    description: str = field(default=None)
    genres: List = field(default_factory=list)
    director: List = field(default_factory=list)
    actors_names: List = field(default_factory=list)
    writers_names: List = field(default_factory=list)
    actors: List = field(default_factory=list)
    writers: List = field(default_factory=list)
    directors: List = field(default_factory=list)
    updated_at: datetime.datetime = field(default=None)

    def elastic_format(self) -> dict:
        di = {}
        for model_field in fields(self):
            f_name = model_field.name
            if f_name == "genres":
                f_name = "genre"
            if f_name == "fw_id":
                f_name = "id"
            if f_name == "updated_at":
                continue
            di[f_name] = getattr(self, model_field.name)
        return di


@dataclass
class Person:
    id: uuid.UUID = field(default=None)
    full_name: str = field(default=None)
    role: List = field(default_factory=list)
    film_ids: List = field(default_factory=list)
    updated_at: datetime.datetime = field(default=None)

    def elastic_format(self) -> dict:
        di = {}
        for model_field in fields(self):
            f_name = model_field.name
            if f_name == "updated_at":
                continue
            di[f_name] = getattr(self, model_field.name)
        return di

@dataclass
class Genre:
    id: uuid.UUID = field(default=None)
    name: str = field(default=None)
    updated_at: datetime.datetime = field(default=None)

    def elastic_format(self) -> dict:
        di = {}
        for model_field in fields(self):
            f_name = model_field.name
            if f_name == "updated_at":
                continue
            di[f_name] = getattr(self, model_field.name)
        return di