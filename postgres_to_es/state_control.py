import abc
import datetime
import json
import os
from typing import Any, Optional


class EnhancedJSONEncoder(json.JSONEncoder):
    """
    Кодировщик для dataclass
    """

    def default(self, obj: Any) -> str:
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None) -> None:
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, "w") as outfile:
            json.dump(state, outfile, cls=EnhancedJSONEncoder)

    def retrieve_state(self) -> dict:
        if not os.path.isfile(self.file_path):
            return {}
        with open(self.file_path) as json_file:
            try:
                data = json.load(json_file)
            except json.decoder.JSONDecodeError:
                return {}
        return data


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    """

    def __init__(self, storage: JsonFileStorage) -> None:
        self.storage = storage
        self.data = {}

        zero_time = datetime.datetime.fromisoformat("1970-01-01T00:00:00.000000+00:00")

        self.default_values = {
            "film_work_upd_at": zero_time,
            "person_upd_at": zero_time,
            "genre_upd_at": zero_time,
            "genres_full_upd_at": zero_time,
            "persons_full_upd_at": zero_time,
        }

        self.parse_data()

    def parse_data(self) -> None:
        """
        Функция проверки значений state файла. Валидация происходит на основе
        словаря со значениями по умолчанию. Если по какой-то причине необходимых
        значений нет, то они создаются со значениями по умолчанию.
        """
        temp_dict = self.storage.retrieve_state()

        for key, value in self.default_values.items():
            if temp_dict.get(key) is None:
                self.data[key] = value
            else:
                self.data[key] = temp_dict[key]

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.data[key] = value
        self.storage.save_state(self.data)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.data.get(key)
