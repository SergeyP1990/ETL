import abc
import json
import os
from typing import Any, Optional


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
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as outfile:
            json.dump(state, outfile)

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
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: JsonFileStorage):
        self.storage = storage
        self.data = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.data[key] = value
        self.storage.save_state(self.data)
        pass

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.data.get(key)
        pass
