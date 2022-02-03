# Помимо этого мне хочется переделать классы сборщиков следующим образом:
# - Передавать аргументы для работы сборщиков в виде словарей, чтобы не городить монструозные
#   конструкции при инициализации этих классов;
# - Убрать передачу экземпляров сборщиков при инициализации. Т.к. классы наследуются,
#   то можно использовать родительские методы и родительские параметры.
#
#   В итоге интерфейс должен получится примерно такой:
# producer_settings_dict = {
#     "sql_query": "query",
#     "sql_params": {"name": "value"},
#     ....
# }
# enricher_settings_dict = {
#     "sql_query": "query",
#     "sql_params": {"name": "value"},
#    ....
# }
# И далее объявление, например, сборщика 2го уровня:
# enricher = Enricher(producer_settings = producer_settings_dict, enricher_settings = enricher_settings_dict)
#
# Как вы оцениваете данную идею?
# Спасибо!
#

# Без этого подключения почему-то не работает аннотация
# именем класса в методах, возвращающих self. В данном случае:
# def __enter__ в PostgresConnection
# Моя версия python 3.8.10
from __future__ import annotations

import dataclasses
import datetime
import logging
import os
import time
from typing import Optional, Iterator, List, Tuple, Any

import backoff
import psycopg2
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from elasticsearch import exceptions as elastic_exceptions
from psycopg2.extras import DictCursor
from psycopg2.sql import SQL

import sql_queries
from config import Config
from data_representation import FilmWork, BaseRecord, FilmWorkPersons, FilmWorkGenres, Person, Genre
from state_control import State, JsonFileStorage

# Считывание конфига происходит здесь, т.к.
# иначе не передать параметры в декоратор @backoff:
# conf должна быть глобальной для этого
conf = Config.parse_config("./config")


class PostgresConnection:
    """
    Класс для работы с Postgres.
    Реализует подключение, выполнение запросов. Может работать через контекстный менеджер
    Функция подключения обёрнута декоратором backoff
    """

    def __init__(self, connection_opts: dict) -> None:
        self.connection_opts = connection_opts
        self.connection = None
        self.cursor = None

    def __enter__(self) -> PostgresConnection:
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    @backoff.on_exception(
        backoff.expo,
        psycopg2.OperationalError,
        max_tries=5,
        max_time=conf.backoff.max_time,
    )
    def connect(self) -> None:
        self.connection = psycopg2.connect(
            **self.connection_opts, cursor_factory=DictCursor
        )
        self.cursor = self.connection.cursor()

    def close(self) -> None:
        self.connection.close()

    def execute(self, sql_query: SQL, params: Optional[dict] = None) -> None:
        self.cursor.execute(sql_query, params or ())

    def fetchall(self) -> List[dict]:
        return self.cursor.fetchall()

    def query(self, sql_query: SQL, params: Optional[dict] = None) -> List[dict]:
        while True:
            try:
                self.cursor.execute(sql_query, params or ())
                return self.fetchall()
            except psycopg2.OperationalError as err:
                logging.error(f"Error connecting to postgres while query: {err}")
                logging.error("Trying to reconnect")
                self.connect()


class Producer:
    """
    Класс сборщик первого уровня (Producer). Принимает на вход:
     - pg_connection: Объект соединения с Postgres;
     - sql_query: SQL объект, описывающий sql запрос с расставленными в нем именными
       placeholder'ами (описаны в sql_queries);
     - sql_values: Словарь, в котором названия placeholder'ов соответствуют значениям,
       подставляемые в sql запрос. Эти значения не меняются от запроса к запросу
       (например, название таблицы или лимит);
     - data_class: dataclass в который разворачиваются собранные после выполнения запроса
       результаты (описаны в data_representation);
     - offset_by: поле в таблице, по которому будет выполняется смещение. Значение из поля
       с именем offset_by будет сохраняться и меняться после каждого запроса, осуществляя
       таким образом чтение пачками;
     - produce_field: опциональный параметр, позволяющий влиять на результат, который
       возвращает producer. Если этот параметр указан, то вместо списка объектов dataclass
       будет возвращаться список значений одного поля этого dataclass. Например, если указать
       produce_field = 'id', то при data_class = FilmWork будет возвращаться список из id
       собранных FilmWork'ов

    """

    def __init__(
        self,
        pg_connection: PostgresConnection,
        sql_query: SQL,
        sql_values: dict,
        data_class: Optional[dataclasses] = BaseRecord,
        offset_by: str = None,
        produce_field: Optional[str] = None,
    ) -> None:
        self.pg_connection = pg_connection
        self.sql_query = sql_query
        self.sql_values = sql_values
        self.data_class = data_class
        self.offset_by = offset_by
        self.produce_field = produce_field
        self.last_upd_at = datetime.datetime.fromtimestamp(0)

    def extract(self) -> List[dataclasses]:
        """
        Метод, осуществляющий запрос к базе. При запросе передается sql запрос из
        атрибута класса self.sql_query и подстановочные именные значения для него из
        self.sql_values

        Возвращается список из dataclass'ов. Используется dataclass, переданный при
        инициализации класса
        """
        raw_data = self.pg_connection.query(self.sql_query, self.sql_values)
        dataclasses_data = [self.data_class(**row) for row in raw_data]
        return dataclasses_data

    def generator(self) -> Iterator[list]:
        """
        Метод, описывающий логику вычитки из базы.
        Создает генератор.

        Повторяем запросы к базе, пока она возвращает списки не нулевой длинны.
        Для итераций возвращается пачка объектов типа dataclass, указанного при инициализации

        Если при инициализации класса был указан produce_field, то возвращается список со
        значениями поля, указанного в produce_field

        После того как по результату проитерируются, будет произведено смещение значения offset,
        подставляемого в sql запрос
        """
        while True:
            result = self.extract()
            if len(result) == 0:
                break
            self.update_sql_value(self.offset_by, getattr(result[-1], self.offset_by))
            self.last_upd_at = self.sql_values["updated_at"]
            if self.produce_field is not None:
                produced_by_field = [
                    getattr(rows, self.produce_field) for rows in result
                ]
                yield produced_by_field
            else:
                yield result
            # Берём последний элемент списка объектов, забираем у него значение из offset_by,
            # и перезаписываем это значение в sql_value. Таким образом осуществляется сдвиг,
            # например, по updated_at

    def update_sql_value(self, key: str, value: any) -> None:
        """Метод для обновления значения по ключу в
        словаре, который подставляется в sql запрос"""
        self.sql_values[key] = value


class Enricher(Producer):
    """
    Класс сборщик второго уровня. Наследуется от класса сборщика первого уровня.
    Помимо параметров родительского класса, при инициализации на вход требует:
    - producer: Экземпляр сборщика первого уровня (Producer);
    - enrich_by: Имя placeholder'а в sql запросе, в который будет подставляться результат
      работы сборщика первого уровня (Producer);

    Нарезка на пачки данных в Enricher осуществляется иначе, чем в Producer.
    Здесь для запросов применяется OFFSET, placeholder под который обязательно должен быть в sql
    запросе, передаваемом Enricher'у.
    """

    def __init__(
        self, *args, producer: Producer, enrich_by: str = None, **kwargs
    ) -> None:
        self.producer = producer
        self.enrich_by = enrich_by
        self.offset = 0
        super().__init__(*args, **kwargs)

    def generator(self) -> Iterator[list]:
        if self.sql_values.get("offset") is None:
            raise ValueError("У enricher должен быть OFFSET")
        # Итерация по генератору из Producer.
        for pr in self.producer.generator():
            while True:
                # Здесь вносится изменение в словарь с подстановками в sql запрос:
                # имени placeholder'а теперь соответствует tuple со значениями,
                # по которым осуществится выборка (например, подставляется в WHERE IN).
                # Tuple используется, т.к. psycopg2 при подстановке в sql запрос корректно
                # преобразует его в перечисленные через запятую значения.
                self.update_sql_value(self.enrich_by, tuple(pr))
                result = self.extract()
                if len(result) == 0:
                    break
                # Увеличение OFFSET для следующего запроса.
                self.move_offset()
                if self.produce_field is not None:
                    enriched_by_field = [
                        getattr(rows, self.produce_field) for rows in result
                    ]
                    yield enriched_by_field
                else:
                    yield result
            # Сбор данных второго уровня по результатам пачки данных из сборщика первого уровня закончен.
            # Следовательно, необходимо сбросить offset, чтобы на следующей итерации сбор второго уровня
            # начинать сначала.
            self.update_sql_value("offset", 0)

    def move_offset(self) -> None:
        """
        Функция сдвига OFFSET'а. Берёт старое значение и прибавляет к нему значение лимита.
        Изменение заносятся в словарь, который используется для подстановки значений в sql
        запрос.
        """
        new_offset = self.sql_values["offset"] + self.sql_values["limit"]
        self.update_sql_value("offset", new_offset)
        self.offset = new_offset


class Merger(Producer):
    """
    Класс сборщика третьего уровня. Подготавливает финальные данные.

    Наследуется от класса сборщика первого уровня и при инициализации помимо родительских атрибутов требует:
     - enricher: экземпляр класса сборщка второго уровня;
     - produce_by: имя placeholder'а, на место которого в sql запросе будут подставляться данные,
       собранные сборщиком второго уровня;
     - Параметр set_limit ограничивает размер множества, по которому будут
       собраны финальные данные (см. ниже);

    Сборщик третьего уровня не выполняет запрос финальных данных сразу же после получения необходимой
    информации от сборщика второго уровня, т.к. при m2m связях данных может быть слишком мало, что
    увеличивает частоту запросов к базе данных. Вместо этого сборщик копит набор уникальных данных,
    полученных от Enricher. Это осуществляется через объединение данных в set(). Когда размер set'а
    становится равен или больше лимита set_limit, сборщик выполняет запрос к базе данных по собранным
    данным и возвращает список результатов.
    """

    def __init__(
        self,
        pg_connection: PostgresConnection,
        enricher: Enricher,
        sql_query: SQL,
        sql_values: dict,
        produce_by: Optional[str] = None,
        set_limit: int = 100,
        **kwargs,
    ) -> None:
        self.enricher = enricher
        self.produce_by = produce_by
        self.set_limit = set_limit
        self.unique_produce_by = set()
        super().__init__(pg_connection, sql_query, sql_values, **kwargs)

    def _get_result_(self) -> List[dataclasses]:
        self.update_sql_value(self.produce_by, tuple(self.unique_produce_by))
        result = self.extract()
        return result

    def generator(self) -> Iterator[list]:
        # Итерация по сборщику второго уровня
        for en in self.enricher.generator():
            # Объединение данных в множество, проверка размера множества
            # Если собрано меньше лимита, то начинаем следующую итерацию
            # Если лимит превышен - подставляем данные на место placholder'а
            self.unique_produce_by = self.unique_produce_by.union(set(en))
            if len(self.unique_produce_by) <= self.set_limit:
                continue
            yield self._get_result_()
            # Очистка множества
            self.unique_produce_by.clear()

        # Если предел множества еще не достигнут, а данные от сборщика второго уровня уже закончились,
        # то часть данных останется не выданной. Поэтому после выхода из генератора Enricher
        # довыдаём остаток
        if len(self.unique_produce_by) != 0:
            yield self._get_result_()
            self.unique_produce_by.clear()


class ElasticRequester:
    """
    Класс работы с Elasticsearch.
    Хранит в себе инстанс с настройками подключения в ES и запросы

    Имеет метод для преобразования списка dataclass в bulk запрос.
    Каждый dataclass должен иметь метод elastic_format, возвращающий
    словарь с именами полей, соотвествующими mapping'у индекса
    """

    def __init__(self, ip: List[str], port: int) -> None:
        self.ip = ip
        self.port = port
        self.elastic_instance = Elasticsearch(self.ip, port=self.port)
        self.bulk_request = []

    def prepare_bulk(
        self,
        objects: List[dataclasses],
        action: str,
        id_key: Optional[str] = "id",
        upsert: Optional[bool] = False,
    ) -> None:
        self.bulk_request.clear()
        for obj in objects:
            elastic_doc = obj.elastic_format()
            doc_id = getattr(obj, id_key)
            req = {"_op_type": action, "_id": doc_id, "doc": elastic_doc}
            if upsert:
                req["doc_as_upsert"] = True
            self.bulk_request.append(req)

    @backoff.on_exception(
        backoff.expo, elastic_exceptions.ConnectionError, max_time=conf.backoff.max_time
    )
    def make_bulk_request(self, to_index: str) -> Tuple[int, int | List[Any]]:
        if len(self.bulk_request) == 0:
            logging.error("Bulk request empty")
            return 0, 0
        res = helpers.bulk(self.elastic_instance, self.bulk_request, index=to_index)
        return res


def fw_producer(
    pg_connection: PostgresConnection,
    elastic_requester: ElasticRequester,
    state: State,
    limit: int,
):
    """
    Выгрузка таблицы film_work
    """
    logging.info("Запуск выгрузки film_work")
    # Считывание updated_at из state файла
    updated_at = state.get_state("film_work_upd_at")

    film_work_producer = Producer(
        pg_connection,
        sql_query=sql_queries.fw_full_sql_query(),
        sql_values={"updated_at": updated_at, "sql_limit": limit},
        data_class=FilmWork,
        offset_by="updated_at",
    )

    # Итерация по данным из базы
    # Загрузчик film_work_producer возвращает для работы
    # список dataclass'ов
    for film_work_objects in film_work_producer.generator():

        # Подготовка bulk запроса. Список с dataclass'ами передаётся в
        # ElasticRequester для форматирования
        elastic_requester.prepare_bulk(
            film_work_objects, "update", "fw_id", upsert=True
        )
        res = elastic_requester.make_bulk_request(to_index="movies")
        # запись в state_file последнего успешно записанного значения updated_at
        state.set_state("film_work_upd_at", film_work_producer.last_upd_at)

    logging.info("Выгрузка film_work завершена")


def persons_or_genres_producer(
    pg_connection: PostgresConnection,
    elastic_requester: ElasticRequester,
    state: State,
    limit: int,
    data_type: str,
):
    """
    Выгрузка таблицы person\genre
    """
    dispatcher = {
        "person": {
            "state_field": "person_upd_at",
            "table_name": "person",
            "related_table": "person_film_work",
            "related_id": "person_id",
            "dataclass": FilmWorkPersons,
            "sql_query": sql_queries.fw_persons_sql_query(),
        },
        "genre": {
            "state_field": "genre_upd_at",
            "table_name": "genre",
            "related_table": "genre_film_work",
            "related_id": "genre_id",
            "dataclass": FilmWorkGenres,
            "sql_query": sql_queries.fw_genres_sql_query(),
        },
    }
    if dispatcher.get(data_type) is None:
        raise ValueError(
            f"Неизвестный тип данных в запросе! Может быть только: {dispatcher.keys()}"
        )

    logging.info(f"Запуск выгрузки {data_type}")
    updated_at = state.get_state(dispatcher[data_type]["state_field"])

    # Здесь создаются загрузчики трех уровней как в архитектуре ETL: producer, enricher, merger
    # для каждого загрузчика - свой sql запрос
    pg_producer = Producer(
        pg_connection,
        sql_query=sql_queries.nested_pre_sql(dispatcher[data_type]["table_name"]),
        sql_values={"updated_at": updated_at, "limit": limit},
        offset_by="updated_at",
        produce_field="id",
    )
    pg_enricher = Enricher(
        pg_connection,
        producer=pg_producer,
        sql_query=sql_queries.nested_fw_ids_sql(
            dispatcher[data_type]["related_table"], dispatcher[data_type]["related_id"]
        ),
        sql_values={"offset": 0, "limit": limit},
        enrich_by="data_ids",
        produce_field="id",
    )
    pg_merger = Merger(
        pg_connection,
        pg_enricher,
        sql_query=dispatcher[data_type]["sql_query"],
        sql_values={},
        produce_by="filmwork_ids",
        set_limit=100,
        data_class=dispatcher[data_type]["dataclass"],
    )

    # В результате работы этого загрузчика, в ES отправляются только персоны\жанры, остальные данные
    # по фильму не загружаются. Если фильма не было на момент создания, то он будет создан по id, благодаря upsert,
    # но вся остальная информация в него попадёт только на момент работы функции fw_producer (которая была выше)
    for pgfw_objects in pg_merger.generator():

        elastic_requester.prepare_bulk(pgfw_objects, "update", "fw_id", upsert=True)
        res = elastic_requester.make_bulk_request(to_index="movies")
        state.set_state(dispatcher[data_type]["state_field"], pg_producer.last_upd_at)

    logging.info(f"Выгрузка {data_type} завершена")

def persons(
    pg_connection: PostgresConnection,
    elastic_requester: ElasticRequester,
    state: State,
    limit: int,
):
    """
    Выгрузка таблицы persons
    """
    logging.info("Запуск выгрузки full_persons")
    # Считывание updated_at из state файла
    updated_at = state.get_state("persons_full_upd_at")

    full_persons_producer = Producer(
        pg_connection,
        sql_query=sql_queries.person_sql(),
        sql_values={"updated_at": updated_at, "limit": limit},
        data_class=Person,
        offset_by="updated_at",
    )

    for film_work_objects in full_persons_producer.generator():

        elastic_requester.prepare_bulk(
            film_work_objects, "update", "id", upsert=True
        )
        res = elastic_requester.make_bulk_request(to_index="person")
        # запись в state_file последнего успешно записанного значения updated_at
        state.set_state("film_work_upd_at", full_persons_producer.last_upd_at)

    logging.info("Выгрузка full_persons завершена")


def genres(
    pg_connection: PostgresConnection,
    elastic_requester: ElasticRequester,
    state: State,
    limit: int,
):
    """
    Выгрузка таблицы genres
    """
    logging.info("Запуск выгрузки full_genres")
    # Считывание updated_at из state файла
    updated_at = state.get_state("genres_full_upd_at")

    full_genres_producer = Producer(
        pg_connection,
        sql_query=sql_queries.genre_sql(),
        sql_values={"updated_at": updated_at, "limit": limit},
        data_class=Genre,
        offset_by="updated_at",
    )

    for film_work_objects in full_genres_producer.generator():

        elastic_requester.prepare_bulk(
            film_work_objects, "update", "id", upsert=True
        )
        res = elastic_requester.make_bulk_request(to_index="genre")
        # запись в state_file последнего успешно записанного значения updated_at
        state.set_state("film_work_upd_at", full_genres_producer.last_upd_at)

    logging.info("Выгрузка full_genres завершена")



if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    logging.info("Начало работы")

    while True:
        load_dotenv()
        pg_dsl = conf.pg_database.dict()
        pg_dsl["password"] = os.environ.get("DB_PASSWD")
        pg_dsl["user"] = os.environ.get("DB_USER")

        with PostgresConnection(pg_dsl) as pg_conn:
            # with PostgresConnection(conf.pg_database.dict()) as pg_conn:
            # Предполагается, что на момент старта скрипта необходимые index'ы уже созданы
            esr = ElasticRequester([conf.elastic.host], port=conf.elastic.port)

            # Считывание state файла. При инициализации класса State
            # отсутствующие необходимые параметры будут заполнены
            # значениями по умолчанию. Подробнее см state_control.py
            js_storage = JsonFileStorage(file_path="./state_file")
            st = State(js_storage)

            # Запуск сборщиков
            fw_producer(pg_conn, esr, st, conf.sql_settings.limit)
            persons_or_genres_producer(
                pg_conn, esr, st, conf.sql_settings.limit, "person"
            )
            persons_or_genres_producer(
                pg_conn, esr, st, conf.sql_settings.limit, "genre"
            )
            persons(pg_conn, esr, st, conf.sql_settings.limit)
            genres(pg_conn, esr, st, conf.sql_settings.limit)

        logging.info(f"Синхронизация закончена. Переход в ждущий режим.")
        logging.info(f"Следующая синхронизация через {conf.etl.time_interval} секунд.")
        time.sleep(conf.etl.time_interval)
