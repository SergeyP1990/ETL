import dataclasses
import datetime
import logging
import time
from dataclasses import fields
from functools import wraps

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

import sql_queries
from config import Config
from data_representation import FilmWork, BaseRecord, FilmWorkPersons


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            while True:
                # logging.error("before call func: {}".format(func.__name__))
                result = func(*args, **kwargs)
                # logging.error("after call func")
                if result is not False:
                    return result
                logging.error("retry")
                time.sleep(2)
        return inner
    return func_wrapper


class PostgresConnection:
    """
    Класс для работы с Postgres.
    Реализует подключение, выполнение запросов. Может работать через контекстный менеджер
    """
    def __init__(self, connection_opts: dict):
        self.connection_opts = connection_opts
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.error("__exit__ called")
        self.close()

    @backoff()
    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.connection_opts, cursor_factory=DictCursor)
            self.cursor = self.connection.cursor()
            return True
        except psycopg2.Error as err:
            logging.error(f"Error connecting to postgres: {err}")
            return False

    def close(self):
        self.connection.close()

    def execute(self, sql_query, params=None):
        self.cursor.execute(sql_query, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql_query, params=None):
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
    def __init__(self, pg_connection: PostgresConnection, sql_query, sql_values: dict,
                 data_class: dataclasses.dataclass = BaseRecord, offset_by = None, produce_field = None):
        self.pg_connection = pg_connection
        self.sql_query = sql_query
        self.sql_values = sql_values
        self.data_class = data_class
        self.offset_by = offset_by
        self.produce_field = produce_field

    def extract(self) -> list:
        """
        Метод, осуществляющий запрос к базе. При запросе передается sql запрос из
        атрибута класса self.sql_query и подстановочные именные значения для него из
        self.sql_values

        Возвращается список из dataclass'ов. Используется dataclass, переданный при
        инициализации класса
        """
        raw_data = self.pg_connection.query(self.sql_query, self.sql_values)
        # for row in raw_data:
        #     for key, value in row.items():
        #
        dataclasses_data = [ self.data_class(**row) for row in raw_data ]
        return dataclasses_data

    def generator(self):
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
            if self.produce_field is not None:
                produced_by_field = [getattr(rows, self.produce_field) for rows in result]
                yield produced_by_field
            else:
                yield result
            # Берём последний элемент списка объектов, забираем у него значение из offset_by,
            # и перезаписываем это значение в sql_value. Таким образом осуществляется сдвиг,
            # например, по updated_at
            self.update_sql_value(self.offset_by, getattr(result[-1], self.offset_by))


    def update_sql_value(self, key: str, value: any):
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
    Здесь для запросов применяется OFFSET, placehol'er под который обязательно должен быть в sql
    запросе, передаваемом Enricher'у.
    """
    def __init__(self, *args, producer: Producer, enrich_by: str = None, **kwargs):
        self.producer = producer
        self.enrich_by = enrich_by
        super().__init__(*args, **kwargs)

    def generator(self):
        if self.sql_values.get('offset') is None:
            raise ValueError("У enricher должен быть OFFSET")
        # Итерация по генератору из Producer
        for pr in self.producer.generator():
            while True:
                # Здесь вносится изменение в словарь с подстановками в sql запрос:
                # имени placeholder'а теперь соответствует tuple со значениями,
                # по которым осуществится выборка (например, подставляется в WHERE IN).
                # Tuple используется, т.к. psycopg2 при подстановке в sql запрос корректно
                # преобразует его в перчисленные через запятую значения.
                self.update_sql_value(self.enrich_by, tuple(pr))
                result = self.extract()
                if len(result) == 0:
                    break
                if self.produce_field is not None:
                    enriched_by_field = [getattr(rows, self.produce_field) for rows in result]
                    yield enriched_by_field
                else:
                    yield result
                # Увеличение OFFSET для следующего запроса.
                self.move_offset()
            # Сбор данных второго уровня по результатам пачки данных из сборщика первого уровня закончен.
            # Следовательно, неободимо сбросить offset, чтобы на следующей итерации сбор второго уровня
            # начинать сначала.
            self.update_sql_value('offset', 0)

    def move_offset(self):
        """
        Функция сдвига OFFSET'а. Берёт старое значение и прибавляет к нему значение лимита.
        Изменение заносятся в словарь, который используется для подстановки значений в sql
        запрос.
        """
        new_offset = self.sql_values['offset'] + self.sql_values['limit']
        self.update_sql_value('offset', new_offset)


class Merger(Producer):
    """
    Класс сборщика третьего уровня. Подготавливает финальные данные.

    Наследуется от класса сборщика первого уровня и при инициализации помимо родительских атрибутов треубет:
     - enricher: экземпляр класса сборщка второго уровня;
     - produce_by: имя placeholder'а, на место которого в sql запросе будут подставляться данные,
       собранные сборщиком второго уровня;
     - Параметр set_limit, ограничивающий размер множества, по которому будут
       собраны финальные данные (см. ниже);

    Сборщик третьего уровня не выполняет запрос финальных данных сразу же после получения необходимой
    информации от сборщика второго уровня, т.к. при m2m связях данных может быть слишком мало, что
    увеличивает частоту запросов к базе данных. Вместо этого сборщик копит набор уникальных данных,
    полученных от Enricher. Это осуществляется через объединение данных в set(). Когда размер set'а
    становится равен или больше лимита set_limit, сборщик выполняет запрос к базе данных по собранным
    данным и возвращает список результатов.
    """
    def __init__(self, pg_connection: PostgresConnection, enricher: Enricher, sql_query, sql_values: dict,
                 produce_by: str = None, set_limit = 100, **kwargs):
        self.enricher = enricher
        self.produce_by = produce_by
        self.set_limit = set_limit
        self.unique_produce_by = set()
        super().__init__(pg_connection, sql_query, sql_values, **kwargs)

    def _get_result_(self):
        self.update_sql_value(self.produce_by, tuple(self.unique_produce_by))
        result = self.extract()
        return result

    def generator(self):
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

        # Если предел множества еще не достигнут, а данные от сборщика второго уровня уже зкончились,
        # то часть данных останется не выданной. Поэтому после выхода из генератора Enricher
        # довыдаём остаток
        if len(self.unique_produce_by) != 0:
            yield self._get_result_()
            self.unique_produce_by.clear()




if __name__ == "__main__":
    conf = Config.parse_config("./config")
    logging.getLogger("app1")
    # logging.basicConfig(level='DEBUG')
    # logging.error("HI")
    # print("Connected")
    # time.sleep(5)
    #
    # print("first")
    # result = es.get(index="table2", id='6cMeOn4BK1Sd5DmWWY3Q')
    # print(result)
    #
    # result = es.get(index="table2", id='6cMeOn4BK1Sd5DmWWY3Q1')
    # print(result)

    # retrieved_document = result

    # print(es)
    # exit(0)

    with PostgresConnection(conf.pg_database.dict()) as pg_conn:
        dt = datetime.datetime.fromtimestamp(0)

        # dt = datetime.datetime.strptime("2020-06-16 20:14:09.222016 +00:00", "%Y-%m-%d %H:%M:%S.%f %z")
        # dt2 = datetime.datetime.strptime("2021-06-16 20:14:09.222016 +00:00", "%Y-%m-%d %H:%M:%S.%f %z")
        # a = PostgresDataExtractor(pg_conn, limit=10)
        def fw_producer():
            es = Elasticsearch(['127.0.0.1'], port=9200)
        # ---- fw producer ----
            a = Producer(pg_conn, sql_query=sql_queries.fw_full_sql_query(),
                         sql_values={'updated_at': dt, 'sql_limit': 10}, data_class=FilmWork,
                         offset_by='updated_at')

            for i in a.generator():

                d = dict(('id' if field.name == "fw_id" else field.name, getattr(i[0], field.name)) for field in fields(i[0]))
                print(d)
                res = es.index(index="movies", id=d['id'], document=d)
                print(res)
                exit(0)
                arr = []
                for item in i:
                    arr.append(dict(('id' if field.name == "fw_id" else field.name, getattr(i[0], field.name)) for field in fields(i[0])))
                print(arr)
                # print(dataclasses.asdict(i[0]))
                return
                print(i[-1].title)

        def merger_call():
            # ---- producer then enricher (person) ----
            offset = 0
            # a = Producer(pg_conn, sql_query=sql_queries.nested_pre_sql('person'),
            #               sql_values={'updated_at': dt, 'limit': 10},
            #               offset_by='updated_at', produce_field='id')
            # b = Enricher(pg_conn, producer=a, sql_query=sql_queries.nested_fw_ids_sql('person_film_work', 'person_id'),
            #              sql_values={'offset': offset, 'limit': 10},
            #              enrich_by='data_ids', produce_field='id')
            # c = Merger(pg_conn, b, sql_query=sql_queries.fw_persons_sql_query(), sql_values={},
            #            produce_by='filmwork_ids', set_limit=50, data_class=FilmWorkPersons)

            a = Producer(pg_conn, sql_query=sql_queries.nested_pre_sql('person'),
                          sql_values={'updated_at': dt, 'limit': 100},
                          offset_by='updated_at', produce_field='id')
            b = Enricher(pg_conn, producer=a, sql_query=sql_queries.nested_fw_ids_sql('person_film_work', 'person_id'),
                         sql_values={'offset': offset, 'limit': 100},
                         enrich_by='data_ids', produce_field='id')
            c = Merger(pg_conn, b, sql_query=sql_queries.fw_persons_sql_query(), sql_values={},
                       produce_by='filmwork_ids', set_limit=100, data_class=FilmWorkPersons)


            s = set()
            for fw_objects in c.generator():
                print(dataclasses.asdict(fw_objects[1]))
                return
                ps = {el.fw_id for el in fw_objects}
                # print(len(fw_objects))
                s = s.union(ps)
                # print({el.id for el in fw_objects})
                print(len(fw_objects))
                print(a.sql_values['updated_at'])
                # print(fw_objects)


                # print(s)
                # new_offset = b.sql_values['offset'] + b.sql_values['limit']
                # b.update_sql_value('offset', new_offset)
                # b.update_sql_value('updated_at_rfw', fw_objects[-1].updated_at)
            print(f"FINAL LEN: {len(s)}")


        def enricher_call():
            # ---- producer then enricher (person) ----
            offset = 0
            a = Producer(pg_conn, sql_query=sql_queries.nested_pre_sql('person'),
                          sql_values={'updated_at': dt, 'limit': 10},
                          offset_by='updated_at', produce_field='id')
            b = Enricher(pg_conn, producer=a, sql_query=sql_queries.nested_fw_ids_sql('person_film_work', 'person_id'),
                         sql_values={'offset': offset, 'limit': 10},
                         enrich_by='data_ids', produce_field='id')
            # ---- producer then enricher (genre) ----
            # offset = 0
            # a = Producer(pg_conn, sql_query=sql_queries.nested_pre_sql('genre'),
            #               sql_values={'updated_at': dt, 'limit': 100})
            # b = Enricher(pg_conn, a, sql_query=sql_queries.nested_fw_ids_sql('genre_film_work', 'genre_id'),
            #              sql_values={'offset': offset, 'limit': 100},
            #              params={'produce_to_enrich': 'id', '':''})

            s = set()
            for fw_objects in b.generator():
            #     print("----")
            #     print(type(fw_objects))
            #     ps = {el.id for el in fw_objects}
                print(len(fw_objects))
                s = s.union(fw_objects)
                # print({el.id for el in fw_objects})
                print(fw_objects)
                # print(s)
                # new_offset = b.sql_values['offset'] + b.sql_values['limit']
                # b.update_sql_value('offset', new_offset)
                # b.update_sql_value('updated_at_rfw', fw_objects[-1].updated_at)
            print(len(s))

        # merger_call()
        fw_producer()
        # enricher_call()


        # fws = a.extract_filmorks(dt)
        # # print(len(fws))
        # c = 0
        # for i in a.generator():
            # print(i.id)
            # print(i[-1].title)
            # a.update_sql_value('updated_at', i[-1].updated_at)

            # exit(0)
            # c = c + len(i)
            # print(c)
        # print(a.last_updated_at)
        #     print([t.writers for t in i])
            # exit(0)
        # while True:
        #     ans = a.extract_filmorks(dt)
        #     if len(ans) == 0:
        #         break
        #     for i in ans:
        #         print(i['title'])
        #         dt = i['updated_at']
        #     print("------------")
        #
        # exit(0)

        # b = NestedExtractor(10, a)
        # ans = a.nested_ids_extract('person',dt)
        # print(ans)
        # set_of_ids = set(data['id'] for data in ans)
        # ans2 = a.nested_ids_extract(set_of_ids, dt)
        # print(ans2)
        # set_of_ids = set(data['id'] for data in ans2)
        # set_of_ids.pop()
        # ans3 = a.extract_fw_helper("person", set_of_ids)
        # # print(ans3[0]['fw_id'])
        # print(ans3[0]['fw_id'])
        # print(ans3[0]['director'])
        # print(ans3[0]['actors'])
        # print(ans3[0]['writers'])

        # print(ans3[1]['fw_id'])
        # print(ans3[1]['director'])
        # print(ans3[1]['actors'])
        # print(ans3[1]['writers'])
        # dt = datetime.datetime.fromtimestamp(0)


            # exit()

        # print(pg_conn.query(l))

        # time.sleep(5)
        # print("new query")
        # print("QERY2 RES:" + str(pg_conn.query("SELECT id, rating, title, description, updated_at FROM content.film_work ORDER BY updated_at LIMIT 2")))
        # print(pg_conn.connection)
        # print(f"CONN: {pg_conn.connection}")
        # print(f"CONN: {pg_conn.cursor}")

