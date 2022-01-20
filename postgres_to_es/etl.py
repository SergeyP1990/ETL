import dataclasses
import datetime
from data_representation import FilmWork, BaseRecord, FilmWorkPersons
from config import Config
from contextlib import contextmanager
import psycopg2
import logging
from functools import wraps
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
import time
import sql_queries
from elasticsearch import Elasticsearch

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

    @backoff()
    def query(self, sql_query, params=None):
        try:
            # from psycopg2 import sql
            # print(sql_query.as_string(self.cursor))
            # exit(0)
            # logging.error(params)

            self.cursor.execute(sql_query, params or ())
            # print(self.cursor.query)

            return self.fetchall()
        except psycopg2.OperationalError as err:
            logging.error(f"Error connecting to postgres while query: {err}")
            logging.error("Trying to reconnect")
            self.connect()
            return False

class PostgresDataExtractor:
    def __init__(self, pg_connection: PostgresConnection, limit=10):
        self.pg_connection = pg_connection
        self.last_updated_at = None
        self.last_related_updated_at = None
        self.limit = limit
        self.merger_limit = 100
        self.data_ids = list

        self.data_name = None
        self.dispatcher = {
            'person': {'related_table':'person_film_work', 'id_name': 'person_id'},
            'genre': {'related_table':'genre_film_work', 'id_name': 'genre_id'},
        }

    def extract(self, sql_query, data_class):
        raw_data = self.pg_connection.query(sql_query)
        dataclasses_data = [ data_class(**row) for row in raw_data ]
        return dataclasses_data

    # def extract_filmorks(self, last_updated_at: datetime.datetime):
    #     self.last_updated_at = last_updated_at
    #     while True:
    #         sql_query = sql_queries.fw_full_sql_query(
    #             sql_limit=self.limit,
    #             updated_at=self.last_updated_at)
    #         result = self.extract(sql_query, FilmWork)
    #         if len(result) == 0:
    #             break
    #         self.last_updated_at = result[-1].updated_at
    #         yield result




    def extract_fw_helper(self, fw_ids: set):
        if self.data_name == "person":
            sql_query = sql_queries.fw_persons_sql_query(fw_ids)
        elif self.data_name == "genre":
            sql_query = sql_queries.fw_genres_sql_query(fw_ids)
        else:
            raise ValueError

        return self.extract(sql_query, FilmWorkPersons)

    def nested_pre_extract(self):
        while True:
            raw_data_first = self.pg_connection.query(sql_queries.nested_pre_sql(
                table=self.data_name,
                updated_at=self.last_updated_at,
                limit=self.limit)
            )
            if len(raw_data_first) == 0:
                break
            data_first = [ BaseRecord(**row) for row in raw_data_first ]
            self.last_updated_at = data_first[-1].updated_at
            yield data_first

    def nested_ids_extract(self, data_ids):
        # self.data_ids = data_ids
        while True:
            raw_data_second = self.pg_connection.query(sql_queries.nested_fw_ids_sql(
                related_table = self.dispatcher[self.data_name]['related_table'],
                related_id = self.dispatcher[self.data_name]['id_name'],
                data_name_ids = data_ids,
                updated_at_rfw = self.last_related_updated_at,
                limit=self.limit
            ))
            if len(raw_data_second) == 0:
                break
            data_second = [ BaseRecord(**row) for row in raw_data_second ]
            self.last_related_updated_at = data_second[-1].updated_at
            yield data_second

    def nested_extractor(self, data_name: str, last_updated_at: datetime.datetime, related_updated_at: datetime.datetime):

        self.data_name = data_name
        self.last_updated_at = last_updated_at
        self.last_related_updated_at = related_updated_at

        first_result = self.nested_pre_extract()
        fw_ids = set()
        c = 0
        for first in first_result:
            first_ids = [rows.id for rows in first]
            print(f"LAST_UPD: {self.last_updated_at}")
            # print(first_ids)
            second_result = self.nested_ids_extract(first_ids)
            for second in second_result:
                # print(f"LAST_UPD_RELATED: {self.last_related_updated_at}")
                # print(f"SECOND: {second}")
                inter_set = {record.id for record in second}
                # # print(inter_set)
                fw_ids = fw_ids.union(inter_set)
                c = c + len(fw_ids)
                if len(fw_ids) >= self.merger_limit:
                    print(f"LEN SET: {len(fw_ids)}")
                    # print(self.extract_fw_helper(fw_ids))
                    yield self.extract_fw_helper(fw_ids)
                    fw_ids.clear()
            self.last_related_updated_at = datetime.datetime.fromtimestamp(0)
        # self.pg_connection.query("SELECT film_work_id as fw_id FROM content.genre_film_work WHERE fw_id NOT IN ")
                # print("second loop")
            #     print(second)
                # final_result = self.extract_fw_helper(data_name, )



class Producer:
    def __init__(self, pg_connection: PostgresConnection, limit=10, sql_query = None, sql_values: dict = None,
                 data_class: dataclasses.dataclass = BaseRecord):
        self.pg_connection = pg_connection
        self.sql_query = sql_query
        self.sql_values = sql_values
        self.data_class = data_class

    def extract(self):
        raw_data = self.pg_connection.query(self.sql_query, self.sql_values)
        dataclasses_data = [ self.data_class(**row) for row in raw_data ]
        return dataclasses_data

    def generator(self):
        # self.marker = marker
        while True:
            # sql_query = sql_queries.fw_full_sql_query()
            result = self.extract()
            if len(result) == 0:
                break
            # self.last_updated_at = result[-1].updated_at
            yield result

    def set_sql_values(self, values: dict):
        self.sql_values = values

    def update_sql_value(self, key: str, value: any):
        self.sql_values[key] = value

class Enricher(Producer):
    def __init__(self, pg_connection: PostgresConnection, producer: Producer, **kwargs):
        self.producer = producer
        super().__init__(pg_connection, **kwargs)

    def extract(self):
        raw_data = self.pg_connection.query(self.sql_query, self.sql_values)
        dataclasses_data = [ self.data_class(**row) for row in raw_data ]
        return dataclasses_data

    def generator(self):
        for pr in self.producer.generator():
            print('Producer in enricher worked')
            print(f"LAST PRODUCED UPD: {pr[-1].updated_at}")
            produced_data = [getattr(rows, 'id') for rows in pr]

            self.producer.update_sql_value('updated_at', getattr(pr[-1], 'updated_at'))
            while True:
                self.update_sql_value('data_ids', tuple(produced_data))
                # sql_query = sql_queries.fw_full_sql_query()
                result = self.extract()
                print(f"ENRICHER OFFSET: {self.sql_values['offset']}")
                if len(result) == 0:
                    break
                # self.last_updated_at = result[-1].updated_at
                yield result
            self.update_sql_value('offset', 0)




if __name__ == "__main__":
    conf = Config.parse_config("./config")
    logging.error("123123")


    with PostgresConnection(conf.pg_database.dict()) as pg_conn:
        dt = datetime.datetime.fromtimestamp(0)

        # dt = datetime.datetime.strptime("2020-06-16 20:14:09.222016 +00:00", "%Y-%m-%d %H:%M:%S.%f %z")
        # dt2 = datetime.datetime.strptime("2021-06-16 20:14:09.222016 +00:00", "%Y-%m-%d %H:%M:%S.%f %z")
        # a = PostgresDataExtractor(pg_conn, limit=10)

        # ---- fw producer ----
        # a = Producer(pg_conn, sql_query=sql_queries.fw_full_sql_query(),
        #              sql_values={'updated_at': dt, 'sql_limit': 10}, data_class=FilmWork)
        # ---- producer then enricher (person) ----
        # offset = 0
        # a = Producer(pg_conn, sql_query=sql_queries.nested_pre_sql('person'),
        #               sql_values={'updated_at': dt, 'limit': 10})
        # b = Enricher(pg_conn, a, sql_query=sql_queries.nested_fw_ids_sql('person_film_work', 'person_id'),
        #              sql_values={'offset': offset, 'limit': 10})
        # ---- producer then enricher (genre) ----
        offset = 0
        a = Producer(pg_conn, sql_query=sql_queries.nested_pre_sql('genre'),
                      sql_values={'updated_at': dt, 'limit': 100})
        b = Enricher(pg_conn, a, sql_query=sql_queries.nested_fw_ids_sql('genre_film_work', 'genre_id'),
                     sql_values={'offset': offset, 'limit': 100})


        # fws = a.generator()
        # fw_persons = a.nested_extractor("person", dt, dt)
        s = set()
        for fw_objects in b.generator():
        #     print("----")
            ps = {el.id for el in fw_objects}
            print(len(ps))
            s = s.union(ps)
            # print({el.id for el in fw_objects})
            print(fw_objects)
            # print(s)
            new_offset = b.sql_values['offset'] + b.sql_values['limit']
            b.update_sql_value('offset', new_offset)
            # b.update_sql_value('updated_at_rfw', fw_objects[-1].updated_at)
        print(len(s))
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

