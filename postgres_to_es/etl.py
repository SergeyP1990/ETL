import datetime

from config import Config
from contextlib import contextmanager
import psycopg2
import logging
from functools import wraps
from psycopg2 import sql
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
import time
import requests

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
                print("before call func")
                result = func(*args, **kwargs)
                print("after call func")
                if result is not False:
                    return result
                print("retry")
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
        print("__exit__ called")
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

    def close(self, commit=True):
        if commit:
            self.connection.commit()
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
            self.cursor.execute(sql_query, params or ())
            return self.fetchall()
        except psycopg2.OperationalError as err:
            logging.error(f"Error connecting to postgres while query: {err}")
            logging.error("Trying to reconnect")
            self.connect()
            return False

class FilmworkExtractor:
    def __init__(self, limit=10):
        # self.last_updated_at = datetime.datetime.strptime()
        self.limit_literal = sql.Literal(limit)
        self.full_sql_query = sql.SQL(
        """
            SELECT
                fw.id as fw_id,
                fw.rating as imbd_rating,
                fw.title,
                fw.description,
                fw.updated_at,
                ARRAY_AGG(DISTINCT g.name ) AS "genres",
                ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'director') AS "director",
                ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'actor') AS "actors_names",
                ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'writer') AS "writers_names",
                JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
                JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            GROUP BY fw_id, fw.updated_at
            ORDER BY fw.updated_at
            LIMIT {sql_limit};
        """
        )

        self.persons_sql_query = sql.SQL(
        """
            SELECT
                fw.id as fw_id,
                JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'director') AS director,
                JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
                JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            WHERE fw.id IN ({filmwork_ids})
            GROUP BY fw_id;
        """
        )

        self.genres_sql_query = sql.SQL(
        """
            SELECT
                fw.id as fw_id,
                ARRAY_AGG(DISTINCT g.name ) AS "genres"
            FROM content.film_work fw
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN ({filmwork_ids})
            GROUP BY fw_id;
        """
        )

    def extract_filmorks(self, pg_connection: PostgresConnection):
        # pg_connection.query(self.full_sql_query.format(sql_limit=self.limit_literal, sql_updated_at))
        pass



if __name__ == "__main__":
    conf = Config.parse_config("./config")
    logging.error("123123")
    # conn = PostgresConnection(conf.pg_database.dict())
    # conn.connect()
    # print(conn.connection.cursor())
    # cur = conn.cursor()

    # print(type(conf.pg_database))
    with PostgresConnection(conf.pg_database.dict()) as pg_conn:
        # print(pg_conn.query("SELECT id, rating, title, description, updated_at FROM content.film_work ORDER BY updated_at LIMIT 2"))
        # q = sql.SQL("SELECT id, rating, title, description, updated_at FROM content.film_work ORDER BY updated_at ")
        a = FilmworkExtractor(limit=1)
        l = a.full_sql_query.format(sql_limit=sql.Literal(2))

        ans = pg_conn.query(l)
        print(ans[0][4])
        # print(pg_conn.query(l))

        # time.sleep(5)
        # print("new query")
        # print("QERY2 RES:" + str(pg_conn.query("SELECT id, rating, title, description, updated_at FROM content.film_work ORDER BY updated_at LIMIT 2")))
        # print(pg_conn.connection)
        # print(f"CONN: {pg_conn.connection}")
        # print(f"CONN: {pg_conn.cursor}")
    # print(f"CONN: {pg_conn.connection}")
    # print(f"CONN: {pg_conn.cursor}")
    # pg_conn.connect()
    # print(f"CONN: {pg_conn.connection}")
        # print("closing")
        # pg_conn.close()
        # print(f"CONN: {pg_conn}")
        # cur = pg_conn.cursor()
        # while True:
        #     print("---------------------------")
        #     try:
        #         cur.execute("SELECT * FROM content.film_work LIMIT 2")
        #         print(cur.fetchall())
        #     except psycopg2.OperationalError as Err:
        #         print(f"ERROR: {Err}")
        #     time.sleep(3)
    # test_func()
