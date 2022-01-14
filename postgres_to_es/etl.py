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


@contextmanager
def postgres_connect(connection_opts: dict):
    pg_connect = psycopg2.connect(**connection_opts, cursor_factory=DictCursor)
    try:
        yield pg_connect
    except psycopg2.Error as err:
        logging.error(f"Error connecting to postgres: {err}")
        return False
    else:
        pg_connect.commit()
    finally:
        pg_connect.close()

class PostgresConnection:
    def __init__(self, connection_opts: dict):
        self.connection_opts = connection_opts
        self.connection = None

    @backoff()
    def __enter__(self):
        try:
            self.connection = psycopg2.connect(**self.connection_opts, cursor_factory=DictCursor)
            return self.connection
        except psycopg2.Error as err:
            logging.error(f"Error connecting to postgres: {err}")
            return False

    def __exit__(self, *args):
        self.connection.close()






if __name__ == "__main__":
    conf = Config.parse_config("./config")
    # print(type(conf.pg_database))
    with PostgresConnection(conf.pg_database.dict()) as pg_conn:
        print(f"CONN: {pg_conn}")
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
