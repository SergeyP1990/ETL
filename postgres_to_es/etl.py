from config import Config
from contextlib import contextmanager
import psycopg2
import logging
from functools import wraps
from psycopg2 import sql
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
import time

# def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
#     """
#     Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)
#
#     Формула:
#         t = start_sleep_time * 2^(n) if t < border_sleep_time
#         t = border_sleep_time if t >= border_sleep_time
#     :param start_sleep_time: начальное время повтора
#     :param factor: во сколько раз нужно увеличить время ожидания
#     :param border_sleep_time: граничное время ожидания
#     :return: результат выполнения функции
#     """
#     print("DECOR1")
def func_wrapper(func):
    print("func_wrapper")
    @wraps(func)
    def inner(*args, **kwargs):
        print("inner")
        return func(*args, **kwargs)
        # while not func(*args, **kwargs):
        #     print("while not in func")
        #     time.sleep(2)
    return inner
    # return func_wrapper


# @contextmanager
# @backoff
# def postgres_connect(connection_opts: dict):
#     print("call func")
#     pg_connect = psycopg2.connect(**connection_opts, cursor_factory=DictCursor)
#     try:
#         yield pg_connect
#     except psycopg2.Error as err:
#         logging.error(f"Error connecting to postgres: {err}")
#         return False
#     else:
#         pg_connect.commit()
#     finally:
#         pg_connect.close()

class PostgresConnection:
    def __init__(self, connection_opts: dict):
        print("init")
        self.connection_opts = connection_opts
        # self.connection = None

    # @backoff()
    @func_wrapper
    def __enter__(self):
        print("enter")
        try:
        # print("pg try")
            self.connection = psycopg2.connect(**self.connection_opts, cursor_factory=DictCursor)
            return self.connection
            # self.connection = psycopg2.connect(**self.connection_opts, cursor_factory=DictCursor)
            # print(f"CONN IN TRY {self.connection}")
            # return self.connection
        except psycopg2.Error as err:
            logging.error(f"Error connecting to postgres: {err}")
            return False

    def __exit__(self, *args):
        print("exit")
        self.connection.close()

# @backoff()
# def test_func():
#     print("lalala")


if __name__ == "__main__":
    conf = Config.parse_config("./config")
    # print(type(conf.pg_database))
    with PostgresConnection(conf.pg_database.dict()) as pg_conn:
        print(f"CONN: {pg_conn}")
    # test_func()
