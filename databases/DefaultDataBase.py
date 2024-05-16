import pymysql

from private_cfg import *


class DefaultDataBase:
    def __init__(self, connection):
        self.__connection = connection

    def _select_one(self, query, args=None):
        try:
            with self.__connection as con:
                with con.cursor() as cursor:
                    cursor.execute(query, args)
                    return cursor.fetchone()
        except Exception as e:
            print(f"({self.__connection.db}) _select_one: {e}")

    def _select(self, query, args=None):
        try:
            with self.__connection as con:
                with con.cursor() as cursor:
                    cursor.execute(query, args)
                    return cursor.fetchall()
        except Exception as e:
            print(f"({self.__connection.db}) _select_all: {e}")
