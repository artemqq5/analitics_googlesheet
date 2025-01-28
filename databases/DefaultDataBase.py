import pymysql

from private_cfg import *


class DefaultDataBase:
    def __init__(self, db_name):
        self.__connection = pymysql.connect(
            host="localhost",
            user="root",
            password=DB_PASSWORD,
            db=db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )

    def _select(self, query, args=None):
        try:
            with self.__connection as con:
                with con.cursor() as cursor:
                    cursor.execute(query, args)
                    return cursor.fetchall()
        except Exception as e:
            print(f"{5*'*'}\n({self.__connection.db}) _select_all: {e}\n\n {query} | {args}\n{5*'*'}\n\n")

    def _select_one(self, query, args=None):
        try:
            with self.__connection as con:
                with con.cursor() as cursor:
                    cursor.execute(query, args)
                    return cursor.fetchone()
        except Exception as e:
            print(f"{5*'*'}\n({self.__connection.db}) _select_one: {e}\n\n {query} | {args}\n{5*'*'}\n\n")
