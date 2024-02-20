import pymysql

from config.cfg import DB_PASSWORD


class MyDataBaseInfo:

    def __init__(self):
        self.connection = pymysql.connect(
            host="localhost",
            user="root",
            password=DB_PASSWORD,
            db="mt_messaging_db",
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )

    def get_chats_creo_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `chats` WHERE `creo` = 1 ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_chats_creo_data: {e}")
            return None

