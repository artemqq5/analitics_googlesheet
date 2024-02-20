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

    def get_chats_google_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `chats` WHERE `google` = 1 ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_chats_google_data: {e}")
            return None

    def get_chats_fb_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `chats` WHERE `fb` = 1 ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_chats_fb_data: {e}")
            return None

    def get_chats_console_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `chats` WHERE `console` = 1 ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_chats_console_data: {e}")
            return None

    def get_chats_agency_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `chats` WHERE `agency` = 1 ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_chats_agency_data: {e}")
            return None

    def get_chats_apps_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `chats` WHERE `apps` = 1 ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_chats_apps_data: {e}")
            return None

    def get_chats_pp_web_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `chats` WHERE `pp_web` = 1 ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_chats_pp_web_data: {e}")
            return None

    def get_chats_pp_ads_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `chats` WHERE `pp_ads` = 1 ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_chats_pp_ads_data: {e}")
            return None

    def get_chats_media_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `chats` WHERE `media` = 1 ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_chats_media_data: {e}")
            return None
