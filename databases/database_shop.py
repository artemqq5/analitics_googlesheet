import pymysql

from config.cfg import DB_PASSWORD


class MyDataBaseShop:

    def __init__(self):
        self.connection = pymysql.connect(
            host="localhost",
            user="root",
            password=DB_PASSWORD,
            db="mt_shop_db",
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )

    def get_orders_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `orders` ORDER BY `date` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_orders_data: {e}")
            return None

    def get_users_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `users` ORDER BY `time` DESC;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_users_data: {e}")
            return None

    def get_accounts_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `accounts`;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_accounts_data: {e}")
            return None

    def get_accounts_orders_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `account_orders`;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_accounts_orders_data: {e}")
            return None

    def get_creo_orders_data(self):
        try:
            with self.connection as connection:
                with connection.cursor() as cursor:
                    _command = '''SELECT * FROM `creo_orders`;'''
                    cursor.execute(_command)
                return cursor.fetchall()
        except Exception as e:
            print(f"get_creo_orders_data: {e}")
            return None
