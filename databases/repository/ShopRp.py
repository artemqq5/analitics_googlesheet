import pymysql

from databases.DefaultDataBase import DefaultDataBase
from private_cfg import DB_PASSWORD, MT_SHOP_DB


class ShopRp(DefaultDataBase):
    def __init__(self):
        super().__init__(MT_SHOP_DB)

    def get_orders_data(self):
        _command = 'SELECT * FROM `orders` ORDER BY `date` DESC;'
        return self._select(_command)

    def get_users_data(self):
        _command = 'SELECT * FROM `users` ORDER BY `join_at` DESC;'
        return self._select(_command)

    def get_items_data(self):
        _command = 'SELECT * FROM `items` ORDER BY `date` DESC;'
        return self._select(_command)

    def get_categories_data(self):
        _command = 'SELECT * FROM `categories` ORDER BY `date` DESC;'
        return self._select(_command)
