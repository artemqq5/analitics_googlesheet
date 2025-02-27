import pymysql

from databases.DefaultDataBase import DefaultDataBase
from private_cfg import DB_PASSWORD, MT_APPS_RENT_DB


class AppsRentRp(DefaultDataBase):
    def __init__(self):
        super().__init__(MT_APPS_RENT_DB)

    def get_all_users(self):
        _command = "SELECT * FROM `users` ORDER BY `join_at` DESC;"
        return self._select(_command)

    def get_all_flows(self):
        _command = "SELECT * FROM `flows`;"
        return self._select(_command)

    def get_all_teams(self):
        _command = "SELECT * FROM `teams`;"
        return self._select(_command)

    def get_all_domains(self):
        _command = "SELECT * FROM `domains`;"
        return self._select(_command)

    def get_all_apps(self):
        _command = "SELECT * FROM `apps`;"
        return self._select(_command)
