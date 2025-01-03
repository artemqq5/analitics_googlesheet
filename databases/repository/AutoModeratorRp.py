import pymysql

from databases.DefaultDataBase import DefaultDataBase
from private_cfg import DB_PASSWORD, MT_AUTO_MODERATOR_DB


class AutoModeratorRp(DefaultDataBase):
    def __init__(self):
        super().__init__(MT_AUTO_MODERATOR_DB)

    def get_all_users(self):
        _command = "SELECT * FROM `users` ORDER BY `time_added_at` DESC;"
        return self._select(_command)
