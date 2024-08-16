import pymysql

from databases.DefaultDataBase import DefaultDataBase
from private_cfg import DB_PASSWORD, MT_AUTO_MODERATOR_DB


class AutoModeratorRp(DefaultDataBase):
    def __init__(self):
        self.con_moderator = pymysql.connect(
            host="localhost",
            user="root",
            password=DB_PASSWORD,
            db=MT_AUTO_MODERATOR_DB,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        super().__init__(connection=self.con_moderator)

    def get_all_users(self):
        _command = "SELECT * FROM `users` ORDER BY `time` DESC;"
        return self._select(_command)
