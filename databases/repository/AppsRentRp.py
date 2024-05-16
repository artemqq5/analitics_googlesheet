import pymysql

from databases.DefaultDataBase import DefaultDataBase
from private_cfg import DB_PASSWORD, MT_APPS_RENT_DB


class AppsRentRp(DefaultDataBase):
    def __init__(self):
        self.con_apps_rent = pymysql.connect(
            host="localhost",
            user="root",
            password=DB_PASSWORD,
            db=MT_APPS_RENT_DB,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        super().__init__(connection=self.con_apps_rent)