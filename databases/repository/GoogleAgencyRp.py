import pymysql

from databases.DefaultDataBase import DefaultDataBase
from private_cfg import DB_PASSWORD, MT_MESSAGING_DB, GOOGLE_AGENCY_DB


class GoogleAgencyRp(DefaultDataBase):

    def __init__(self):
        super().__init__(GOOGLE_AGENCY_DB)

    def get_taxes_transactions(self):
        _command = f'SELECT * FROM `taxes` ORDER BY `id` DESC;'
        return self._select(_command)

