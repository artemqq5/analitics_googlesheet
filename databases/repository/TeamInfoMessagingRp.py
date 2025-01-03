import pymysql

from databases.DefaultDataBase import DefaultDataBase
from private_cfg import DB_PASSWORD, MT_MESSAGING_DB


class TeamInfoMessagingRp(DefaultDataBase):

    def __init__(self):
        super().__init__(MT_MESSAGING_DB)

    def get_chat_data(self, chat_type):
        _command = f'SELECT * FROM `chats` WHERE `{chat_type}` = 1 ORDER BY `time` DESC;'
        return self._select(_command)

    def get_users_from_info_bot(self):
        _command = 'SELECT * FROM `users` ORDER BY `time` DESC;'
        return self._select(_command)
