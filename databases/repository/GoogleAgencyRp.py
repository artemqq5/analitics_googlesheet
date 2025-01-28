from databases.DefaultDataBase import DefaultDataBase
from private_cfg import GOOGLE_AGENCY_DB


class GoogleAgencyRp(DefaultDataBase):

    def __init__(self):
        super().__init__(GOOGLE_AGENCY_DB)

    def get_taxes_transactions(self):
        _command = f'SELECT * FROM `taxes` ORDER BY `id` DESC;'
        return self._select(_command)

    def get_teams(self):
        _command = f'SELECT * FROM `teams` ORDER BY `team_id` DESC;'
        return self._select(_command)

    def get_accounts(self):
        _command = f'SELECT * FROM `sub_accounts` ORDER BY `created` DESC;'
        return self._select(_command)

    def get_refunded_accounts(self):
        _command = f'SELECT * FROM `refunded_accounts` ORDER BY `created` DESC;'
        return self._select(_command)

    def get_mcc(self):
        _command = f'SELECT * FROM `mcc` ORDER BY `created` DESC;'
        return self._select(_command)

    def get_balances(self):
        _command = f'SELECT * FROM `balances`;'
        return self._select(_command)

    def get_account_transactions(self):
        _command = f'SELECT * FROM `sub_transactions` ORDER BY `id` DESC;'
        return self._select(_command)

    def get_account_by_uid(self, account_uid):
        query = "SELECT * FROM `sub_accounts` WHERE `account_uid` = %s LIMIT 1;"
        return self._select_one(query, (account_uid,))

    def get_mcc_by_uuid(self, mcc_uuid):
        query = "SELECT * FROM `mcc` WHERE `mcc_uuid` = %s LIMIT 1;"
        return self._select_one(query, (mcc_uuid,))

    def get_mcc_transactions(self):
        _command = f'SELECT * FROM `transactions` ORDER BY `id` DESC;'
        return self._select(_command)


