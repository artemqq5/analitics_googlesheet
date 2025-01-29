import asyncio
import logging
import os
import pickle
from datetime import datetime

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from tqdm import tqdm

from YeezyAPI import YeezyAPI
from databases.repository.GoogleAgencyRp import GoogleAgencyRp
from private_cfg import MCC_ID, MCC_TOKEN

# Настроим логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

request_count = 0  # Добавлено для отслеживания количества запросов


class GoogleSheetAPI:
    def __init__(self):
        self.request_count = 0
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.TOKEN_FILENAME = 'token.pickle'
        self.CREDS_FILENAME = 'credential.json'
        self.SPREADSHEET_ID = '1g0SNORP1BpENLKOcmmZRV0MZgJeQmr5ooNXz_15MhRE'

    async def time_limiter_count(self):
        self.request_count += 1

        if self.request_count % 1 == 0:
            await asyncio.sleep(3)

    def authenticate(self):
        creds = None
        if os.path.exists(self.TOKEN_FILENAME):
            with open(self.TOKEN_FILENAME, 'rb') as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.CREDS_FILENAME, self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Збереження нового токена в файл
            with open(self.TOKEN_FILENAME, 'wb') as token:
                pickle.dump(creds, token)
        return creds

    async def get_sheets(self, service):
        sheets_metadata = service.spreadsheets().get(spreadsheetId=self.SPREADSHEET_ID).execute()
        await self.time_limiter_count()
        return {sheet['properties']['title']: sheet['properties']['sheetId'] for sheet in
                sheets_metadata.get('sheets', [])}

    async def create_sheet(self, sheet_name, service):
        body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        response = service.spreadsheets().batchUpdate(spreadsheetId=self.SPREADSHEET_ID, body=body).execute()
        await self.time_limiter_count()
        return response["replies"][0]["addSheet"]["properties"]["sheetId"]

    async def update_sheet(self, teams_data):
        creds = self.authenticate()
        service = build('sheets', 'v4', credentials=creds)
        existing_sheets = await self.get_sheets(service)

        for team_data in teams_data:
            sheet_name = team_data['team_name']
            row_count = len(team_data['data'])

            sheet_id = existing_sheets.get(sheet_name) or await self.create_sheet(sheet_name, service)

            await self.clear_page(sheet_name, sheet_id, self.SPREADSHEET_ID, service)
            await self.add_formulas(sheet_name, service)

            values = self.format_data_for_sheets(team_data['data'])
            service.spreadsheets().values().update(
                spreadsheetId=self.SPREADSHEET_ID,
                range=f'{sheet_name}!A5',
                valueInputOption="RAW",
                body={'values': values}
            ).execute()

            await self.time_limiter_count()
            await self.create_table_with_formatting(sheet_id, row_count, service, team_data['data'])

            logging.info(f"Updated sheet for {sheet_name} at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    async def clear_page(self, sheet_name, sheet_id, table_id, service):
        service.spreadsheets().values().clear(
            spreadsheetId=table_id,
            range=f"{sheet_name}!A1:Z",
            body={}
        ).execute()
        self.request_count += 1

    def format_data_for_sheets(self, data):
        if not data:
            return []

        headers = ['MCC', 'DATE', 'EMAIL', 'AMOUNT', 'SPENT', 'REFUND']
        formatted_data = [headers]

        for row in data:
            formatted_data.append([
                row.get('MCC', ''),
                row.get('DATE', '').strftime("%Y-%m-%d") if isinstance(row.get('DATE'), datetime) else row.get('DATE',
                                                                                                               ''),
                row.get('EMAIL', ''),
                row.get('AMOUNT', ''),
                row.get('SPENT', ''),
                row.get('REFUND', '') if row.get('REFUND') is not None else ''
            ])

        return formatted_data

    async def add_formulas(self, sheet_name, service):
        values = [
            ["Updated:", datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["Spend:", "=SUM(E6:E)"],
            ["Accounts:", "=SUMPRODUCT((MONTH(B6:B)=MONTH(TODAY()))*(YEAR(B6:B)=YEAR(TODAY())))"]
        ]

        service.spreadsheets().values().update(
            spreadsheetId=self.SPREADSHEET_ID,
            range=f"{sheet_name}!A1:B4",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()
        await self.time_limiter_count()

    async def create_table_with_formatting(self, sheet_id, row_count, service, data):
        requests = [
            {
                'repeatCell': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 5, 'startColumnIndex': 0,
                              'endColumnIndex': 6},
                    'cell': {'userEnteredFormat': {'textFormat': {'bold': True}}},
                    'fields': 'userEnteredFormat.textFormat.bold'
                }
            },
            {
                'updateBorders': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 5 + row_count,
                              'startColumnIndex': 0, 'endColumnIndex': 6},
                    'top': {'style': 'SOLID', 'width': 1},
                    'bottom': {'style': 'SOLID', 'width': 1},
                    'left': {'style': 'SOLID', 'width': 1},
                    'right': {'style': 'SOLID', 'width': 1},
                    'innerHorizontal': {'style': 'SOLID', 'width': 1},
                    'innerVertical': {'style': 'SOLID', 'width': 1}
                }
            }
        ]

        for i, row in enumerate(data):
            if row.get('REFUND') not in [None]:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 5 + i,
                            'endRowIndex': 6 + i,
                            'startColumnIndex': 0,
                            'endColumnIndex': 6
                        },
                        'cell': {
                            'userEnteredFormat': {'backgroundColor': {'red': 1, 'green': 0.8, 'blue': 0.8}}
                        },
                        'fields': 'userEnteredFormat.backgroundColor'
                    }
                })
            else:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 5 + i,
                            'endRowIndex': 6 + i,
                            'startColumnIndex': 0,
                            'endColumnIndex': 6
                        },
                        'cell': {
                            'userEnteredFormat': {'backgroundColor': {'red': 1, 'green': 1, 'blue': 1}}
                        },
                        'fields': 'userEnteredFormat.backgroundColor'
                    }
                })

        service.spreadsheets().batchUpdate(spreadsheetId=self.SPREADSHEET_ID, body={'requests': requests}).execute()
        await self.time_limiter_count()

    @staticmethod
    def process_transactions(sub_transactions, refunded, accounts):
        """
        Обрабатывает данные из двух списков, объединяя их в нужный формат.
        """
        team_data = {}

        logging.info(
            f"Начинаем обработку транзакций. Получено {len(sub_transactions)} sub_transactions, {len(refunded)} refunded, {len(accounts)} accounts")

        # Авторизация MCC API
        auth = YeezyAPI().generate_auth(MCC_ID, MCC_TOKEN)
        if not auth:
            logging.error(f"Ошибка авторизации MCC: {MCC_ID}")

        unique_accounts = [
            (acc['account_uid'], acc['mcc_uuid'], acc['team_name'])
            for acc in accounts
        ]
        unique_transactions = [
            (transaction['sub_account_uid'], transaction['mcc_uuid'], transaction['team_name'])
            for transaction in sub_transactions
        ]
        unique_refunds = [
            (refund['account_uid'], refund['mcc_uuid'], refund['team_name'])
            for refund in refunded
        ]
        unique_accounts = set(unique_transactions + unique_refunds + unique_accounts)
        unique_result = [
            {"sub_account_uid": account[0], "mcc_uuid": account[1], "team_name": account[2]}
            for account in unique_accounts
        ]
        logging.info(f"Унікальних акаунтів {len(unique_result)}")

        with tqdm(total=len(unique_result), desc="Обработка transactions", unit="транзакция") as pbar:
            for transaction in unique_result:
                team_name = transaction['team_name']

                if team_name not in team_data:
                    team_data[team_name] = []

                # Получаем данные об аккаунте из API
                account_api_response = YeezyAPI().get_verify_account(auth['token'], transaction['sub_account_uid'])
                if not account_api_response:
                    logging.error(f"❌ Не удалось получить данные аккаунта {transaction['sub_account_uid']} из API")
                    pbar.update(1)
                    continue

                account_api = account_api_response.get('accounts', [{}])[0]

                mcc = GoogleAgencyRp().get_mcc_by_uuid(transaction['mcc_uuid']) or {}
                ref_account = GoogleAgencyRp().get_refunded_account_by_uid(transaction['sub_account_uid'])
                refund_value = ref_account.get('refund_value', 0) if ref_account else None
                account = GoogleAgencyRp().get_account_by_uid(transaction['sub_account_uid']) or {}

                formatted_entry = {
                    'MCC': mcc.get('mcc_name', None),
                    'DATE': account.get('created', None),
                    'EMAIL': account_api.get('email', None),
                    'AMOUNT': account_api.get('balance', None),
                    'SPENT': account_api.get('spend', None),
                    'REFUND': refund_value
                }

                team_data[team_name].append(formatted_entry)
                pbar.update(1)

        logging.info(f"Готово! Обработано {len(team_data)} команд.")
        return [{'team_name': team, 'data': data} for team, data in team_data.items()]


def start_google_analitics():
    sub_transactions = GoogleAgencyRp().get_account_transactions()
    refunded = GoogleAgencyRp().get_refunded_accounts()
    accounts = GoogleAgencyRp().get_accounts()

    formatted_data = GoogleSheetAPI().process_transactions(sub_transactions, refunded, accounts)
    for team in formatted_data:
        team['data'].sort(
            key=lambda x: x['DATE'] if x['DATE'] else datetime.min,
            reverse=True
        )

    def save_list_to_file(data_list, filename):
        """
        Save a list of strings to a text file.
        Each element of the list will be written as a new line in the file.
        """
        try:
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(f"{data_list}\n")
            print(f"List successfully saved to {filename}")
        except Exception as e:
            print(f"An error occurred while saving the list to file: {e}")

    save_list_to_file(formatted_data, f'temp/data_{datetime.now().strftime("%Y-%m-%d %H:%M")}.txt')

    sheet_api = GoogleSheetAPI()
    asyncio.run(sheet_api.update_sheet(formatted_data))


# start_google_analitics()


