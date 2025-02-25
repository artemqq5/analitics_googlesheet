import asyncio
import json
import logging
import os
import pickle
import time
from datetime import datetime
from functools import lru_cache

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from tqdm import tqdm

from YeezyAPI import YeezyAPI
from databases.repository.GoogleAgencyRp import GoogleAgencyRp
from private_cfg import MCC_ID, MCC_TOKEN

# Настроим логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class GoogleSheetAPI:
    def __init__(self):
        self.request_count = 0
        self.last_request_time = time.time()
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.TOKEN_FILENAME = 'token.pickle'
        self.CREDS_FILENAME = 'credential.json'
        self.SPREADSHEET_ID = '1g0SNORP1BpENLKOcmmZRV0MZgJeQmr5ooNXz_15MhRE'

    async def time_limiter_count(self):
        self.request_count += 1
        if self.request_count % 5 == 0:  # Чекаємо кожні 5 запитів
            await asyncio.sleep(10)  # Чекаємо 10 секунд

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

        updates = []
        formatting_requests = []
        batch_clear_and_formulas = []

        for team_data in teams_data:
            sheet_name = team_data['team_name']
            row_count = len(team_data['data'])

            sheet_id = existing_sheets.get(sheet_name) or await self.create_sheet(sheet_name, service)

            batch_clear_and_formulas.extend(self.create_clear_and_formulas_requests(sheet_name, sheet_id))

            values = self.format_data_for_sheets(team_data['data'])
            updates.append({
                "range": f'{sheet_name}!A5',
                "values": values
            })
            formatting_requests.extend(self.create_formatting_requests(sheet_id, row_count, team_data['data']))

        # Виконуємо batchUpdate для очищення і формул
        if batch_clear_and_formulas:
            await self.batch_update_clear_and_formulas(batch_clear_and_formulas, service)

        # Виконуємо batchUpdate для запису значень
        if updates:
            await self.batch_update_sheets(updates, service)

        # Виконуємо batchUpdate для форматування
        if formatting_requests:
            await self.batch_update_formatting(formatting_requests, service)

        logging.info(f"Updated {len(teams_data)} sheets at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    def create_clear_and_formulas_requests(self, sheet_name, sheet_id):
        return [
            # очищенння
            {
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0, "endRowIndex": 1000,  # Очищаємо до 1000 рядків
                        "startColumnIndex": 0, "endColumnIndex": 10  # Очищаємо до 10 колонок
                    },
                    "fields": "userEnteredValue, userEnteredFormat, textFormatRuns, dataValidation"
                }
            },
            # Додавання формул
            {
                "updateCells": {
                    "rows": [
                        {
                            "values": [
                                {"userEnteredValue": {"stringValue": "Updated:"}},
                                {"userEnteredValue": {"stringValue": datetime.now().strftime('%Y-%m-%d %H:%M')}}
                            ]
                        },
                        {
                            "values": [
                                {"userEnteredValue": {"stringValue": "Spend:"}},
                                {"userEnteredValue": {"formulaValue": "=SUM(F6:F)"}}
                            ]
                        },
                        {
                            "values": [
                                {"userEnteredValue": {"stringValue": "Accounts:"}},
                                {"userEnteredValue": {
                                    "formulaValue": "=SUMPRODUCT((MONTH(C6:C)=MONTH(TODAY()))*(YEAR(C6:C)=YEAR(TODAY())))"}}
                            ]
                        }
                    ],
                    "start": {"sheetId": sheet_id, "rowIndex": 0, "columnIndex": 0},
                    "fields": "userEnteredValue"
                }
            }
        ]

    async def batch_update_sheets(self, updates, service):
        batch_data = {"valueInputOption": "RAW", "data": updates}
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.SPREADSHEET_ID, body=batch_data).execute()
        await self.time_limiter_count()

    async def batch_update_formatting(self, requests, service):
        service.spreadsheets().batchUpdate(
            spreadsheetId=self.SPREADSHEET_ID, body={'requests': requests}).execute()
        await self.time_limiter_count()

    async def batch_update_clear_and_formulas(self, requests, service):
        service.spreadsheets().batchUpdate(
            spreadsheetId=self.SPREADSHEET_ID, body={'requests': requests}).execute()
        await self.time_limiter_count()

    async def clear_page(self, sheet_name, table_id, service):
        service.spreadsheets().values().clear(
            spreadsheetId=table_id,
            range=f"{sheet_name}!A1:Z",
            body={}
        ).execute()
        await self.time_limiter_count()

    def format_data_for_sheets(self, data):
        if not data:
            return []

        headers = ['ID', 'MCC', 'DATE', 'EMAIL', 'AMOUNT', 'SPEND', 'REFUND', 'CURRENT STATUS']
        formatted_data = [headers]

        for row in data:
            formatted_data.append([
                row.get('ID', ''),
                row.get('MCC', ''),
                row.get('DATE', '').strftime("%Y-%m-%d") if isinstance(row.get('DATE'), datetime) else row.get('DATE',
                                                                                                               ''),
                row.get('EMAIL', ''),
                row.get('AMOUNT', ''),
                row.get('SPEND', ''),
                row.get('REFUND', '') if row.get('REFUND') is not None else '',
                row.get('CURRENT STATUS', '')
            ])

        return formatted_data

    async def add_formulas(self, sheet_name, service):
        values = [
            ["Updated:", datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["Spend:", "=SUM(F6:F)"],
            ["Accounts:", "=SUMPRODUCT((MONTH(C6:C)=MONTH(TODAY()))*(YEAR(C6:C)=YEAR(TODAY())))"]
        ]

        service.spreadsheets().values().update(
            spreadsheetId=self.SPREADSHEET_ID,
            range=f"{sheet_name}!A1:B4",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()
        await self.time_limiter_count()

    def create_formatting_requests(self, sheet_id, row_count, data):
        requests = [
            {
                'repeatCell': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 5, 'startColumnIndex': 0,
                              'endColumnIndex': 8},
                    'cell': {'userEnteredFormat': {'textFormat': {'bold': True}}},
                    'fields': 'userEnteredFormat.textFormat.bold'
                }
            },
            {
                'updateBorders': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 5 + row_count,
                              'startColumnIndex': 0, 'endColumnIndex': 8},
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
            color = {'red': 1, 'green': 0.8, 'blue': 0.8} if row.get('REFUND') not in [None] or row.get(
                'CURRENT STATUS') in ('INACTIVE', 'CLOSED', 'FORCE_CLOSED') else {'red': 1, 'green': 1, 'blue': 1}

            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 5 + i,
                        'endRowIndex': 6 + i,
                        'startColumnIndex': 0,
                        'endColumnIndex': 8
                    },
                    'cell': {
                        'userEnteredFormat': {'backgroundColor': color}
                    },
                    'fields': 'userEnteredFormat.backgroundColor'
                }
            })

        return requests

    @staticmethod
    @lru_cache(maxsize=None)
    def get_mcc_by_uuid_cached(mcc_uuid):
        return GoogleAgencyRp().get_mcc_by_uuid(mcc_uuid) or {}

    def process_transactions(self, sub_transactions, refunded, accounts):
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

                mcc = self.get_mcc_by_uuid_cached(transaction['mcc_uuid'])
                ref_account = GoogleAgencyRp().get_refunded_account_by_uid(transaction['sub_account_uid'])
                refund_value = ref_account.get('refund_value', 0) if ref_account else None
                account = GoogleAgencyRp().get_account_by_uid(transaction['sub_account_uid']) or {}

                if account and account_api['status'] not in ('INACTIVE', 'CLOSED', 'FORCE_CLOSED'):
                    date_created = account.get('created', None)
                elif ref_account:
                    date_created = ref_account.get('completed_time', None) or ref_account.get('created', None)
                else:
                    date_created = None

                if account_api.get('spend', 0) == 0 and ref_account:
                    spend_account = ref_account.get('last_spend', 0)
                else:
                    spend_account = account_api.get('spend', 0)

                formatted_entry = {
                    'ID': account_api.get('customer_id') or "N/A",
                    'MCC': mcc.get('mcc_name', None),
                    'DATE': date_created,
                    'EMAIL': account_api.get('email', None),
                    'AMOUNT': account_api.get('balance', None),
                    'SPEND': spend_account,
                    'REFUND': refund_value,
                    'CURRENT STATUS': account_api['status']
                }

                team_data[team_name].append(formatted_entry)
                pbar.update(1)

        logging.info(f"Готово! Обработано {len(team_data)} команд.")
        return [{'team_name': team, 'data': data} for team, data in team_data.items()]


async def start_google_analitics():
    sub_transactions = GoogleAgencyRp().get_account_transactions()
    refunded = GoogleAgencyRp().get_refunded_accounts()
    accounts = GoogleAgencyRp().get_accounts_with_team()

    formatted_data = GoogleSheetAPI().process_transactions(sub_transactions, refunded, accounts)
    for team in formatted_data:
        team['data'].sort(
            key=lambda x: x['DATE'] if x['DATE'] else datetime.min,
            reverse=True
        )

    save_list_to_file(formatted_data, f'temp/data_{datetime.now().strftime("%Y-%m-%d %H:%M")}.json')

    # data = load_list_from_file(f'temp/data_2025-30-01.json')

    sheet_api = GoogleSheetAPI()
    await sheet_api.update_sheet(formatted_data)


# start_google_analitics()

def save_list_to_file(data_list, filename):
    """Зберігає список у JSON-файл (перетворюючи datetime в строку)."""
    try:
        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return obj.strftime("%Y-%m-%d %H:%M:%S")  # Формат дати як рядок
            return obj

        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(data_list, file, ensure_ascii=False, indent=4, default=convert_datetime)
        print(f"List successfully saved to {filename}")
    except Exception as e:
        print(f"An error occurred while saving the list to file: {e}")


def load_list_from_file(filename):
    """Завантажує список з JSON-файлу."""
    try:
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as file:
                return json.load(file)
    except Exception as e:
        print(f"An error occurred while loading the list from file: {e}")
    return None
