import asyncio
import logging
import os
import pickle
import time
from datetime import datetime

import aiohttp
import google.auth
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from tqdm import tqdm

from YeezyAPI import YeezyAPI
from databases.repository.GoogleAgencyRp import GoogleAgencyRp
from private_cfg import MCC_ID, MCC_TOKEN

# Настроим логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class GoogleSheetAPI:
    def __init__(self):
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.TOKEN_FILENAME = 'token.pickle'
        self.CREDS_FILENAME = 'credential.json'
        self.SPREADSHEET_ID = '1g0SNORP1BpENLKOcmmZRV0MZgJeQmr5ooNXz_15MhRE'
        self.request_semaphore = asyncio.Semaphore(50)  # Обмеження на 50 запитів одночасно
        self.last_request_time = time.time()

    async def limited_request(self, coro):
        """Лімітує кількість запитів."""
        async with self.request_semaphore:
            current_time = time.time()
            # Якщо час між запитами менший за 1.2 секунди, робимо паузу (підлаштовуємо під ліміт 60 запитів/хв)
            if current_time - self.last_request_time < 1.2:
                await asyncio.sleep(1.2 - (current_time - self.last_request_time))
            self.last_request_time = time.time()
            return await coro

    def authenticate(self):
        if os.path.exists(self.TOKEN_FILENAME):
            with open(self.TOKEN_FILENAME, 'rb') as token:
                creds = pickle.load(token)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(self.CREDS_FILENAME, self.SCOPES)
            creds = flow.run_local_server(port=0)
            with open(self.TOKEN_FILENAME, 'wb') as token:
                pickle.dump(creds, token)

        return creds

    def get_sheets(self, service):
        sheets_metadata = service.spreadsheets().get(spreadsheetId=self.SPREADSHEET_ID).execute()
        return {sheet['properties']['title']: sheet['properties']['sheetId'] for sheet in
                sheets_metadata.get('sheets', [])}

    def create_sheet(self, sheet_name, service):
        body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        response = service.spreadsheets().batchUpdate(spreadsheetId=self.SPREADSHEET_ID, body=body).execute()
        return response["replies"][0]["addSheet"]["properties"]["sheetId"]

    def update_sheet(self, teams_data):
        creds = self.authenticate()
        service = build('sheets', 'v4', credentials=creds)
        existing_sheets = self.get_sheets(service)

        for team_data in teams_data:
            sheet_name = team_data['team_name']
            row_count = len(team_data['data'])

            sheet_id = existing_sheets.get(sheet_name) or self.create_sheet(sheet_name, service)

            self.clear_page(sheet_name, sheet_id, self.SPREADSHEET_ID, service)
            self.add_formulas(sheet_name, service)

            values = self.format_data_for_sheets(team_data['data'])
            service.spreadsheets().values().update(
                spreadsheetId=self.SPREADSHEET_ID,
                range=f'{sheet_name}!A5',
                valueInputOption="RAW",
                body={'values': values}
            ).execute()

            self.create_table_with_formatting(sheet_id, row_count, service, team_data['data'])

            print(f"Updated sheet for {sheet_name} at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    @staticmethod
    def clear_page(sheet_name, sheet_id, table_id, service):
        """Полностью очищает таблицу, удаляет все данные, форматирование и границы."""
        # Очищаем значения в таблице
        service.spreadsheets().values().clear(
            spreadsheetId=table_id,
            range=f"{sheet_name}!A1:Z",
            body={}
        ).execute()

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

    def add_formulas(self, sheet_name, service):
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

    async def create_table_with_formatting(self, sheet_id, row_count, service, data):
        requests = [
            # Жирные заголовки
            {
                'repeatCell': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 5, 'startColumnIndex': 0,
                              'endColumnIndex': 6},
                    'cell': {'userEnteredFormat': {'textFormat': {'bold': True}}},
                    'fields': 'userEnteredFormat.textFormat.bold'
                }
            },
            # Обводка таблицы
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

        # **Выделяем красным только те строки, где REFUND имеет значение (не None)**
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

        await self.limited_request(service.spreadsheets().batchUpdate(
            spreadsheetId=self.SPREADSHEET_ID,
            body={'requests': requests}
        ).execute)

    @staticmethod
    async def fetch_mcc(mcc_uuid):
        """Асинхронный запрос MCC по UUID."""
        return GoogleAgencyRp().get_mcc_by_uuid(mcc_uuid)

    @staticmethod
    async def fetch_account(sub_account_uid):
        """Асинхронный запрос аккаунта по UID."""
        return GoogleAgencyRp().get_account_by_uid(sub_account_uid)

    @staticmethod
    async def fetch_refunded_account(sub_account_uid):
        """Асинхронный запрос рефаунд-аккаунта по UID."""
        return GoogleAgencyRp().get_refunded_account_by_uid(sub_account_uid)

    @staticmethod
    async def fetch_verify_account(auth_token, account_uid):
        """Асинхронный запрос данных аккаунта через API."""
        return await YeezyAPI().get_verify_account(auth_token, account_uid)

    @staticmethod
    async def process_transactions(sub_transactions, refunded):
        """
        Асинхронна обробка даних з двох списків, об'єднуючи їх у потрібний формат.
        """
        team_data = {}

        logging.info(
            f"Початок обробки транзакцій. Отримано {len(sub_transactions)} sub_transactions і {len(refunded)} refunded.")

        # Авторизація MCC API
        auth = YeezyAPI().generate_auth(MCC_ID, MCC_TOKEN)
        if not auth:
            logging.error(f"Помилка авторизації MCC: {MCC_ID}")
            return []

        async with aiohttp.ClientSession() as session:
            # Обробляємо перший список (sub_transactions)
            pbar = tqdm(total=len(sub_transactions), desc="Обробка sub_transactions", unit="транзакція")
            tasks = []
            for tx in sub_transactions:
                tasks.append(GoogleSheetAPI.process_transaction(tx, team_data, auth, session, pbar))
            await asyncio.gather(*tasks)
            pbar.close()

            # Обробляємо другий список (refunded)
            pbar = tqdm(total=len(refunded), desc="Обробка refunded", unit="транзакція")
            tasks = []
            for refund in refunded:
                tasks.append(GoogleSheetAPI.process_refund(refund, team_data, session, pbar))
            await asyncio.gather(*tasks)
            pbar.close()

        logging.info(f"Готово! Оброблено {len(team_data)} команд.")
        return [{'team_name': team, 'data': data} for team, data in team_data.items()]

    @staticmethod
    async def process_transaction(tx, team_data, auth, session, pbar):
        """Обробляє одну транзакцію."""
        team_name = tx['team_name']
        if team_name not in team_data:
            team_data[team_name] = []

        mcc, account = await asyncio.gather(
            GoogleSheetAPI.fetch_mcc(tx['mcc_uuid']),
            GoogleSheetAPI.fetch_account(tx['sub_account_uid'])
        )

        if not mcc:
            logging.error(f"Не знайдено MCC для mcc_uuid={tx['mcc_uuid']}")
            pbar.update(1)
            return

        if not account:
            logging.error(f"Не знайдено акаунт для sub_account_uid={tx['sub_account_uid']}")
            account = await GoogleSheetAPI.fetch_refunded_account(tx['sub_account_uid'])
            if not account:
                logging.error(f"Не знайдено акаунт (РЕФАУНД) для sub_account_uid={tx['sub_account_uid']}")
                pbar.update(1)
                return

        account_api_response = await GoogleSheetAPI.fetch_verify_account(auth['token'], account['account_uid'])
        if not account_api_response:
            logging.error(f"Не вдалося отримати дані акаунту {account['account_uid']} з API")
            pbar.update(1)
            return

        account_api = account_api_response.get('accounts', [{}])[0]

        formatted_entry = {
            'MCC': mcc['mcc_name'],
            'DATE': tx['created'].strftime("%Y-%m-%d %H:%M"),
            'EMAIL': account['account_email'],
            'AMOUNT': tx['value'],
            'SPENT': account_api.get('spend', None),
            'REFUND': None
        }

        team_data[team_name].append(formatted_entry)
        pbar.update(1)

    @staticmethod
    async def process_refund(refund, team_data, session, pbar):
        """Обробляє один рефаунд."""
        team_name = refund['team_name']
        if team_name not in team_data:
            team_data[team_name] = []

        mcc = await GoogleSheetAPI.fetch_mcc(refund['mcc_uuid'])
        if not mcc:
            logging.error(f"Не знайдено MCC для mcc_uuid={refund['mcc_uuid']}")
            pbar.update(1)
            return

        formatted_entry = {
            'MCC': mcc['mcc_name'],
            'DATE': refund.get('completed_time', None).strftime("%Y-%m-%d %H:%M") if refund.get(
                'completed_time') else None,
            'EMAIL': refund['account_email'],
            'AMOUNT': None,
            'SPENT': refund.get('last_spend', None),
            'REFUND': refund.get('refund_value', None)
        }

        team_data[team_name].append(formatted_entry)
        pbar.update(1)


# 🔹 **Пример использования**
if __name__ == "__main__":
    sub_transactions = GoogleAgencyRp().get_account_transactions()
    refunded = GoogleAgencyRp().get_refunded_accounts()

    formatted_data = asyncio.run(GoogleSheetAPI().process_transactions(sub_transactions, refunded))

    sheet_api = GoogleSheetAPI()
    sheet_api.update_sheet(formatted_data)
