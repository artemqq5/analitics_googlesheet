import asyncio
import logging
import os
import pickle
import time
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

LIMITER_TIME = 5
last_time = 0


class GoogleSheetAPI:
    def __init__(self):
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.TOKEN_FILENAME = 'token.pickle'
        self.CREDS_FILENAME = 'credential.json'
        self.SPREADSHEET_ID = '1g0SNORP1BpENLKOcmmZRV0MZgJeQmr5ooNXz_15MhRE'

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

    def get_sheets(self, service):
        sheets_metadata = service.spreadsheets().get(spreadsheetId=self.SPREADSHEET_ID).execute()
        return {sheet['properties']['title']: sheet['properties']['sheetId'] for sheet in
                sheets_metadata.get('sheets', [])}

    def create_sheet(self, sheet_name, service):
        body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        response = service.spreadsheets().batchUpdate(spreadsheetId=self.SPREADSHEET_ID, body=body).execute()
        return response["replies"][0]["addSheet"]["properties"]["sheetId"]

    async def update_sheet(self, teams_data):
        creds = self.authenticate()
        service = build('sheets', 'v4', credentials=creds)
        existing_sheets = self.get_sheets(service)

        request_count = 0  # Добавлено для отслеживания количества запросов

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

            request_count += 1  # Увеличиваем счетчик запросов
            if request_count % 10 == 0:  # Каждые 10 запросов
                logging.info("Достижение лимита, ожидание 10 секунд...")
                await asyncio.sleep(10)  # Задержка 10 секунд

            await self.create_table_with_formatting(sheet_id, row_count, service, team_data['data'])

            logging.info(f"Updated sheet for {sheet_name} at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    @staticmethod
    def clear_page(sheet_name, sheet_id, table_id, service):
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
        await asyncio.sleep(10)

    @staticmethod
    def process_transactions(sub_transactions, refunded):
        """
        Обрабатывает данные из двух списков, объединяя их в нужный формат.
        """
        team_data = {}

        logging.info(
            f"Начинаем обработку транзакций. Получено {len(sub_transactions)} sub_transactions и {len(refunded)} refunded.")

        # Авторизация MCC API
        auth = YeezyAPI().generate_auth(MCC_ID, MCC_TOKEN)
        if not auth:
            logging.error(f"Ошибка авторизации MCC: {MCC_ID}")

        unique_transactions = [
            (transaction['sub_account_uid'], transaction['mcc_uuid'], transaction['team_name'])
            for transaction in sub_transactions
        ]
        unique_refunds = [
            (refund['account_uid'], refund['mcc_uuid'], refund['team_name'])
            for refund in refunded
        ]
        unique_accounts = set(unique_transactions + unique_refunds)
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
                refund_value = ref_account.get('refund_value', None) if ref_account else None
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


# 🔹 **Пример использования**
if __name__ == "__main__":
    sub_transactions = GoogleAgencyRp().get_account_transactions()
    refunded = GoogleAgencyRp().get_refunded_accounts()

    formatted_data = GoogleSheetAPI().process_transactions(sub_transactions, refunded)
    print(formatted_data)

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


    save_list_to_file(formatted_data, f'data_{datetime.now().strftime("%Y-%m-%d %H:%M")}.txt')

    formatted_data = [
        {'team_name': 'team1', 'data': [
            {'MCC': '101', 'DATE': '2025-01-05', 'EMAIL': 'user1@example.com', 'AMOUNT': 50, 'SPENT': 20,
             'REFUND': None},
            {'MCC': '102', 'DATE': '2025-01-10', 'EMAIL': 'user2@example.com', 'AMOUNT': 75, 'SPENT': 30, 'REFUND': 5}
        ]},
        {'team_name': 'team2', 'data': [
            {'MCC': '201', 'DATE': '2025-02-15', 'EMAIL': 'user3@example.com', 'AMOUNT': 100, 'SPENT': 50,
             'REFUND': 10},
            {'MCC': '202', 'DATE': '2025-02-20', 'EMAIL': 'user4@example.com', 'AMOUNT': 150, 'SPENT': 80,
             'REFUND': None}
        ]}
    ]

    sheet_api = GoogleSheetAPI()
    asyncio.run(sheet_api.update_sheet(formatted_data))
