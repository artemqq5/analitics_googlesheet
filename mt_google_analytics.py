import os
import pickle
from datetime import datetime
import google.auth
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

from YeezyAPI import YeezyAPI
from databases.repository.GoogleAgencyRp import GoogleAgencyRp


class GoogleSheetAPI:
    def __init__(self):
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.TOKEN_FILENAME = 'token.pickle'
        self.CREDS_FILENAME = 'credential.json'
        self.SPREADSHEET_ID = '1g0SNORP1BpENLKOcmmZRV0MZgJeQmr5ooNXz_15MhRE'

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

    def create_table_with_formatting(self, sheet_id, row_count, service, data):
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

        service.spreadsheets().batchUpdate(spreadsheetId=self.SPREADSHEET_ID, body={'requests': requests}).execute()

    @staticmethod
    def process_transactions(sub_transactions, refunded):
        """
        Обрабатывает данные из двух списков, объединяя их в нужный формат.
        """
        team_data = {}



        # Обрабатываем первый список (sub_transactions)
        for tx in sub_transactions:
            team_name = tx['team_name']
            if team_name not in team_data:
                team_data[team_name] = []  # Создаем список для команды

            mcc = GoogleAgencyRp().get_mcc_by_uuid(tx['mcc_uuid'])

            # Try Authorizate MCC API
            auth = YeezyAPI().generate_auth(mcc['mcc_id'], mcc['mcc_token'])

            if not auth:
                return

            # Get Account API info
            account_api_response = YeezyAPI().get_verify_account(auth['token'], tx['account_uid'])
            if not account_api_response:
                return

            account_api = account_api_response.get('accounts', [{}])[0]

            formatted_entry = {
                'MCC': mcc['mcc_name'],
                'DATE': tx['created'].strftime("%Y-%m-%d %H:%M"),
                'EMAIL': GoogleAgencyRp().get_account_by_uid(tx['sub_account_uid'])['account_email'],
                'AMOUNT': tx['value'],
                'SPENT': account_api['spend'],
                'REFUND': None
            }

            team_data[team_name].append(formatted_entry)

        # Обрабатываем второй список (refunded)
        for refund in refunded:
            team_name = refund['team_name']

            if team_name not in team_data:
                team_data[team_name] = []  # Если новой команды нет, создаем

            formatted_entry = {
                'MCC': GoogleAgencyRp().get_mcc_by_uuid(refund['mcc_uuid'])['mcc_name'],
                'DATE': refund['completed_time'].strftime("%Y-%m-%d %H:%M"),
                'EMAIL': refund['account_email'],
                'AMOUNT': None,
                'SPENT': refund['last_spend'],
                'REFUND': refund['refund_value']
            }

            team_data[team_name].append(formatted_entry)

        return [{'team_name': team, 'data': data} for team, data in team_data.items()]


# 🔹 **Пример использования**
if __name__ == "__main__":
    sub_transactions = GoogleAgencyRp().get_account_transactions()
    refunded = GoogleAgencyRp().get_refunded_accounts()

    formatted_data = GoogleSheetAPI().process_transactions(sub_transactions, refunded)


    # teams_data = [
    #     {'team_name': 'team1', 'data': [
    #         {'MCC': '101', 'DATE': '2025-01-05', 'EMAIL': 'user1@example.com', 'AMOUNT': 50, 'SPENT': 20,
    #          'REFUND': None},
    #         {'MCC': '102', 'DATE': '2025-01-10', 'EMAIL': 'user2@example.com', 'AMOUNT': 75, 'SPENT': 30, 'REFUND': 5},
    #         {'MCC': '103', 'DATE': '2025-01-15', 'EMAIL': 'user3@example.com', 'AMOUNT': 100, 'SPENT': 50,
    #          'REFUND': 5},
    #         {'MCC': '104', 'DATE': '2025-01-18', 'EMAIL': 'user4@example.com', 'AMOUNT': 120, 'SPENT': 80,
    #          'REFUND': 10},
    #         {'MCC': '105', 'DATE': '2025-01-22', 'EMAIL': 'user5@example.com', 'AMOUNT': 90, 'SPENT': 60,
    #          'REFUND': None},
    #         {'MCC': '106', 'DATE': '2025-01-25', 'EMAIL': 'user6@example.com', 'AMOUNT': 200, 'SPENT': 150,
    #          'REFUND': None},
    #         {'MCC': '107', 'DATE': '2025-01-27', 'EMAIL': 'user7@example.com', 'AMOUNT': 300, 'SPENT': 220,
    #          'REFUND': None},
    #     ]}
    # ]

    sheet_api = GoogleSheetAPI()
    sheet_api.update_sheet(formatted_data)
