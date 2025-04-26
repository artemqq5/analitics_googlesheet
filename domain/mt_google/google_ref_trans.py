import asyncio
import logging
import os
import pickle
from datetime import datetime

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from databases.repository.GoogleAgencyRp import GoogleAgencyRp
from private_cfg import SPREADSHEET_GOOGLE_AGENCY_ID2

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class GoogleSheetUploaderLimited:
    def __init__(self):
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.TOKEN_FILENAME = 'token.pickle'
        self.CREDS_FILENAME = 'credential.json'
        self.SPREADSHEET_ID = SPREADSHEET_GOOGLE_AGENCY_ID2
        self.request_count = 0

    async def time_limiter_count(self):
        self.request_count += 1
        if self.request_count % 5 == 0:
            logging.info(f"Request limit reached ({self.request_count}). Sleeping for 10 seconds.")
            await asyncio.sleep(10)

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
            with open(self.TOKEN_FILENAME, 'wb') as token:
                pickle.dump(creds, token)
        return creds

    async def clear_sheet(self, sheet_name='Sheet1'):
        service = build('sheets', 'v4', credentials=self.authenticate())
        service.spreadsheets().values().clear(
            spreadsheetId=self.SPREADSHEET_ID,
            range=sheet_name,
            body={}
        ).execute()
        await self.time_limiter_count()

    async def upload_data(self, data, headers, sheet_name='Sheet1'):
        service = build('sheets', 'v4', credentials=self.authenticate())
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        values = [
                     [f"Updated: {now}"],
                     headers,
                 ] + [
                     [row.get(header, '') for header in headers] for row in data
                 ]

        service.spreadsheets().values().update(
            spreadsheetId=self.SPREADSHEET_ID,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        await self.time_limiter_count()

        # Formatting headers bold
        sheet_metadata = service.spreadsheets().get(spreadsheetId=self.SPREADSHEET_ID).execute()
        sheet_id_list = [s['properties']['sheetId'] for s in sheet_metadata['sheets'] if
                         s['properties']['title'] == sheet_name]
        if not sheet_id_list:
            raise ValueError(f"Sheet '{sheet_name}' not found.")
        sheet_id = sheet_id_list[0]

        body = {
            'requests': [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,
                            'endRowIndex': 2,
                            'startColumnIndex': 0,
                            'endColumnIndex': len(headers)
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.textFormat.bold'
                    }
                }
            ]
        }

        service.spreadsheets().batchUpdate(
            spreadsheetId=self.SPREADSHEET_ID,
            body=body
        ).execute()
        await self.time_limiter_count()

    async def process_and_upload_mcc_transactions(self, sheet_name='Sheet1'):
        mcc_transactions = GoogleAgencyRp().get_mcc_transactions()
        processed_data = []

        for mcc_transaction in mcc_transactions:
            transaction_id = mcc_transaction.get("id") or "None"
            team_name = mcc_transaction.get("team_name") or "None"
            created_at = mcc_transaction.get('created')
            amount = mcc_transaction.get('value')

            processed_data.append({
                'ID': transaction_id,
                'Team': team_name,
                'Date': created_at.strftime('%Y-%m-%d %H:%M') if isinstance(created_at, datetime) else created_at,
                'Amount': float(amount),
            })

        headers = ['ID', 'Team', 'Date', 'Amount']

        await self.clear_sheet(sheet_name)
        await self.upload_data(processed_data, headers, sheet_name)
        logging.info(f"MCC transactions uploaded: sheet={sheet_name}, records={len(processed_data)}")

    async def process_and_upload_accounts_transactions(self, sheet_name='Sheet1'):
        account_transactions = GoogleAgencyRp().get_account_transactions()
        processed_data = []

        for account_transaction in account_transactions:
            account = GoogleAgencyRp().get_account_by_uid(account_transaction.get("sub_account_uid")) or {}

            transaction_id = account_transaction.get("id") or "None"
            team_name = account_transaction.get("team_name") or "None"
            account_email = account.get("account_email") or "None"
            account_customer_id = account.get("customer_id") or "None"
            created_at = account_transaction.get('created')
            amount = account_transaction.get('value')

            processed_data.append({
                'ID': transaction_id,
                'Team': team_name,
                'Email': account_email,
                'Customer ID': account_customer_id,
                'Date': created_at.strftime('%Y-%m-%d %H:%M') if isinstance(created_at, datetime) else created_at,
                'Amount': float(amount),
            })

        headers = ['ID', 'Team', 'Email', 'Customer ID', 'Date', 'Amount']

        await self.clear_sheet(sheet_name)
        await self.upload_data(processed_data, headers, sheet_name)
        logging.info(f"Account transactions uploaded: sheet={sheet_name}, records={len(processed_data)}")

    async def process_and_upload_refunds(self, sheet_name='Sheet1'):
        refunds = GoogleAgencyRp().get_refunded_accounts()
        processed_data = []

        for refund in refunds:
            team_name = refund.get("team_name") or "None"
            account_email = refund.get("account_email") or "None"
            account_customer_id = refund.get("customer_id") or "None"
            created_at = refund.get('created')
            refund_value = refund.get('refund_value')
            commission = refund.get('commission')

            processed_data.append({
                'Team': team_name,
                'Email': account_email,
                'Customer ID': account_customer_id,
                'Date': created_at.strftime('%Y-%m-%d %H:%M') if isinstance(created_at, datetime) else created_at,
                'Refund Amount': float(refund_value) if refund_value else "None",
                'Commission': float(commission) if commission else "None",
            })

        headers = ['Team', 'Email', 'Customer ID', 'Date', 'Refund Amount', 'Commission']

        await self.clear_sheet(sheet_name)
        await self.upload_data(processed_data, headers, sheet_name)
        logging.info(f"Refunds uploaded: sheet={sheet_name}, records={len(processed_data)}")
