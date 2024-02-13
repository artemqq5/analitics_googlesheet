import os
import pickle
from datetime import datetime
import schedule
import time

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from config.cfg import *

from database import MyDataBase

orders = "Orders!A1"
users = "Users!A1"
accounts = "Account_items!A1"
accounts_orders = "Account_orders!A1"
creo_orders = "Creo_orders!A1"


def update_google_sheets_(data, range_name):

    scopes = ['https://www.googleapis.com/auth/spreadsheets']

    # Шлях до файлу облікових даних
    creds_filename = 'credential.json'
    token_filename = 'token.pickle'

    # Завантажуємо збережені облікові дані, якщо вони є
    if os.path.exists(token_filename):
        with open(token_filename, 'rb') as token:
            creds = pickle.load(token)
    else:
        # Якщо збережених облікових даних немає, виконуємо авторизацію
        flow = InstalledAppFlow.from_client_secrets_file(creds_filename, scopes)
        creds = flow.run_local_server(port=0)
        # Зберігаємо облікові дані для наступних запусків
        with open(token_filename, 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    values = [list(row) for row in data]

    # Опції для запису
    body = {
        'values': [[f"last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"]] + values
    }

    # # Виконання запису
    sheet = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()

    print(f"Updated {sheet.get('updatedCells')} cells at {datetime.now().strftime('%Y-%m-%d %H:%M')}")


def format_data_for_sheets(data):
    if not data:
        return []

    # Отримуємо заголовки з ключів першого словника
    headers = list(data[0].keys())

    # Додаємо заголовки як перший рядок
    formatted_data = [headers]

    # Додаємо дані
    for row in data:
        formatted_row = []
        for key in headers:
            value = row[key]
            # Якщо значення є datetime, перетворюємо його у строку
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            formatted_row.append(value)
        formatted_data.append(formatted_row)

    return formatted_data


def update_all_data():
    update_google_sheets_(format_data_for_sheets(MyDataBase().get_orders_data()), orders)
    update_google_sheets_(format_data_for_sheets(MyDataBase().get_users_data()), users)
    update_google_sheets_(format_data_for_sheets(MyDataBase().get_accounts_data()), accounts)
    update_google_sheets_(format_data_for_sheets(MyDataBase().get_accounts_orders_data()), accounts_orders)
    update_google_sheets_(format_data_for_sheets(MyDataBase().get_creo_orders_data()), creo_orders)


if __name__ == '__main__':
    schedule.every().minute.do(update_all_data)

    while True:
        schedule.run_pending()
        time.sleep(1)

