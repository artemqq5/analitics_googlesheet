import os
import pickle
from datetime import datetime
import schedule
import time

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from config.cfg import *
from databases.database_info import MyDataBaseInfo

from databases.database_shop import MyDataBaseShop

# mt shop
orders = "Orders!A1"
users = "Users!A1"
accounts = "Account_items!A1"
accounts_orders = "Account_orders!A1"
creo_orders = "Creo_orders!A1"

# mt team info
chats_creo = "ChatsCreo!A1"
chats_google = "ChatsGoogle!A1"
chats_fb = "ChatsFB!A1"
chats_console = "ChatsConsole!A1"
chats_agency_fb = "ChatsAgencyFB!A1"
chats_agency_google = "ChatsAgencyGoogle!A1"
chats_apps = "ChatsApps!A1"
chats_pp_web = "ChatsPartnersWeb!A1"
chats_pp_ads = "ChatsPartnersADS!A1"
chats_media = "ChatsMedia!A1"
users_info = "Users!A1"


def clear_range(range_name, table_id, service):
    service.spreadsheets().values().clear(
        spreadsheetId=table_id,
        range=range_name.replace("!A1", ""),
        body={}
    ).execute()


def update_google_sheets_(data, range_name, table_id):

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

    clear_range(range_name, table_id, service)
    values = [list(row) for row in data]

    # Опції для запису
    body = {
        'values': [[f"last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"]] + values
    }

    # # Виконання запису
    sheet = service.spreadsheets().values().update(
        spreadsheetId=table_id,
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
                value = value.strftime("%Y-%m-%d %H:%M")
            formatted_row.append(value)
        formatted_data.append(formatted_row)

    return formatted_data


def update_all_data():
    # update mt shop analitycs
    update_google_sheets_(format_data_for_sheets(MyDataBaseShop().get_orders_data()), orders, SPREADSHEET_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseShop().get_users_data()), users, SPREADSHEET_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseShop().get_accounts_data()), accounts, SPREADSHEET_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseShop().get_accounts_orders_data()), accounts_orders, SPREADSHEET_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseShop().get_creo_orders_data()), creo_orders, SPREADSHEET_ID)

    # update mt team info analitycs
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_creo_data()), chats_creo, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_google_data()), chats_google, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_fb_data()), chats_fb, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_console_data()), chats_console, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_agency_fb_data()), chats_agency_fb, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_agency_google_data()), chats_agency_google, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_apps_data()), chats_apps, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_pp_web_data()), chats_pp_web, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_pp_ads_data()), chats_pp_ads, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_chats_media_data()), chats_media, SPREADSHEET_INFO_ID)
    update_google_sheets_(format_data_for_sheets(MyDataBaseInfo().get_users_from_info_bot()), users_info, SPREADSHEET_INFO_ID)


if __name__ == '__main__':
    update_all_data()

    # schedule.every().day.do(update_all_data)
    #
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)

