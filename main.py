import asyncio
import logging
import os
import pickle
from datetime import datetime

from _decimal import Decimal
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from databases.repository.AppsRentRp import AppsRentRp
from databases.repository.AutoModeratorRp import AutoModeratorRp
from databases.repository.GoogleAgencyRp import GoogleAgencyRp
from databases.repository.ShopRp import ShopRp
from databases.repository.TeamInfoMessagingRp import TeamInfoMessagingRp
from domain.mt_google.google_ref_trans import GoogleSheetUploaderLimited
from domain.mt_google.mt_google_analytics import start_google_analitics
from private_cfg import *

# mt shop
users_shop = "Users!A1"
orders_shop = "Orders!A1"
items_shop = "Items!A1"
categories_shop = "Categories!A1"

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

# mt auto moderator
users_auto_moder = "Users!A1"

# mt apps rent
users_apps_rent = "Users!A1"
teams_apps_rent = "Teams!A1"
flows_apps_rent = "Flows!A1"
domains_apps_rent = "Domains!A1"
apps_apps_rent = "Apps!A1"

# google agency
google_taxes = "Taxes!A1"
google_teams = "Teams!A1"
google_accounts = "Accounts!A1"
google_mcc = "MCC!A1"
google_balances = "Balances!A1"
# google agency 2
google_refunded_accounts = "Refunds"
google_account_transactions = "Account Transactions"
google_mcc_transactions = "MCC Transactions"


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)


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

    print(f"{range_name} | Updated {sheet.get('updatedCells')} cells at {datetime.now().strftime('%Y-%m-%d %H:%M')}")


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

            if isinstance(value, Decimal):
                value = float(value)

            formatted_row.append(value)
        formatted_data.append(formatted_row)

    return formatted_data


def update_all_data():
    # update mt shop
    update_google_sheets_(format_data_for_sheets(ShopRp().get_users_data()), users_shop, SPREADSHEET_SHOP_ID)
    update_google_sheets_(format_data_for_sheets(ShopRp().get_orders_data()), orders_shop, SPREADSHEET_SHOP_ID)
    update_google_sheets_(format_data_for_sheets(ShopRp().get_items_data()), items_shop, SPREADSHEET_SHOP_ID)
    update_google_sheets_(format_data_for_sheets(ShopRp().get_categories_data()), categories_shop, SPREADSHEET_SHOP_ID)

    # update mt team info
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('creo')), chats_creo,
                          SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('google')), chats_google,
                          SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('fb')), chats_fb,
                          SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('console')), chats_console,
                          SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('agency_fb')), chats_agency_fb,
                          SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('agency_google')),
                          chats_agency_google, SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('apps')), chats_apps,
                          SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('pp_web')), chats_pp_web,
                          SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('pp_ads')), chats_pp_ads,
                          SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_chat_data('media')), chats_media,
                          SPREADSHEET_TEAM_INFO_ID)
    update_google_sheets_(format_data_for_sheets(TeamInfoMessagingRp().get_users_from_info_bot()), users_info,
                          SPREADSHEET_TEAM_INFO_ID)

    # update auto moderator
    update_google_sheets_(format_data_for_sheets(AutoModeratorRp().get_all_users()), users_auto_moder,
                          SPREADSHEET_AUTO_MODERATOR_ID)

    # update apps rent
    update_google_sheets_(format_data_for_sheets(AppsRentRp().get_all_users()), users_apps_rent,
                          SPREADSHEET_APPS_RENT_ID)
    update_google_sheets_(format_data_for_sheets(AppsRentRp().get_all_teams()), teams_apps_rent,
                          SPREADSHEET_APPS_RENT_ID)
    update_google_sheets_(format_data_for_sheets(AppsRentRp().get_all_flows()), flows_apps_rent,
                          SPREADSHEET_APPS_RENT_ID)
    update_google_sheets_(format_data_for_sheets(AppsRentRp().get_all_domains()), domains_apps_rent,
                          SPREADSHEET_APPS_RENT_ID)
    update_google_sheets_(format_data_for_sheets(AppsRentRp().get_all_apps()), apps_apps_rent, SPREADSHEET_APPS_RENT_ID)

    # update google agency
    update_google_sheets_(format_data_for_sheets(GoogleAgencyRp().get_taxes_transactions()), google_taxes,
                          SPREADSHEET_GOOGLE_AGENCY_ID)
    # update google agency 2
    # update_google_sheets_(format_data_for_sheets(GoogleAgencyRp().get_refunded_accounts()), google_accounts, SPREADSHEET_GOOGLE_AGENCY_ID)
    # update_google_sheets_(format_data_for_sheets(GoogleAgencyRp().get_refunded_accounts()), google_refunded_accounts, SPREADSHEET_GOOGLE_AGENCY_ID)
    # update_google_sheets_(format_data_for_sheets(GoogleAgencyRp().get_teams()), google_teams, SPREADSHEET_GOOGLE_AGENCY_ID)
    # update_google_sheets_(format_data_for_sheets(GoogleAgencyRp().get_mcc()), google_mcc, SPREADSHEET_GOOGLE_AGENCY_ID)
    # update_google_sheets_(format_data_for_sheets(GoogleAgencyRp().get_balances()), google_balances, SPREADSHEET_GOOGLE_AGENCY_ID)
    # update_google_sheets_(format_data_for_sheets(GoogleAgencyRp().get_account_transactions()), google_account_transactions, SPREADSHEET_GOOGLE_AGENCY_ID)
    # update_google_sheets_(format_data_for_sheets(GoogleAgencyRp().get_mcc_transactions()), google_mcc_transactions, SPREADSHEET_GOOGLE_AGENCY_ID)


async def main():
    uploader = GoogleSheetUploaderLimited()

    # all data raw database
    update_all_data()

    # teams statistic
    await start_google_analitics()

    # mcc transactions
    await uploader.process_and_upload_mcc_transactions(sheet_name=google_mcc_transactions)
    # account transactions
    await uploader.process_and_upload_accounts_transactions(sheet_name=google_account_transactions)
    # refunds
    await uploader.process_and_upload_refunds(sheet_name=google_refunded_accounts)


if __name__ == '__main__':
    asyncio.run(main())
