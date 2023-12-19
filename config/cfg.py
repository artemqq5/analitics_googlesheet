from config.private_cfg import *

is_debug = True

if is_debug:
    DB_PASSWORD = DB_PASSWORD_TEST
    SPREADSHEET_ID = SPREADSHEET_ID_TEST
else:
    DB_PASSWORD = DB_PASSWORD_PROD  
    SPREADSHEET_ID = SPREADSHEET_ID_PROD
