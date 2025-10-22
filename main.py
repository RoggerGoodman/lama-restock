from scrapper import Scrapper
from DatabaseManager import DatabaseManager
from decision_maker import DecisionMaker
from orderer import Orderer
from verifier import verify_stocks_from_excel
from adjuster import adjust_stocks_from_excel
from helpers import Helper
from consts import SPREADSHEETS_FOLDER, DATABASE_FOLDER, INVENTORY_FOLDER
import os
import re

settore = "S.PALOMBA SURGELATI"
coverage = 4
helper = Helper()
db = DatabaseManager(helper)

def list_import():
    for file_name in os.listdir(SPREADSHEETS_FOLDER):
            
        if not file_name.endswith('.xlsx'):  # Adjust for your spreadsheet extension
            continue

        storage_name = os.path.splitext(file_name)[0]  # Filename without extension
        storage_name = re.sub(r'^\d+\s+', '', storage_name) # Filename without number
        # Full path to the current spreadsheet file
        file_path = os.path.join(SPREADSHEETS_FOLDER, file_name)

        db.import_from_excel(file_path, settore=storage_name)
    db.close()

def update():
    scrapper = Scrapper(helper, db)
    scrapper.navigate()
    scrapper.init_product_stats_for_settore(settore)
    scrapper.driver.quit()

def estimate():
   db.estimate_and_update_stock_for_settore(settore)

def verify():
    verify_stocks_from_excel(db)

def make_order():
    decision_maker = DecisionMaker(helper)
    decision_maker.decide_orders_for_settore(settore, coverage)
    orders_list = decision_maker.orders_list
    orderer = Orderer()
    orderer.login()
    orderer.make_orders(settore, orders_list)
    orderer.driver.quit()

def adjust_inventory():
    adjust_stocks_from_excel(db)

#list_import()
#estimate()
#update()
#verify()
make_order()

#adjust_inventory()