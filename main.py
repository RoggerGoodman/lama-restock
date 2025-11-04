from scrapper import Scrapper
from DatabaseManager import DatabaseManager
from decision_maker import DecisionMaker
from inventory_scrapper import Inventory_Scrapper 
from orderer import Orderer
from inventory_reader import verify_stocks_from_excel, verify_lost_stock_from_excel_combined
from helpers import Helper
from consts import SPREADSHEETS_FOLDER, DATABASE_FOLDER, INVENTORY_FOLDER
import os
import re
storages = ["01 RIANO GENERI VARI", "23 S.PALOMBA SURGELATI", "02 POMEZIA DEPERIBILI"]

product_list = [(14463, 1), (21043, 1), (30437, 1), (31951,1), (37033,1), (37306,3)]

settore = "S.PALOMBA SURGELATI"
coverage = 4
helper = Helper()
db = DatabaseManager(helper)
Inv_S = Inventory_Scrapper()

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

def register_prducts():
    scrapper = Scrapper(helper, db)
    scrapper.navigate()
    scrapper.init_products_and_stats_from_list(product_list, settore)
    scrapper.driver.quit()

def update():
    scrapper = Scrapper(helper, db)
    scrapper.navigate()
    scrapper.init_product_stats_for_settore(settore)
    scrapper.driver.quit()

def losess_recorder():
    target1 = "ROTTURE"
    target2 = "SCADUTO"
    target3 = "UTILIZZO INTERNO"
    #Inv_S.login()
    #Inv_S.inventory()
    #Inv_S.inventory_creator(target)
    #Inv_S.downloader(target1)
    #Inv_S.downloader(target2)
    #Inv_S.downloader(target3)
    #Inv_S.clean_up(target)
    verify_lost_stock_from_excel_combined(db)

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
    verify_stocks_from_excel(db) 


#list_import()
#estimate()
#update()
losess_recorder()
#verify()
make_order()
#register_prducts()

#adjust_inventory()