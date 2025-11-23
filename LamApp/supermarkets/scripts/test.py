#from DatabaseManager import DatabaseManager
from constants import SPREADSHEETS_FOLDER, DATABASE_FOLDER, INVENTORY_FOLDER, USERNAME, PASSWORD
#from helpers import Helper
from finder import Finder
#elper = Helper()
#db = DatabaseManager(helper)

#db.create_tables()
#db.adjust_stock(11036, 1, 72) 

#db.verify_stock(38636, 1, 0)

#db.init_product_stats(33744, 1, None, None, 22, True)


finder = Finder()

finder.login(USERNAME, PASSWORD)
storages = finder.find_storages()
print(storages)




#db.add_product(33744, 1, "KINDER SORPRESA T3X32 UNISEX 60G", 1, 64, "RIANO GENERI VARI", "No")

#db.close()