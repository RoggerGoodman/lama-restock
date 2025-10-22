from DatabaseManager import DatabaseManager
from consts import SPREADSHEETS_FOLDER, DATABASE_FOLDER, INVENTORY_FOLDER
from helpers import Helper
helper = Helper()
db = DatabaseManager(helper)

db.adjust_stock(32770, 1, 10)
db.adjust_stock(32751, 1, 8)
db.adjust_stock(1044, 1, -9)
db.adjust_stock(27445, 1, 3)
#db.verify_stock(35510, 1, 15)

db.close()