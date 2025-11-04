from DatabaseManager import DatabaseManager
from consts import SPREADSHEETS_FOLDER, DATABASE_FOLDER, INVENTORY_FOLDER
from helpers import Helper
helper = Helper()
db = DatabaseManager(helper)

db.create_tables()
#db.adjust_stock(18022, 1, -2) 

#db.verify_stock(33744, 1, 22)

#db.init_product_stats(33744, 1, None, None, 22, True)


def purge_entry():
    cur = db.conn.cursor()
    entries_to_delete = [
        (36090, 1),  # (cod, v)
        (34018, 1)
    ]

    for cod, v in entries_to_delete:
        # Delete from product_stats first (dependent)
        cur.execute("DELETE FROM product_stats WHERE cod=? AND v=?", (cod, v))
        
        # Then from products (main table)
        cur.execute("DELETE FROM products WHERE cod=? AND v=?", (cod, v))

    db.conn.commit()


#db.add_product(33744, 1, "KINDER SORPRESA T3X32 UNISEX 60G", 1, 64, "RIANO GENERI VARI", "No")

db.close()