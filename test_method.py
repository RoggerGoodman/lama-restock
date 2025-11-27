import sqlite3
from datetime import date

# Path to your database file
db_path = r"C:\Users\rugge\Documents\GitHub\lama-restock\Database\supermarket.db"

conn = sqlite3.connect(db_path)
cur = conn.cursor()

def purge_unverified_products():
    

    # Step 1
    cur.execute("SELECT cod, v FROM product_stats WHERE verified = 0")
    to_purge = cur.fetchall()

    TABLES = [
        "products",
        "product_stats",
        "economics"
    ]

    for cod, v in to_purge:
        for table in TABLES:
            cur.execute(f"DELETE FROM {table} WHERE cod=? AND v=?", (cod, v))

    conn.commit()
    print("Purge complete.")

def change_data( old_value="RIANO GENERI VARI", new_value="GENERI VARI"):
    cur.execute("""
        UPDATE products
        SET settore = ?
        WHERE settore = ?
    """, (new_value, old_value))
    conn.commit()
    print(f"Updated {cur.rowcount} rows.")


    
change_data()
#purge_unverified_products()
#cur.execute("DROP TABLE economics;")