import sqlite3
from datetime import date

# Path to your database file
db_path = r"C:\Users\Ruggero\Documents\GitHub\lama-restock\Database\supermarket.db"

conn = sqlite3.connect(db_path)
cur = conn.cursor()

try:
    # Start transaction
    cur.execute("BEGIN TRANSACTION;")
    
    # 1️⃣ Create the new table
    cur.execute("""
        CREATE TABLE product_stats_new (
            cod INTEGER NOT NULL,
            v INTEGER NOT NULL,
            sold_last_24 TEXT CHECK(json_valid(sold_last_24)),
            bought_last_24 TEXT CHECK(json_valid(bought_last_24)),
            stock INTEGER DEFAULT 0,
            verified BOOLEAN DEFAULT 0,
            last_update DATE,
            FOREIGN KEY (cod, v) REFERENCES products (cod, v),
            PRIMARY KEY (cod, v)
        );
    """)

    # 2️⃣ Copy all data from old table, setting today's date
    cur.execute("""
        INSERT INTO product_stats_new (
            cod, v, sold_last_24, bought_last_24, stock, verified, last_update
        )
        SELECT
            cod, v, sold_last_24, bought_last_24, stock, verified, DATE('now')
        FROM product_stats;
    """)

    # 3️⃣ Drop old table
    cur.execute("DROP TABLE product_stats;")

    # 4️⃣ Rename new table
    cur.execute("ALTER TABLE product_stats_new RENAME TO product_stats;")

    # Commit changes
    conn.commit()

    print("✅ Table altered successfully.")
    print("Today's SQLite DATE('now') =", date.today().isoformat())

except Exception as e:
    conn.rollback()
    print("❌ Error:", e)

finally:
    conn.close()