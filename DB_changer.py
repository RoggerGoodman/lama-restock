import sqlite3

# Path to your database file
db_path = r"C:\Users\rugge\Documents\GitHub\lama-restock\LamApp\databases\Matteo_Todis_Gubbio.db"

conn = sqlite3.connect(db_path)
cur = conn.cursor()

try:
    # Check if the column already exists
    cur.execute("PRAGMA table_info(product_stats);")
    columns = [row[1] for row in cur.fetchall()]

    if "sales_sets" not in columns:
        print("Adding 'sales_sets' column...")
        cur.execute("""
            ALTER TABLE product_stats
            ADD COLUMN sales_sets TEXT CHECK (json_valid(sales_sets)) DEFAULT '[]';
        """)
        print("Column added.")
    else:
        print("Column 'sales_sets' already exists. Skipping.")

    conn.commit()
    print("✅ Table altered successfully.")

except Exception as e:
    conn.rollback()
    print("❌ Error:", e)

finally:
    conn.close()
