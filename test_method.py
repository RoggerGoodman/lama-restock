import sqlite3
from datetime import date

# Path to your database file
db_path = r"C:\Users\Ruggero\Documents\GitHub\lama-restock\Database\supermarket.db"

conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute("DROP TABLE extra_losses;")