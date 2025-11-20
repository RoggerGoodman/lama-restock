from DatabaseManager import DatabaseManager  # or whatever your filename is

db = DatabaseManager("supermarket.db")
db.create_tables()
db.close()