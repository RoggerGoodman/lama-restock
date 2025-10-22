import pandas as pd
import os
from DatabaseManager import DatabaseManager
from consts import INVENTORY_FOLDER

def adjust_stocks_from_excel(db:DatabaseManager):
    # Load Excel (first sheet by default)
    for file_name in os.listdir(INVENTORY_FOLDER):
            
        if not file_name.endswith('.csv'):
            raise ValueError('filename must be csv')
        # Full path to the current CSV file
        file_path = os.path.join(INVENTORY_FOLDER, file_name)

        # Read CSV file
        df = pd.read_csv(file_path)
        # Adjust column names based on your Excel structure
        COD_COL = "Cod."
        V_COL = "Diff."
        STOCK_COL = "Qta"
        tot = 0

        # Go through each row
        for _, row in df.iterrows():
            try:
                cod_str = str(row[COD_COL]).replace('.', '').replace(',', '.').split('.')[0]
                v_str = str(row[V_COL]).replace('.', '').replace(',', '.').split('.')[0]
                stock_str = str(row[STOCK_COL]).replace('.', '').replace(',', '.').split('.')[0]

                cod = int(cod_str)                
                v = int(v_str)
                new_stock = int(stock_str) 
                #new_stock = (new_stock * -1) #in case to subtract stock

                db.adjust_stock(cod, v, new_stock)
                print(f"Verified stock for {cod}.{v} → {new_stock}")
                tot += 1

            except Exception as e:
                print(f"Skipped row due to error: {e}")

        try:
            os.remove(file_path)
            print(f"Deleted file: {file_path}")
        except Exception as e:
            print(f"Could not delete file {file_path}: {e}")

    db.conn.close()
    print("All verifications complete.")
    print(f"Verified {tot} entries")
