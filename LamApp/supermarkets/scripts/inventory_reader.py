import pandas as pd
import os
from .DatabaseManager import DatabaseManager
from .constants import INVENTORY_FOLDER

def verify_stocks_from_excel(db: DatabaseManager):
    """
    Verifies and updates stock levels from CSV files inside INVENTORY_FOLDER.

    - Supports multiple rows with the same (Codice, Variante).
    - When duplicates exist, their 'Qta Originale' values are summed.
    - Each processed file is deleted after successful processing.
    """
    COD_COL = "Codice"
    V_COL = "Variante"
    STOCK_COL = "Qta Originale"

    for file_name in os.listdir(INVENTORY_FOLDER):
        if not file_name.endswith('.csv'):
            raise ValueError("filename must be csv")
        
        cluster = os.path.splitext(file_name)[0]  # Filename without extension

        file_path = os.path.join(INVENTORY_FOLDER, file_name)
        print(f"Processing file: {file_path}")

        try:
            # Load CSV
            df = pd.read_csv(file_path)

            # Clean and normalize numeric columns
            for col in [COD_COL, V_COL, STOCK_COL]:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )

            # Convert to integers safely
            df[COD_COL] = df[COD_COL].astype(float).astype(int)
            df[V_COL] = df[V_COL].astype(float).astype(int)
            df[STOCK_COL] = df[STOCK_COL].astype(float)

            # 🔸 Combine duplicates by summing STOCK_COL
            combined = (
                df.groupby([COD_COL, V_COL], as_index=False)[STOCK_COL]
                .sum()
                .astype({COD_COL: int, V_COL: int, STOCK_COL: int})
            )

            # Go through combined rows
            for _, row in combined.iterrows():
                cod = int(row[COD_COL])
                v = int(row[V_COL])
                new_stock = int(row[STOCK_COL])

                try:
                    db.verify_stock(cod, v, new_stock, cluster)
                    
                except Exception as e:
                    print(f"⚠️ Skipped {cod}.{v} due to error: {e}")
            try:
                os.remove(file_path)
                print(f"🗑️ Deleted file: {file_path}")
            except Exception as e:
                print(f"⚠️ Could not delete file {file_path}: {e}")

        except Exception as e:
            print(f"❌ Error reading or processing file {file_name}: {e}")            

    db.conn.close()
    print("All verifications complete.")
    

def verify_lost_stock_from_excel_combined(db: DatabaseManager):
    """
    Verifies and updates stock levels from CSV files inside INVENTORY_FOLDER.

    - Supports multiple rows with the same (Codice, Variante).
    - When duplicates exist, their 'Qta Originale' values are summed.
    - Each processed file is deleted after successful processing.
    """
    COD_COL = "Codice"
    V_COL = "Variante"
    STOCK_COL = "Qta Originale"

    for file_name in os.listdir(INVENTORY_FOLDER):
        if not file_name.endswith('.csv'):
            raise ValueError("filename must be csv")

        file_path = os.path.join(INVENTORY_FOLDER, file_name)
        print(f"Processing file: {file_path}")

        try:
            # Load CSV
            df = pd.read_csv(file_path)

            # Clean and normalize numeric columns
            for col in [COD_COL, V_COL, STOCK_COL]:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )

            # Convert to integers safely
            df[COD_COL] = df[COD_COL].astype(float).astype(int)
            df[V_COL] = df[V_COL].astype(float).astype(int)
            df[STOCK_COL] = df[STOCK_COL].astype(float)

            # 🔸 Combine duplicates by summing STOCK_COL
            combined = (
                df.groupby([COD_COL, V_COL], as_index=False)[STOCK_COL]
                .sum()
                .astype({COD_COL: int, V_COL: int, STOCK_COL: int})
            )

            # Go through combined rows
            for _, row in combined.iterrows():
                cod = int(row[COD_COL])
                v = int(row[V_COL])
                new_stock = int(row[STOCK_COL])

                try:
                    match file_name:
                        case "ROTTURE.csv":
                            type = "broken"
                        case "SCADUTO.csv":
                            type = "expired"
                        case "UTILIZZO INTERNO.csv":
                            type = "internal"
                    db.register_losses(cod, v, new_stock, type)
                    
                except Exception as e:
                    print(f"⚠️ Skipped {cod}.{v} due to error: {e}")

        except Exception as e:
            print(f"❌ Error reading or processing file {file_name}: {e}")
        finally:
            # Attempt to delete the file (whether success or not)
            try:
                os.remove(file_path)
                print(f"🗑️ Deleted file: {file_path}")
            except Exception as e:
                print(f"⚠️ Could not delete file {file_path}: {e}")

    db.conn.close()


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
    print("All adjustment complete.")
    print(f"Adjusted {tot} entries")
