import pandas as pd
import os
from .DatabaseManager import DatabaseManager
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

# Two separate folders for different purposes
INVENTORY_FOLDER = str(settings.INVENTORY_FOLDER)  # For verification
LOSSES_FOLDER = str(settings.LOSSES_FOLDER)  # For loss recording


def verify_stocks_from_excel(db: DatabaseManager, cluster_mode:bool = False):
    """
    Verifies and updates stock levels from CSV files inside INVENTORY_FOLDER.
    Used for manual stock verification with inventory counts.

    - Supports multiple rows with the same (Codice, Variante).
    - When duplicates exist, their 'Qta Originale' values are summed.
    - Each processed file is deleted after successful processing.
    """
    COD_COL = "Codice"
    V_COL = "Variante"
    STOCK_COL = "Qta Originale"

    for file_name in os.listdir(INVENTORY_FOLDER):
        if not file_name.endswith('.csv'):
            logger.warning(f"Skipping non-CSV file: {file_name}")
            continue
        
        cluster = os.path.splitext(file_name)[0]  # Filename without extension

        file_path = os.path.join(INVENTORY_FOLDER, file_name)
        logger.info(f"Processing verification file: {file_path}")

        try:
            # Load CSV
            df = pd.read_csv(file_path)

            # Check if required columns exist
            if COD_COL not in df.columns or V_COL not in df.columns or STOCK_COL not in df.columns:
                logger.error(f"Missing required columns in {file_name}. Expected: {COD_COL}, {V_COL}, {STOCK_COL}")
                continue

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

            # Combine duplicates by summing STOCK_COL
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
                    if cluster_mode == False:
                        db.verify_stock(cod, v, new_stock, cluster)
                        logger.debug(f"Verified stock: {cod}.{v} = {new_stock}")
                    elif cluster_mode == True:
                        new_stock = None
                        db.verify_stock(cod, v, new_stock, cluster)
                        logger.debug(f"Cluster assigned for: {cod}.{v} = {cluster}")
                except Exception as e:
                    logger.warning(f"Skipped {cod}.{v} due to error: {e}")
            
            # Delete file after processing
            try:
                os.remove(file_path)
                logger.info(f"Processed and deleted file: {file_path}")
            except Exception as e:
                logger.error(f"Could not delete file {file_path}: {e}")

        except Exception as e:
            logger.exception(f"Error reading or processing file {file_name}")            

    logger.info("All verifications complete.")


def verify_lost_stock_from_excel_combined(db: DatabaseManager):
    """
    Process loss files (ROTTURE, SCADUTO, UTILIZZO INTERNO) from LOSSES_FOLDER.
    
    CRITICAL FIXES:
    1. Only processes files that exist in folder
    2. Checks for required columns before processing
    3. Validates data types before database operations
    4. Proper error handling and logging
    5. Deletes files ONLY after successful processing
    """
    COD_COL = "Codice"
    V_COL = "Variante"
    STOCK_COL = "Qta Originale"
    
    # Map filenames to loss types
    LOSS_FILES = {
        "ROTTURE.csv": "broken",
        "SCADUTO.csv": "expired",
        "UTILIZZO INTERNO.csv": "internal"
    }

    logger.info(f"Starting loss processing. Checking folder: {LOSSES_FOLDER}")
    
    # List all files in folder
    all_files = os.listdir(LOSSES_FOLDER)
    logger.info(f"Files in folder: {all_files}")
    
    files_processed = 0
    total_losses = 0

    for file_name, loss_type in LOSS_FILES.items():
        file_path = os.path.join(LOSSES_FOLDER, file_name)
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.warning(f"Loss file not found: {file_name} (expected at {file_path})")
            continue
        
        logger.info(f"Processing loss file: {file_name} (type: {loss_type})")

        try:
            # Load CSV
            df = pd.read_csv(file_path, encoding='utf-8')
            
            logger.info(f"File loaded. Shape: {df.shape}, Columns: {df.columns.tolist()}")

            # Check if required columns exist
            if COD_COL not in df.columns:
                logger.error(f"Missing column '{COD_COL}' in {file_name}. Available: {df.columns.tolist()}")
                continue
            
            if V_COL not in df.columns:
                logger.error(f"Missing column '{V_COL}' in {file_name}. Available: {df.columns.tolist()}")
                continue
                
            if STOCK_COL not in df.columns:
                logger.error(f"Missing column '{STOCK_COL}' in {file_name}. Available: {df.columns.tolist()}")
                continue

            # Log first few rows for debugging
            logger.info(f"First 3 rows of {file_name}:")
            logger.info(f"\n{df[[COD_COL, V_COL, STOCK_COL]].head(3).to_string()}")

            # Clean and normalize numeric columns
            for col in [COD_COL, V_COL, STOCK_COL]:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )

            # Convert to numeric, dropping invalid rows
            df[COD_COL] = pd.to_numeric(df[COD_COL], errors='coerce')
            df[V_COL] = pd.to_numeric(df[V_COL], errors='coerce')
            df[STOCK_COL] = pd.to_numeric(df[STOCK_COL], errors='coerce')
            
            # Drop rows with NaN values
            initial_rows = len(df)
            df = df.dropna(subset=[COD_COL, V_COL, STOCK_COL])
            dropped_rows = initial_rows - len(df)
            
            if dropped_rows > 0:
                logger.warning(f"Dropped {dropped_rows} invalid rows from {file_name}")

            # Convert to integers
            df[COD_COL] = df[COD_COL].astype(int)
            df[V_COL] = df[V_COL].astype(int)
            df[STOCK_COL] = df[STOCK_COL].astype(float)

            # Combine duplicates by summing STOCK_COL
            combined = (
                df.groupby([COD_COL, V_COL], as_index=False)[STOCK_COL]
                .sum()
                .astype({COD_COL: int, V_COL: int, STOCK_COL: int})
            )
            
            logger.info(f"After combining duplicates: {len(combined)} unique products")

            # Process each row
            processed_count = 0
            absent_count = 0
            error_count = 0
            
            for _, row in combined.iterrows():
                cod = int(row[COD_COL])
                v = int(row[V_COL])
                delta = int(row[STOCK_COL])
                
                if delta == 0:
                    continue  # Skip zero losses

                try:
                    db.register_losses(cod, v, delta, loss_type)
                    processed_count += 1
                    total_losses += delta
                    logger.debug(f"Registered {loss_type}: {cod}.{v} = {delta}")
                except ValueError as e:
                    # Product not found in database
                    logger.debug(f"Product {cod}.{v} not in database (will be skipped)")
                    absent_count += 1
                except Exception as e:
                    logger.warning(f"Error processing {cod}.{v}: {type(e).__name__}: {e}")
                    error_count += 1
            
            logger.info(f" Processed {file_name}: {processed_count} losses registered, {absent_count} skipped. There were {error_count} errors")
            files_processed += 1

            # Delete file ONLY after successful processing
            try:
                os.remove(file_path)
                logger.info(f" Deleted processed file: {file_path}")
            except Exception as e:
                logger.error(f"Could not delete file {file_path}: {e}")

        except Exception as e:
            logger.exception(f" Error reading or processing file {file_name}")
            continue
    
    logger.info(f"Loss processing complete: {files_processed} files processed, {total_losses} total units of losses registered")