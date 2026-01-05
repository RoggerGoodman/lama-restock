# LamApp/supermarkets/scripts/inventory_reader.py - FIXED VERSION

import pandas as pd
import os
import pdfplumber
import re
from pathlib import Path
from .DatabaseManager import DatabaseManager
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

INVENTORY_FOLDER = str(settings.INVENTORY_FOLDER)
LOSSES_FOLDER = str(settings.LOSSES_FOLDER)


def verify_stocks_from_excel(db: DatabaseManager, file_path: str, cluster: str = None):
    """
    FIXED: Cluster is now passed as parameter, not derived from filename.
    
    Verifies and updates stock levels from a specific CSV file.
    
    Args:
        db: DatabaseManager instance
        file_path: Full path to CSV file
        cluster: Optional cluster name to assign (user-provided, not filename)
    """
    COD_COL = "Codice"
    V_COL = "Variante"
    STOCK_COL = "Qta Originale"
    
    logger.info(f"Processing verification file: {file_path}")
    if cluster:
        logger.info(f"Assigning cluster: {cluster}")
    
    try:
        # Load CSV
        df = pd.read_csv(file_path)
        
        # Check if required columns exist
        if COD_COL not in df.columns or V_COL not in df.columns or STOCK_COL not in df.columns:
            logger.error(f"Missing required columns. Expected: {COD_COL}, {V_COL}, {STOCK_COL}")
            return {
                'success': False,
                'error': f'Missing columns. File must have: {COD_COL}, {V_COL}, {STOCK_COL}'
            }
        
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
        
        verified_count = 0
        skipped_count = 0
        
        # Process each row
        for _, row in combined.iterrows():
            cod = int(row[COD_COL])
            v = int(row[V_COL])
            new_stock = int(row[STOCK_COL])
            
            try:
                # Verify stock and optionally assign cluster
                db.verify_stock(cod, v, new_stock, cluster)
                verified_count += 1
                logger.debug(f"Verified: {cod}.{v} = {new_stock}" + (f" (cluster: {cluster})" if cluster else ""))
            except ValueError as e:
                logger.warning(f"Skipped {cod}.{v}: {e}")
                skipped_count += 1
            except Exception as e:
                logger.exception(f"Error processing {cod}.{v}")
                skipped_count += 1
        
        # Clean up file
        try:
            os.remove(file_path)
            logger.info(f"Processed and deleted file: {file_path}")
        except Exception as e:
            logger.error(f"Could not delete file {file_path}: {e}")
        
        logger.info(f"Verification complete: {verified_count} verified, {skipped_count} skipped")
        
        return {
            'success': True,
            'verified': verified_count,
            'skipped': skipped_count,
            'cluster': cluster
        }
        
    except Exception as e:
        logger.exception(f"Error processing file {file_path}")
        return {
            'success': False,
            'error': str(e)
        }


def assign_clusters_from_csv(db: DatabaseManager, file_path: str, cluster: str):
    """
    Assign cluster to products listed in CSV (no stock update).
    
    Args:
        db: DatabaseManager instance
        file_path: Full path to CSV file
        cluster: Cluster name to assign (REQUIRED)
    """
    COD_COL = "Codice"
    V_COL = "Variante"
    
    if not cluster:
        raise ValueError("Cluster name is required for cluster assignment")
    
    logger.info(f"Assigning cluster '{cluster}' from file: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        
        if COD_COL not in df.columns or V_COL not in df.columns:
            logger.error(f"Missing required columns. Expected: {COD_COL}, {V_COL}")
            return {
                'success': False,
                'error': f'Missing columns. File must have: {COD_COL}, {V_COL}'
            }
        
        # Clean columns
        for col in [COD_COL, V_COL]:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
        
        df[COD_COL] = df[COD_COL].astype(float).astype(int)
        df[V_COL] = df[V_COL].astype(float).astype(int)
        
        assigned_count = 0
        skipped_count = 0
        
        for _, row in df.iterrows():
            cod = int(row[COD_COL])
            v = int(row[V_COL])
            
            try:
                # Verify with cluster only (no stock change)
                db.verify_stock(cod, v, new_stock=None, cluster=cluster)
                assigned_count += 1
                logger.debug(f"Assigned cluster '{cluster}' to {cod}.{v}")
            except ValueError as e:
                logger.warning(f"Skipped {cod}.{v}: {e}")
                skipped_count += 1
            except Exception as e:
                logger.exception(f"Error processing {cod}.{v}")
                skipped_count += 1
        
        # Clean up
        try:
            os.remove(file_path)
            logger.info(f"Processed and deleted file: {file_path}")
        except Exception as e:
            logger.error(f"Could not delete file {file_path}: {e}")
        
        logger.info(f"Cluster assignment complete: {assigned_count} assigned, {skipped_count} skipped")
        
        return {
            'success': True,
            'assigned': assigned_count,
            'skipped': skipped_count,
            'cluster': cluster
        }
        
    except Exception as e:
        logger.exception(f"Error assigning clusters from {file_path}")
        return {
            'success': False,
            'error': str(e)
        }


def verify_lost_stock_from_excel_combined(db: DatabaseManager):
    """
    Process loss files (ROTTURE, SCADUTO, UTILIZZO INTERNO) from LOSSES_FOLDER.
    
    UNCHANGED - works correctly as is.
    """
    COD_COL = "Code"
    V_COL = "Variant"
    STOCK_COL = "Quantity"
    
    LOSS_FILES = {
        "ROTTURE.csv": "broken",
        "SCADUTO.csv": "expired",
        "UTILIZZO INTERNO.csv": "internal"
    }

    logger.info(f"Starting loss processing. Checking folder: {LOSSES_FOLDER}")
    
    all_files = os.listdir(LOSSES_FOLDER)
    logger.info(f"Files in folder: {all_files}")
    
    files_processed = 0
    total_losses = 0

    for file_name, loss_type in LOSS_FILES.items():
        file_path = os.path.join(LOSSES_FOLDER, file_name)
        
        if not os.path.exists(file_path):
            logger.warning(f"Loss file not found: {file_name} (expected at {file_path})")
            continue
        
        logger.info(f"Processing loss file: {file_name} (type: {loss_type})")

        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            
            logger.info(f"File loaded. Shape: {df.shape}, Columns: {df.columns.tolist()}")

            if COD_COL not in df.columns:
                logger.error(f"Missing column '{COD_COL}' in {file_name}. Available: {df.columns.tolist()}")
                continue
            
            if V_COL not in df.columns:
                logger.error(f"Missing column '{V_COL}' in {file_name}. Available: {df.columns.tolist()}")
                continue
                
            if STOCK_COL not in df.columns:
                logger.error(f"Missing column '{STOCK_COL}' in {file_name}. Available: {df.columns.tolist()}")
                continue

            logger.info(f"First 3 rows of {file_name}:")
            logger.info(f"\n{df[[COD_COL, V_COL, STOCK_COL]].head(3).to_string()}")

            for col in [COD_COL, V_COL, STOCK_COL]:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )

            df[COD_COL] = pd.to_numeric(df[COD_COL], errors='coerce')
            df[V_COL] = pd.to_numeric(df[V_COL], errors='coerce')
            df[STOCK_COL] = pd.to_numeric(df[STOCK_COL], errors='coerce')
            
            initial_rows = len(df)
            df = df.dropna(subset=[COD_COL, V_COL, STOCK_COL])
            dropped_rows = initial_rows - len(df)
            
            if dropped_rows > 0:
                logger.warning(f"Dropped {dropped_rows} invalid rows from {file_name}")

            df[COD_COL] = df[COD_COL].astype(int)
            df[V_COL] = df[V_COL].astype(int)
            df[STOCK_COL] = df[STOCK_COL].astype(float)

            combined = (
                df.groupby([COD_COL, V_COL], as_index=False)[STOCK_COL]
                .sum()
                .astype({COD_COL: int, V_COL: int, STOCK_COL: int})
            )
            
            logger.info(f"After combining duplicates: {len(combined)} unique products")

            processed_count = 0
            absent_count = 0
            error_count = 0
            
            for _, row in combined.iterrows():
                cod = int(row[COD_COL])
                v = int(row[V_COL])
                delta = int(row[STOCK_COL])
                
                if delta == 0:
                    continue

                try:
                    db.register_losses(cod, v, delta, loss_type)
                    processed_count += 1
                    total_losses += delta
                    logger.debug(f"Registered {loss_type}: {cod}.{v} = {delta}")
                except ValueError as e:
                    logger.debug(f"Product {cod}.{v} not in database (will be skipped)")
                    absent_count += 1
                except Exception as e:
                    logger.warning(f"Error processing {cod}.{v}: {type(e).__name__}: {e}")
                    error_count += 1
            
            logger.info(f" Processed {file_name}: {processed_count} losses registered, {absent_count} skipped, {error_count} errors")
            files_processed += 1

            try:
                os.remove(file_path)
                logger.info(f" Deleted processed file: {file_path}")
            except Exception as e:
                logger.error(f"Could not delete file {file_path}: {e}")

        except Exception as e:
            logger.exception(f" Error reading or processing file {file_name}")
            continue
    
    logger.info(f"Loss processing complete: {files_processed} files processed, {total_losses} total units of losses registered")
    
    return {
        'success': True,
        'files_processed': files_processed,
        'total_losses': total_losses
    }

def parse_pdf(pdf_path: str):
    """
    Parse loss PDF file and extract product data.

    Aggregates quantities when the same (cod, v) appears multiple times.
    """
    aggregated = {}  # (cod, v) -> qty

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                lines = text.split('\n')

                for line in lines:
                    # Skip headers, empty lines, and page markers
                    if not line.strip():
                        continue
                    if any(x in line for x in (
                        'Stampa Articoli',
                        'Punto Vendita',
                        'Codice a Barre',
                        'Fine Stampa',
                        'Pagina'
                    )):
                        continue

                    try:
                        # Split on last " PZ "
                        left, qty_part = line.rsplit(" PZ ", 1)

                        qty = float(qty_part.replace(",", "."))

                        parts = left.split()
                        if len(parts) < 3:
                            continue

                        cod = int(parts[1])
                        v = int(parts[2])

                    except Exception:
                        continue

                    key = (cod, v)

                    # ✅ Aggregate quantity
                    aggregated[key] = aggregated.get(key, 0) + int(qty)

                    logger.debug(
                        f"Accumulated: {cod}.{v} -> {aggregated[key]}"
                    )

    except Exception:
        logger.exception(f"Error parsing PDF {pdf_path}")
        return []

    # Convert aggregated dict to final result format
    results = [
        {'cod': cod, 'v': v, 'qty': qty}
        for (cod, v), qty in aggregated.items()
    ]

    logger.info(f"Parsed {len(results)} unique entries from PDF")
    return results


def process_loss_pdf(db: DatabaseManager, pdf_path: str, loss_type: str):
    """
    Process a loss PDF file and register losses in database.
    
    Args:
        db: DatabaseManager instance
        pdf_path: Path to PDF file
        loss_type: Type of loss (broken/expired/internal)
    
    Returns:
        dict: Processing results
    """
    logger.info(f"Processing loss PDF: {pdf_path} (type: {loss_type})")
    
    # Parse PDF
    entries = parse_pdf(pdf_path)
    
    if not entries:
        return {
            'success': False,
            'error': 'No valid entries found in PDF'
        }
    
    processed_count = 0
    absent_count = 0
    error_count = 0
    total_losses = 0
    
    # Register each loss
    for entry in entries:
        cod = entry['cod']
        v = entry['v']
        qty = entry['qty']
        
        try:
            db.register_losses(cod, v, qty, loss_type)
            processed_count += 1
            total_losses += qty
            logger.debug(f"Registered {loss_type}: {cod}.{v} = {qty}")
        
        except ValueError as e:
            logger.debug(f"Product {cod}.{v} not in database (skipped)")
            absent_count += 1
        
        except Exception as e:
            logger.warning(f"Error processing {cod}.{v}: {e}")
            error_count += 1
    
    logger.info(
        f"✅ Processed PDF: {processed_count} registered, "
        f"{absent_count} skipped, {error_count} errors"
    )
    
    return {
        'success': True,
        'processed': processed_count,
        'absent': absent_count,
        'errors': error_count,
        'total_losses': total_losses
    }
    
def assign_clusters_from_pdf(db: DatabaseManager, pdf_path: str, cluster: str):
    """
    REFACTORED: Assign cluster to products listed in PDF (no stock update).
    Reuses parse_loss_pdf() and feeds data to existing cluster logic.
    
    Args:
        db: DatabaseManager instance
        pdf_path: Full path to PDF file
        cluster: Cluster name to assign (REQUIRED)
    """
    if not cluster:
        raise ValueError("Cluster name is required for cluster assignment")
    
    logger.info(f"Assigning cluster '{cluster}' from PDF: {pdf_path}")
    
    try:
        # Parse PDF using existing parser
        parsed_entries = parse_pdf(pdf_path)
        
        if not parsed_entries:
            logger.error(f"No valid entries found in PDF")
            return {
                'success': False,
                'error': 'No valid entries found in PDF. Check file format.'
            }
        
        logger.info(f"Parsed {len(parsed_entries)} entries from PDF")
        
        # Get unique products (ignore quantities for cluster assignment)
        unique_products = {}
        for entry in parsed_entries:
            key = (entry['cod'], entry['v'])
            unique_products[key] = entry
        
        logger.info(f"Found {len(unique_products)} unique products")
        
        assigned_count = 0
        skipped_count = 0
        
        # Assign cluster to each product
        for (cod, v) in unique_products.keys():
            try:
                # Verify with cluster only (no stock change)
                db.verify_stock(cod, v, new_stock=None, cluster=cluster)
                assigned_count += 1
                logger.debug(f"Assigned cluster '{cluster}' to {cod}.{v}")
            except ValueError as e:
                logger.warning(f"Skipped {cod}.{v}: {e}")
                skipped_count += 1
            except Exception as e:
                logger.exception(f"Error processing {cod}.{v}")
                skipped_count += 1
        
        # Clean up
        try:
            os.remove(pdf_path)
            logger.info(f"Processed and deleted file: {pdf_path}")
        except Exception as e:
            logger.error(f"Could not delete file {pdf_path}: {e}")
        
        logger.info(f"Cluster assignment complete: {assigned_count} assigned, {skipped_count} skipped")
        
        return {
            'success': True,
            'assigned': assigned_count,
            'skipped': skipped_count,
            'cluster': cluster
        }
        
    except Exception as e:
        logger.exception(f"Error assigning clusters from PDF {pdf_path}")
        return {
            'success': False,
            'error': str(e)
        }
