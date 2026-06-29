# LamApp/supermarkets/scripts/inventory_reader.py - FIXED VERSION

import pandas as pd
import os
import pdfplumber
from .DatabaseManager import DatabaseManager
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

INVENTORY_FOLDER = str(settings.INVENTORY_FOLDER)
LOSSES_FOLDER = str(settings.LOSSES_FOLDER)


def verify_lost_stock_from_excel_combined(db: DatabaseManager):
    """
    Process loss files (ROTTURE, SCADUTO, UTILIZZO INTERNO) from LOSSES_FOLDER.
    
    UNCHANGED - works correctly as is.
    """
    EAN_COL = "EAN"
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
    by_settore = {}  # settore -> {loss_type -> [{cod, v, descrizione, qty}]}
    absent_eans = []  # EANs from CSV not found in products table

    for file_name, loss_type in LOSS_FILES.items():
        file_path = os.path.join(LOSSES_FOLDER, file_name)

        if not os.path.exists(file_path):
            logger.warning(f"Loss file not found: {file_name} (expected at {file_path})")
            continue

        logger.info(f"Processing loss file: {file_name} (type: {loss_type})")

        try:
            df = pd.read_csv(file_path, encoding='utf-8')

            logger.info(f"File loaded. Shape: {df.shape}, Columns: {df.columns.tolist()}")

            if EAN_COL not in df.columns:
                logger.error(f"Missing column '{EAN_COL}' in {file_name}. Available: {df.columns.tolist()}")
                continue

            if STOCK_COL not in df.columns:
                logger.error(f"Missing column '{STOCK_COL}' in {file_name}. Available: {df.columns.tolist()}")
                continue

            df[EAN_COL] = df[EAN_COL].astype(str).str.strip()

            def _parse_qty(s: str) -> str:
                s = s.strip()
                # Only strip dots when a comma is also present (Italian thousands separator).
                # Without a comma, a dot is a decimal point — "12.0" must stay "12.0", not become "120".
                if ',' in s:
                    return s.replace('.', '').replace(',', '.')
                return s

            df[STOCK_COL] = df[STOCK_COL].astype(str).map(_parse_qty)
            df[STOCK_COL] = pd.to_numeric(df[STOCK_COL], errors='coerce')

            initial_rows = len(df)
            df = df.dropna(subset=[EAN_COL, STOCK_COL])
            df = df[df[EAN_COL] != '']
            dropped_rows = initial_rows - len(df)
            if dropped_rows > 0:
                logger.warning(f"Dropped {dropped_rows} invalid rows from {file_name}")

            df[STOCK_COL] = df[STOCK_COL].astype(float)

            combined = (
                df.groupby(EAN_COL, as_index=False)[STOCK_COL]
                .sum()
            )

            logger.info(f"After combining duplicates: {len(combined)} unique EANs")

            processed_count = 0
            absent_count = 0
            error_count = 0

            for _, row in combined.iterrows():
                ean = row[EAN_COL]
                delta = int(row[STOCK_COL])

                if delta == 0:
                    continue

                product = db.get_cod_v_by_ean(ean)
                if product is None:
                    logger.info(f"  EAN {ean} x{delta} — not found in database (skipped)")
                    absent_count += 1
                    absent_eans.append({'ean': ean, 'qty': delta, 'loss_type': loss_type})
                    continue

                cod = product['cod']
                v = product['v']
                settore = product['settore']
                descrizione = product['descrizione']

                try:
                    db.register_losses(cod, v, delta, loss_type)
                    processed_count += 1
                    total_losses += delta
                    logger.info(f"  {loss_type}: EAN {ean} ({descrizione}) x{delta}")

                    by_settore.setdefault(settore, {}).setdefault(loss_type, []).append({
                        'cod': cod, 'v': v, 'descrizione': descrizione, 'qty': delta
                    })
                except ValueError as e:
                    logger.info(f"  EAN {ean} ({cod}.{v}) — not in product_stats (skipped)")
                    absent_count += 1
                except Exception as e:
                    logger.warning(f"Error processing EAN {ean} ({cod}.{v}): {type(e).__name__}: {e}")
                    error_count += 1

            logger.info(f"Processed {file_name}: {processed_count} losses registered, {absent_count} skipped, {error_count} errors")
            files_processed += 1

            try:
                os.remove(file_path)
                logger.info(f"Deleted processed file: {file_path}")
            except Exception as e:
                logger.error(f"Could not delete file {file_path}: {e}")

        except Exception as e:
            logger.exception(f" Error reading or processing file {file_name}")
            continue

    logger.info(f"Loss processing complete: {files_processed} files processed, {total_losses} total units of losses registered")

    total_unique_products = sum(
        len(items)
        for types in by_settore.values()
        for items in types.values()
    )

    return {
        'success': True,
        'files_processed': files_processed,
        'total_losses': total_losses,
        'total_unique_products': total_unique_products,
        'by_settore': by_settore,
        'absent_eans': absent_eans,
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


def process_loss_csv_dropzone(db: DatabaseManager, csv_path: str, loss_type: str):
    """
    Process a raw Dropzone loss CSV export.
    Expected columns: 'Cod. Barre' (EAN), 'Originale' (qty).
    Multiple rows with the same EAN are summed before registering.
    """
    EAN_COL = "Cod. Barre"
    QTY_COL = "Originale"

    try:
        df = pd.read_csv(csv_path, encoding='utf-8', skipinitialspace=True)
    except Exception as e:
        return {'success': False, 'error': f"Could not read CSV: {e}"}

    if EAN_COL not in df.columns:
        return {'success': False, 'error': f"Missing column '{EAN_COL}'. Columns found: {df.columns.tolist()}"}
    if QTY_COL not in df.columns:
        return {'success': False, 'error': f"Missing column '{QTY_COL}'. Columns found: {df.columns.tolist()}"}

    df[EAN_COL] = df[EAN_COL].astype(str).str.strip()
    df[QTY_COL] = pd.to_numeric(df[QTY_COL], errors='coerce')
    df = df.dropna(subset=[EAN_COL, QTY_COL])
    df = df[df[EAN_COL] != '']

    combined = df.groupby(EAN_COL, as_index=False)[QTY_COL].sum()

    processed_count = 0
    absent_count = 0
    error_count = 0
    total_losses = 0
    absent_eans = []

    for _, row in combined.iterrows():
        ean = str(row[EAN_COL]).strip()
        delta = int(row[QTY_COL])
        if delta == 0:
            continue

        product = db.get_cod_v_by_ean(ean)
        if product is None:
            logger.info(f"EAN {ean} x{delta} — not found in database (skipped)")
            absent_count += 1
            absent_eans.append({'ean': ean, 'qty': delta})
            continue

        try:
            db.register_losses(product['cod'], product['v'], delta, loss_type)
            logger.info(f"  {loss_type}: EAN {ean} ({product['descrizione']}) x{delta}")
            processed_count += 1
            total_losses += delta
        except Exception as e:
            logger.warning(f"Error registering loss for EAN {ean}: {e}")
            error_count += 1

    return {
        'success': True,
        'processed': processed_count,
        'absent': absent_count,
        'errors': error_count,
        'total_losses': total_losses,
        'absent_eans': absent_eans,
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
