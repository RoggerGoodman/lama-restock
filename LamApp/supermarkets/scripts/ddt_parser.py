# LamApp/supermarkets/scripts/ddt_parser.py
"""
Parser for DDT (Documento Di Trasporto) delivery documents.
Extracts product codes and quantities from PDF files.
"""
import pdfplumber
import re
import logging

logger = logging.getLogger(__name__)


def parse_ddt_pdf(pdf_path):
    """
    Parse DDT PDF and extract product deliveries.

    Args:
        pdf_path: Path to DDT PDF file

    Returns:
        list: List of tuples (cod, var, qty)
    """
    line_regex = re.compile(
        r"""
        ^\s*
        (?P<cod>\d+\.\d+)      # CODICE ARTICOLO
        \s+.+?\s+
        PZ\s+
        \d+\s+                 # colli
        \d+\s+                 # pezzi per collo
        (?P<qty>[\d,]+)        # QUANTITA' (allow comma)
        \s+[\d,]+              # prezzo unitario
        \s+[\d,]+              # totale
        """,
        re.VERBOSE
    )

    results = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"Parsing DDT PDF: {pdf_path} ({len(pdf.pages)} pages)")

            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                if not text:
                    logger.warning(f"No text found on page {page_num}")
                    continue

                for line in text.splitlines():
                    match = line_regex.search(line)
                    if not match:
                        continue

                    try:
                        full_code = match.group("cod")
                        qty_str = match.group("qty")

                        # ❌ Skip KG-based products (e.g. "3,5")
                        if "," in qty_str:
                            logger.debug(f"Skipping KG product line: {line}")
                            continue

                        cod_str, v_str = full_code.split(".")
                        cod = int(cod_str)
                        var = int(v_str)
                        qty = int(qty_str)

                        results.append((cod, var, qty))
                        logger.debug(f"Extracted: {cod}.{var} = {qty}")

                    except (ValueError, AttributeError) as e:
                        logger.warning(f"Could not parse line: {line} ({e})")

            logger.info(f"DDT parsing complete: found {len(results)} products")
            return results

    except Exception:
        logger.exception(f"Error parsing DDT PDF: {pdf_path}")
        raise


def process_ddt_deliveries(db_manager, ddt_entries):
    """
    Process DDT entries and add stock to database.
    
    Args:
        db_manager: DatabaseManager instance
        ddt_entries: List of (cod, var, qty) tuples
        
    Returns:
        dict: Processing results
    """
    processed = 0
    added_stock = 0
    skipped = []
    errors = []
    
    logger.info(f"Processing {len(ddt_entries)} DDT entries")
    
    for cod, var, qty in ddt_entries:
        try:
            # Check if product exists
            try:
                current_stock = db_manager.get_stock(cod, var)
            except ValueError:
                logger.warning(f"Product {cod}.{var} not found in database")
                skipped.append({
                    'cod': cod,
                    'var': var,
                    'qty': qty,
                    'reason': 'Product not in database'
                })
                continue
            
            # Add quantity to stock
            db_manager.adjust_stock(cod, var, qty)
            new_stock = db_manager.get_stock(cod, var)
            
            logger.info(f"Added {qty} to {cod}.{var}: {current_stock} → {new_stock}")
            
            processed += 1
            added_stock += qty
            
        except Exception as e:
            logger.exception(f"Error processing {cod}.{var}")
            errors.append({
                'cod': cod,
                'var': var,
                'qty': qty,
                'error': str(e)
            })
            continue
    
    result = {
        'success': True,
        'processed': processed,
        'total_qty_added': added_stock,
        'skipped': len(skipped),
        'errors': len(errors),
        'skipped_products': skipped,
        'error_products': errors
    }
    
    logger.info(
        f"DDT processing complete: {processed} processed, "
        f"{len(skipped)} skipped, {len(errors)} errors"
    )
    
    return result