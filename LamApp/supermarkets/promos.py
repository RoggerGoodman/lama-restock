"""
Chain-wide promo loading: apply a parsed promo PDF to every real store, either
from the upload_promos_all command or from a "Promo" email (see admin_mailbox.py).
"""
import logging
import os
import tempfile

from .demo import real_supermarkets
from .scripts.DatabaseManager import DatabaseManager
from .scripts.helpers import Helper

logger = logging.getLogger(__name__)


def apply_promo_list(promo_list):
    """
    Write parse_promo_pdf output to every real store (Rione stores: RIONE rows only).
    Returns [(supermarket, rows_sent, rows_matched, error)].
    """
    results = []
    for sm in real_supermarkets().order_by("name"):
        store_list = Helper.promos_for_store(promo_list, sm.is_rione)
        try:
            db = DatabaseManager(supermarket_name=sm.name)
            try:
                matched = db.update_promos(store_list)
            finally:
                db.close()
            results.append((sm, len(store_list), matched, None))
        except Exception as e:
            logger.exception(f"[PROMO] Failed to apply promos to {sm.name}")
            results.append((sm, len(store_list), 0, str(e)))
    return results


def load_promo_pdfs(pdfs):
    """
    Apply each (filename, bytes) PDF to every store. Raises if a file has no promo
    rows or a store fails, so the email stays in the inbox and is retried.
    """
    for filename, payload in pdfs:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(payload)
            path = f.name
        try:
            promo_list = Helper.parse_promo_pdf(path)
        finally:
            os.remove(path)
        if not promo_list:
            raise ValueError(f"no promo rows found in {filename}")
        results = apply_promo_list(promo_list)
        failed = [sm.name for sm, _, _, error in results if error]
        logger.info(
            f"[PROMO MAIL] {filename}: {len(promo_list)} rows "
            f"({sum(1 for r in promo_list if r[6])} RIONE) applied to "
            f"{len(results) - len(failed)} store(s)"
            + (f", failed: {', '.join(failed)}" if failed else "")
        )
        if failed:
            raise RuntimeError(f"stores failed: {', '.join(failed)}")
