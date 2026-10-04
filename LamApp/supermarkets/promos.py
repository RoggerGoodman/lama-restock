"""
Chain-wide promo loading: apply a parsed promo PDF to every real store, either
from the upload_promos_all command or from the PROMO_IMAP mailbox.

Mailbox: every inbox message whose subject is exactly "Promo" has its PDF
attachments applied, then is archived under the PROCESSED_LABEL label.
Credentials come from PROMO_IMAP_USER / PROMO_IMAP_PASSWORD (a Gmail app password).
"""
import email
import imaplib
import logging
import os
import tempfile
from email.header import decode_header, make_header

from .demo import real_supermarkets
from .scripts.DatabaseManager import DatabaseManager
from .scripts.helpers import Helper

logger = logging.getLogger(__name__)

IMAP_HOST = "imap.gmail.com"
PROMO_SUBJECT = "promo"
PROCESSED_LABEL = "Caricate"


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


def _subject(msg):
    return str(make_header(decode_header(msg.get("Subject", "")))).strip()


def _pdf_attachments(msg):
    for part in msg.walk():
        filename = part.get_filename()
        if filename:
            filename = str(make_header(decode_header(filename)))
        is_pdf = part.get_content_type() == "application/pdf" or (
            filename and filename.lower().endswith(".pdf")
        )
        if is_pdf and part.get_payload(decode=True):
            yield filename or "promo.pdf", part.get_payload(decode=True)


def import_promo_emails():
    """Apply every pending "Promo" email. Returns how many emails were loaded."""
    user = os.environ.get("PROMO_IMAP_USER")
    password = os.environ.get("PROMO_IMAP_PASSWORD")
    if not user or not password:
        logger.warning("[PROMO MAIL] PROMO_IMAP_USER / PROMO_IMAP_PASSWORD not set, skipping")
        return 0

    loaded = 0
    imap = imaplib.IMAP4_SSL(IMAP_HOST)
    try:
        imap.login(user, password.replace(" ", ""))
        imap.create(PROCESSED_LABEL)  # answers NO if it already exists
        imap.select("INBOX")
        _, data = imap.search(None, '(SUBJECT "Promo")')  # substring match, refined below
        for num in data[0].split():
            _, msg_data = imap.fetch(num, "(BODY.PEEK[])")
            msg = email.message_from_bytes(msg_data[0][1])
            subject = _subject(msg)
            if subject.lower() != PROMO_SUBJECT:
                continue

            pdfs = list(_pdf_attachments(msg))
            if not pdfs:
                logger.warning(f"[PROMO MAIL] '{subject}' from {msg.get('From')} has no PDF, left in inbox")
                continue

            try:
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
            except Exception:
                # Left in the inbox: retried next night (re-applying is harmless)
                logger.exception(f"[PROMO MAIL] Could not load '{subject}' from {msg.get('From')}")
                continue

            # Label and archive (Gmail archives on expunge from INBOX)
            imap.store(num, "+X-GM-LABELS", PROCESSED_LABEL)
            imap.store(num, "+FLAGS", "\\Deleted")
            loaded += 1
        imap.expunge()
    finally:
        try:
            imap.logout()
        except Exception:
            pass
    return loaded
