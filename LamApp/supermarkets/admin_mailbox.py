"""
Nightly read of the admin mailbox: a Gmail account read over IMAP with an app
password (PROMO_IMAP_USER / PROMO_IMAP_PASSWORD in .env.production).

Inbox emails are handled by subject (exact, any case):
  "Promo"        - PDF attachments applied to every store (promos.load_promo_pdfs)
  "Sostituzione" - "cod.v = cod.v" lines (subentrante = sostituito) added to the
                   chain list; the 03:45 sync_chain_product_links task applies them
A handled email is labelled PROCESSED_LABEL and archived. One that fails stays in
the inbox and is retried the next night; other subjects are left untouched.
"""
import email
import html
import imaplib
import logging
import os
import re
from email.header import decode_header, make_header

from django.utils import timezone

from .chain_links import add_to_chain_list, fmt_pair, parse_email_pairs
from .promos import load_promo_pdfs

logger = logging.getLogger(__name__)

IMAP_HOST = "imap.gmail.com"
PROCESSED_LABEL = "Caricate"


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


def _body_text(msg):
    """The plain-text body; falls back to the HTML body with tags stripped."""
    html_body = None
    for part in msg.walk():
        if part.get_content_maintype() != "text" or part.get_filename():
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
        if part.get_content_type() == "text/plain":
            return text
        if part.get_content_type() == "text/html" and html_body is None:
            html_body = text
    if html_body is None:
        return ""
    html_body = re.sub(r"(?i)<br\s*/?>|</(p|div|li|tr)>", "\n", html_body)
    return html.unescape(re.sub(r"<[^>]+>", "", html_body))


def handle_promo(msg):
    pdfs = list(_pdf_attachments(msg))
    if not pdfs:
        raise ValueError("no PDF attached")
    load_promo_pdfs(pdfs)


def handle_sostituzione(msg):
    pairs = parse_email_pairs(_body_text(msg))
    notes = f"Email del {timezone.localdate():%d/%m/%Y}"
    results = add_to_chain_list([(pri, sec, notes, None, False) for pri, sec in pairs])
    for pair, link, skip_reason in results:
        if link is None:
            logger.info(f"[LINK MAIL] skip {fmt_pair(pair)}: {skip_reason}")
        else:
            logger.info(f"[LINK MAIL] added {fmt_pair(pair)} to the chain list")


HANDLERS = {
    "promo": handle_promo,
    "sostituzione": handle_sostituzione,
}


def import_mailbox():
    """Handle every pending email. Returns how many were handled."""
    user = os.environ.get("PROMO_IMAP_USER")
    password = os.environ.get("PROMO_IMAP_PASSWORD")
    if not user or not password:
        logger.warning("[MAILBOX] PROMO_IMAP_USER / PROMO_IMAP_PASSWORD not set, skipping")
        return 0

    handled = 0
    imap = imaplib.IMAP4_SSL(IMAP_HOST)
    try:
        imap.login(user, password.replace(" ", ""))
        imap.create(PROCESSED_LABEL)  # answers NO if it already exists
        imap.select("INBOX")
        _, data = imap.search(None, "ALL")
        for num in data[0].split():
            # Headers first: the inbox may hold unrelated mail with large attachments
            _, head = imap.fetch(num, "(BODY.PEEK[HEADER.FIELDS (SUBJECT)])")
            handler = HANDLERS.get(_subject(email.message_from_bytes(head[0][1])).lower())
            if handler is None:
                continue
            _, msg_data = imap.fetch(num, "(BODY.PEEK[])")
            msg = email.message_from_bytes(msg_data[0][1])
            try:
                handler(msg)
            except Exception:
                logger.exception(f"[MAILBOX] Could not handle '{_subject(msg)}' from {msg.get('From')}, left in inbox")
                continue
            imap.store(num, "+X-GM-LABELS", PROCESSED_LABEL)
            imap.store(num, "+FLAGS", "\\Deleted")  # Gmail archives on expunge from INBOX
            handled += 1
        imap.expunge()
    finally:
        try:
            imap.logout()
        except Exception:
            pass
    return handled
