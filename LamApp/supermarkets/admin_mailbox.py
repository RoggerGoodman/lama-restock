"""
Nightly read of the admin mailbox: a Gmail account read over IMAP with an app
password (PROMO_IMAP_USER / PROMO_IMAP_PASSWORD in .env.production).

Inbox emails are handled by subject (exact, any case):
  "Promo"        - PDF attachments applied to every store (promos.load_promo_pdfs)
  "Sostituzione" - "cod.v = cod.v" lines (subentrante = sostituito) added to the
                   chain list; the 03:45 sync_chain_product_links task applies them
A handled email is labelled PROCESSED_LABEL and archived. One that fails stays in
the inbox and is retried the next night; other subjects are left untouched.

Before every order, refresh_links_before_order also picks up "Sostituzione"
emails and applies the chain list to that store, so an order never runs with
a substitution still unread.
"""
import email
import html
import imaplib
import logging
import os
import re
from contextlib import contextmanager
from email.header import decode_header, make_header

from django.conf import settings
from django.utils import timezone

from .chain_links import add_to_chain_list, fmt_pair, parse_email_pairs, sync_store
from .demo import real_supermarkets
from .models import ChainProductLink
from .promos import load_promo_pdfs

logger = logging.getLogger(__name__)

IMAP_HOST = "imap.gmail.com"
PROCESSED_LABEL = "Caricate"

# One mailbox, read by the nightly task and by every order: one reader at a time
LOCK_NAME = "admin-mailbox"
PRE_ORDER_IMAP_TIMEOUT = 10   # seconds per Gmail operation; an order never waits longer
PRE_ORDER_LOCK_WAIT = 60      # seconds an order waits for another reader to finish


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


@contextmanager
def mailbox_lock(wait):
    """Yields True once the lock is held, False if it could not be had within `wait` seconds."""
    import redis
    client = redis.Redis.from_url(getattr(settings, "CELERY_BROKER_URL", "redis://localhost:6379/0"))
    lock = client.lock(LOCK_NAME, timeout=600, blocking_timeout=wait)  # expires if a worker dies
    acquired = lock.acquire()
    try:
        yield acquired
    finally:
        if acquired:
            try:
                lock.release()
            except Exception:
                pass


def refresh_links_before_order(supermarket):
    """
    Read pending "Sostituzione" emails, then add any chain link that applies to
    this store. Never raises: on any failure the order runs with the links it has.
    """
    if not real_supermarkets().filter(pk=supermarket.pk).exists():
        return
    try:
        with mailbox_lock(PRE_ORDER_LOCK_WAIT) as acquired:
            if not acquired:
                logger.warning(f"[MAILBOX] Pre-order link check skipped for {supermarket.name}: mailbox busy")
                return
            try:
                import_mailbox(subjects={"sostituzione"}, timeout=PRE_ORDER_IMAP_TIMEOUT)
            except Exception:
                logger.warning(f"[MAILBOX] Pre-order mailbox check failed for {supermarket.name}", exc_info=True)
            chain_links = list(ChainProductLink.objects.select_related("created_by"))
            report = sync_store(supermarket, chain_links, cleanup=False)
            for line in report.lines():
                logger.info(f"[CHAIN LINKS] {supermarket.name} (pre-order): {line}")
    except Exception:
        logger.exception(f"[MAILBOX] Pre-order link refresh failed for {supermarket.name}")


def import_mailbox(subjects=None, timeout=None):
    """
    Handle every pending email, or only those whose subject is in `subjects`.
    Returns how many were handled.
    """
    handlers = HANDLERS if subjects is None else {k: HANDLERS[k] for k in subjects}
    user = os.environ.get("PROMO_IMAP_USER")
    password = os.environ.get("PROMO_IMAP_PASSWORD")
    if not user or not password:
        logger.warning("[MAILBOX] PROMO_IMAP_USER / PROMO_IMAP_PASSWORD not set, skipping")
        return 0

    handled = 0
    imap = imaplib.IMAP4_SSL(IMAP_HOST, timeout=timeout)
    try:
        imap.login(user, password.replace(" ", ""))
        imap.create(PROCESSED_LABEL)  # answers NO if it already exists
        imap.select("INBOX")
        _, data = imap.search(None, "ALL")
        for num in data[0].split():
            # Headers first: the inbox may hold unrelated mail with large attachments
            _, head = imap.fetch(num, "(BODY.PEEK[HEADER.FIELDS (SUBJECT)])")
            handler = handlers.get(_subject(email.message_from_bytes(head[0][1])).lower())
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
