# LamApp/supermarkets/document_import.py
"""
Hourly import of Dropzone documents: DDTs (BOL) become deliveries booked on the
day the goods arrive; credit notes (NAC) become CreditNotes awaiting approval.

Every document is recorded once in the per-supermarket `dropzone_documents`
ledger under "type-year-series-number", so re-reading the same 7-day window
every hour never imports anything twice.
"""
import logging
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from itertools import product

from django.db import IntegrityError, transaction
from django.utils import timezone

from .models import CreditNote, CreditNoteLine, RestockLog, RestockSchedule
from .scripts.DatabaseManager import DatabaseManager
from .scripts.dropzone_client import DropzoneClient

logger = logging.getLogger(__name__)

WINDOW_DAYS = 7
# Pruning a document still inside the import window would make it look new and book it
# twice, so the ledger keeps the window plus a day. Older DDT numbers stay in the import logs.
LEDGER_RETENTION_DAYS = WINDOW_DAYS + 1
IMPORTED_TYPES = ('BOL', 'NAC')
# Header and line values agree to the cent once a document is complete
TOTAL_TOLERANCE = 0.05
# The old 05:00 import's last retry ends around 06:30; the cutover never seeds before this
CUTOVER_EARLIEST_HOUR = 7
# Weeks of DDT dates the processing table is learned from
HISTORY_WEEKS = 5
# A DDT weekday is regular when it shows up in at least this share of those weeks
REGULAR_SHARE = 0.6
# A stored table is relearned after this long, or as soon as the agenda's order days change
TABLE_MAX_AGE_DAYS = 7
SUNDAY = 6


def doc_key(header) -> str:
    return f"{header['X5CNAT']}-{header['X5NBAA']}-{header['X5NRCD']}-{header['X5NRCC']}"


def _parse_ymd(value) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def _qty(value) -> int:
    return int(round(float(value or 0)))


def cutover_covered_through(last_old_log_date, today) -> date:
    """
    Newest DDT date the old once-a-day import can have booked: it read only the day
    before its run. No log at all (a new supermarket) still leaves earlier history alone.
    """
    yesterday = today - timedelta(days=1)
    if last_old_log_date is None:
        return yesterday
    return min(last_old_log_date - timedelta(days=1), yesterday)


def regular_ddt_weekdays(ddt_dates, weeks=HISTORY_WEEKS) -> set:
    seen = defaultdict(set)
    for d in ddt_dates:
        seen[d.weekday()].add(d.isocalendar()[:2])
    return {wd for wd, w in seen.items() if len(w) >= REGULAR_SHARE * weeks}


def learn_processing_table(order_days, ddt_weekdays):
    """
    {DDT weekday: order weekday}, or None when the history allows more than one reading.

    A DDT is dated the day the warehouse processes the order, which is the day it was
    sent or the next working day, depending on a cutoff that differs per warehouse and
    store. Warehouses never process on Sunday. Each order day therefore owns exactly one
    regular DDT weekday, and only one assignment is accepted.
    """
    if not order_days:
        return None
    options = []
    for o in order_days:
        following = 0 if o + 1 >= SUNDAY else o + 1
        candidates = {following} if o == SUNDAY else {o, following}
        options.append([p for p in candidates if p in ddt_weekdays] or [None])
    fits = []
    for combo in product(*options):
        mapped = [p for p in combo if p is not None]
        if len(mapped) == len(set(mapped)):
            fits.append(combo)
    if len(fits) != 1:
        return None
    return {p: o for p, o in zip(fits[0], order_days) if p is not None}


def group_documents(header_rows) -> dict:
    """Headers come one row per (document, reparto); fold them into documents."""
    docs = {}
    for row in header_rows:
        if row["X5CNAT"] not in IMPORTED_TYPES:
            continue
        key = doc_key(row)
        doc = docs.setdefault(key, {"header": row, "total": 0.0})
        doc["total"] += float(row["X5VI01"] or 0)
    return docs


class DocumentImporter:

    def __init__(self, supermarket):
        self.supermarket = supermarket
        self.storages = list(supermarket.storages.all())
        self.today = date.today()
        self.summary = {'new': 0, 'pending': 0, 'credit_notes': 0, 'skipped': 0, 'incomplete': 0, 'failed': 0}
        self._ddt_history = None
        self._tables = {}

    def run(self) -> dict:
        client = DropzoneClient(self.supermarket.username, self.supermarket.password)
        client.login()

        x5cper = self.supermarket.x5cper or client.fetch_x5cper()
        self.client, self.x5cper = client, x5cper
        id_cliente = self.supermarket.id_cliente or int(client.fetch_client()["value"])
        self.storage_by_code = self._map_warehouses(client.fetch_warehouses(id_cliente))

        date_from = (self.today - timedelta(days=WINDOW_DAYS)).strftime("%Y%m%d")
        docs = group_documents(client.fetch_document_headers(x5cper, date_from, self.today.strftime("%Y%m%d")))

        db = DatabaseManager(supermarket_name=self.supermarket.name)
        try:
            if not db.has_document_ledger():
                if not self._cutover_safe_now():
                    return self.summary
                db.ensure_document_ledger()
                self._seed_cutover(db, docs)

            known = db.known_document_keys(docs.keys())
            for key in sorted(k for k in docs if k not in known):
                try:
                    self._import(db, client, key, docs[key])
                except Exception:
                    # Not recorded, so the next hourly run retries it
                    self.summary['failed'] += 1
                    logger.exception(f"[DOCS] {self.supermarket.name}: failed on {key}")
        finally:
            db.close()

        logger.info(f"[DOCS] {self.supermarket.name}: {self.summary}")
        return self.summary

    def _map_warehouses(self, warehouses) -> dict:
        """Warehouse code ("01", "22"...) → Storage, via the IDCodMag storage sync saves."""
        by_id = {s.id_cod_mag: s for s in self.storages if s.id_cod_mag is not None}
        by_name = {" ".join(s.name.split()).upper(): s for s in self.storages}
        mapping = {}
        for w in warehouses:
            if w["rebilling"]:
                continue
            storage = by_id.get(w["id_cod_mag"]) or by_name.get(" ".join(w["name"].split()).upper())
            if storage:
                mapping[w["code"]] = storage
        return mapping

    def _cutover_safe_now(self) -> bool:
        """
        The old import (05:00, retried up to 3 x 15 min) must be finished for good:
        a run of it landing after the cutover could book a DDT the new import books too.
        """
        if timezone.localtime().hour < CUTOVER_EARLIEST_HOUR:
            logger.info(f"[DOCS] {self.supermarket.name}: cutover waits until {CUTOVER_EARLIEST_HOUR}:00")
            return False
        if RestockLog.objects.filter(
            storage__supermarket=self.supermarket, operation_type='ddt_import', status='processing',
            started_at__gte=timezone.now() - timedelta(hours=6),
        ).exists():
            logger.warning(f"[DOCS] {self.supermarket.name}: a DDT import is still running, cutover postponed")
            return False
        return True

    def _seed_cutover(self, db, docs):
        """
        First run on a supermarket. The old import ran once a day at 05:00, read only the
        DDTs dated the day before, and created its log before booking anything. So if its
        newest log is from day X, it can only have booked DDTs dated X-1 or earlier: those
        become legacy and are never booked; anything later it provably never saw.

        The DDT numbers inside its logs are NOT trusted for this: a one-line DDT was booked
        but filed under no storage, and a number repeated across series was booked once.
        They only feed the warning for DDTs the old import may have missed.
        """
        logs = RestockLog.objects.filter(storage__supermarket=self.supermarket, operation_type='ddt_import')
        last = logs.order_by('-started_at').first()
        covered_through = cutover_covered_through(
            timezone.localdate(last.started_at) if last else None, self.today
        )

        # A DDT dated t could only be listed by the run of day t+1
        logged = set()
        for log in logs.filter(started_at__gte=timezone.now() - timedelta(days=WINDOW_DAYS + 3)):
            run_day = timezone.localdate(log.started_at)
            for number in log.get_results().get('invoices', []):
                logged.add((str(number).lstrip('0') or '0', run_day))

        legacy = []
        for key, doc in docs.items():
            h = doc["header"]
            doc_date = _parse_ymd(h["X5DDOC"])
            if h["X5CNAT"] == 'BOL' and doc_date <= covered_through:
                legacy.append((key, h["X5NRCD"].strip(), h["X5NRCC"].lstrip('0') or '0', doc_date))
        # The old import skipped a number it had already seen that day, whatever the series
        same_day_numbers = Counter((number, doc_date) for _, _, number, doc_date in legacy)

        seeded, maybe_missed = 0, []
        for key, series, number, doc_date in legacy:
            storage = self.storage_by_code.get(series)
            seeded += db.record_document(key, 'BOL', number, doc_date, 'legacy',
                                         settore=storage.settore if storage else None)
            listed = (number, doc_date + timedelta(days=1)) in logged
            if storage is not None and (not listed or same_day_numbers[(number, doc_date)] > 1):
                maybe_missed.append(f"{storage.name} DDT {number} del {doc_date:%d/%m}")

        logger.info(f"[DOCS] {self.supermarket.name}: ledger created, {seeded} DDTs dated up to "
                    f"{covered_through} marked legacy (old import's last log: "
                    f"{timezone.localtime(last.started_at) if last else 'none'})")
        if maybe_missed:
            # Usually a one-line DDT the old import booked without listing it; possibly one
            # published after its 05:00 run, which it never booked. Check, upload the PDF if so.
            logger.warning(f"[DOCS] {self.supermarket.name}: legacy DDTs not listed in the old "
                           f"import's logs, check they were loaded: {maybe_missed}")

    def _import(self, db, client, key, doc):
        h = doc["header"]
        doc_date = _parse_ymd(h["X5DDOC"])
        number = h["X5NRCC"].lstrip('0') or '0'
        lines = client.fetch_document_lines(h)

        # A document can be visible before all its lines are; wait for it to settle.
        # Older ones go through anyway, so a permanent mismatch can't block them forever.
        line_total = sum(float(l.get("UAVCES") or 0) for l in lines)
        if abs(abs(doc["total"]) - line_total) > TOTAL_TOLERANCE:
            if doc_date >= self.today - timedelta(days=1):
                self.summary['incomplete'] += 1
                logger.warning(f"[DOCS] {key}: lines total {line_total:.2f} vs header {abs(doc['total']):.2f}, retrying later")
                return
            logger.warning(f"[DOCS] {key}: lines total {line_total:.2f} vs header {abs(doc['total']):.2f}, importing anyway")

        self.summary['new'] += 1
        if h["X5CNAT"] == 'BOL':
            self._import_ddt(db, key, h, number, doc_date, lines)
        else:
            self._import_credit_note(db, key, number, doc_date, lines)

    def _import_ddt(self, db, key, h, number, doc_date, lines):
        # One DDT series per warehouse: the series is the warehouse code
        storage = self.storage_by_code.get(h["X5NRCD"].strip())
        if storage is None:
            self.summary['skipped'] += 1
            db.record_document(key, 'BOL', number, doc_date, 'skipped')
            logger.info(f"[DOCS] {key}: warehouse {h['X5NRCD']} has no storage, skipped")
            return

        if db.manual_ddt_claimed(storage.settore, number, since=doc_date - timedelta(days=WINDOW_DAYS)):
            db.record_document(key, 'BOL', number, doc_date, 'manual', settore=storage.settore)
            logger.info(f"[DOCS] {key}: already loaded by hand from its PDF, not booked again")
            return

        qty_by_product = defaultdict(lambda: {"qty": 0, "descrizione": ""})
        for l in lines:
            try:
                pair = (int(l["UACART"]), int(l["UACDAR"]))
            except (TypeError, ValueError):
                continue
            qty_by_product[pair]["qty"] += _qty(l.get("UAQESP"))
            qty_by_product[pair]["descrizione"] = (l.get("UAXART") or "").strip()

        delivery_date = self._delivery_date(db, key, storage, h["X5NRCD"].strip(), doc_date)
        payload = [{"cod": c, "v": v, "qty": p["qty"], "descrizione": p["descrizione"]}
                   for (c, v), p in qty_by_product.items()]
        db.record_document(key, 'BOL', number, doc_date, 'pending',
                           settore=storage.settore, delivery_date=delivery_date, lines=payload)
        if delivery_date > self.today:
            self.summary['pending'] += 1
        logger.info(f"[DOCS] {key} ({storage.name}): {len(payload)} products, delivery {delivery_date}")

    def _delivery_date(self, db, key, storage, series, doc_date):
        """
        The DDT date is the day the warehouse processed the order, not the day it was
        sent (the order date on its lines is that same processing day). The learned
        table says which agenda order day each DDT weekday belongs to; the agenda then
        says when that order lands. Extra orders ride the same truck as the regular one.
        """
        try:
            schedule = storage.schedule
        except RestockSchedule.DoesNotExist:
            logger.warning(f"[DOCS] {key} ({storage.name}): no agenda, booked on its DDT date {doc_date}")
            return doc_date

        table = self._processing_table(db, storage, schedule, series)
        order_day = (table or {}).get(doc_date.weekday())
        if order_day is not None:
            order_date = doc_date - timedelta(days=(doc_date.weekday() - order_day) % 7)
            return max(doc_date, schedule.delivery_date_for_order(order_date))

        # A holiday, an order sent at an unusual time, or a changed agenda. Server log
        # only: the client never sees this.
        delivery_days = {(o + schedule.get_delivery_offset(o)) % 7 for o in schedule.get_order_days()}
        fallback = next((doc_date + timedelta(days=i) for i in range(7)
                         if (doc_date + timedelta(days=i)).weekday() in delivery_days), doc_date)
        logger.warning(f"[DOCS] {key} ({storage.name}): DDT dated {doc_date:%a %d/%m} matches no agenda order "
                       f"day (table {table}), booked on the next delivery day {fallback}")
        return fallback

    def _processing_table(self, db, storage, schedule, series):
        if storage.pk in self._tables:
            return self._tables[storage.pk]

        order_days = schedule.get_order_days()
        agenda = ",".join(map(str, order_days))
        stored = db.get_processing_table(storage.settore)
        if stored and stored["agenda"] == agenda and (self.today - stored["learned_on"]).days < TABLE_MAX_AGE_DAYS:
            table = {int(p): o for p, o in stored["mapping"].items()} if stored["mapping"] is not None else None
        else:
            if self._ddt_history is None:
                since = self.today - timedelta(weeks=HISTORY_WEEKS)
                headers = self.client.fetch_document_headers(
                    self.x5cper, since.strftime("%Y%m%d"), (self.today - timedelta(days=1)).strftime("%Y%m%d"))
                self._ddt_history = defaultdict(set)
                for doc in group_documents(headers).values():
                    h = doc["header"]
                    if h["X5CNAT"] == 'BOL':
                        self._ddt_history[h["X5NRCD"].strip()].add(_parse_ymd(h["X5DDOC"]))
            ddt_weekdays = regular_ddt_weekdays(self._ddt_history[series])
            table = learn_processing_table(order_days, ddt_weekdays)
            db.save_processing_table(storage.settore, agenda, table, self.today)
            logger.info(f"[DOCS] {storage.name}: processing table learned {table} "
                        f"(order days {order_days}, regular DDT weekdays {sorted(ddt_weekdays)})")
        self._tables[storage.pk] = table
        return table

    def _import_credit_note(self, db, key, number, doc_date, lines):
        by_storage = defaultdict(list)
        for l in lines:
            storage = self.storage_by_code.get((l.get("UACMAG") or "").strip().zfill(2))
            if storage is not None:
                by_storage[storage].append(l)

        for storage, storage_lines in by_storage.items():
            pairs = set()
            for l in storage_lines:
                try:
                    pairs.add((int(l["UACART"]), int(l["UACDAR"])))
                except (TypeError, ValueError):
                    pass
            verified = db.verified_pairs(storage.settore, pairs)
            kept = [l for l in storage_lines
                    if (int(l["UACART"]), int(l["UACDAR"])) in verified and _qty(l.get("UAQESP")) > 0]
            if not kept:
                continue
            try:
                with transaction.atomic():
                    note, created = CreditNote.objects.get_or_create(
                        storage=storage, doc_key=key,
                        defaults={'number': number, 'doc_date': doc_date},
                    )
                    if created:
                        CreditNoteLine.objects.bulk_create([
                            CreditNoteLine(
                                credit_note=note,
                                cod=int(l["UACART"]),
                                v=int(l["UACDAR"]),
                                descrizione=(l.get("UAXART") or "").strip()[:255],
                                original_qty=_qty(l.get("UAQESP")),
                                qty=_qty(l.get("UAQESP")),
                                reason=(l.get("UACIMC") or "").strip()[:120],
                                reference=self._credit_reference(l),
                            )
                            for l in kept
                        ])
                        self.summary['credit_notes'] += 1
            except IntegrityError:
                # An overlapping run created it first
                pass

        db.record_document(key, 'NAC', number, doc_date, 'recorded')

    @staticmethod
    def _credit_reference(line) -> str:
        kind = (line.get("UACTDR") or "").strip()
        ref = (line.get("UACRID") or "").strip()
        if not ref:
            return ""
        when = line.get("UADDOC") or ""
        try:
            when = datetime.strptime(when, "%Y-%m-%d").strftime("%d/%m/%Y")
        except ValueError:
            pass
        return f"{kind} {ref} del {when}".strip()[:60]
