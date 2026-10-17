# LamApp/supermarkets/supply_checks.py
"""
Why a product ran short, shown as badges on "Articoli esauriti" and on the calibration
report: the supplier could not provide it, its delivery came without it or short, or
its linked twin was covering for it.

Everything is judged as of a date `before`: orders in the WINDOW_DAYS before it, and
deliveries strictly before it.
"""
import logging
from collections import defaultdict
from datetime import date, timedelta

from django.utils import timezone

from .models import ProductLink, RestockLog, RestockSchedule
from .scripts.DatabaseManager import DatabaseManager

logger = logging.getLogger(__name__)

WINDOW_DAYS = 7


def link_partners(supermarket):
    """({product: its partner}, {product: the role its partner plays}) for every ProductLink."""
    partner, twin_role = {}, {}
    for primary, secondary in ProductLink.build_pairs(supermarket):
        partner[primary], partner[secondary] = secondary, primary
        twin_role[primary], twin_role[secondary] = 'Sostituito', 'Subentrante'
    return partner, twin_role


def supplier_unavailable(storage, before):
    """
    {(cod, v): reason} for products the supplier could not provide in an order run of
    the WINDOW_DAYS before `before`: unavailable on the list at order time, or refused
    by Dropzone on sending. The latest run wins.
    """
    runs = RestockLog.objects.filter(
        storage=storage,
        operation_type='full_restock',
        started_at__date__gte=before - timedelta(days=WINDOW_DAYS),
        started_at__date__lt=before,
    ).exclude(results='').only('results', 'started_at').order_by('started_at')

    reasons = {}
    for run in runs:
        day = timezone.localdate(run.started_at).strftime('%d/%m')
        results = run.get_results()
        for cod, v in results.get('unavailable_at_order', []):
            reasons[(cod, v)] = f"Non disponibile a listino all'ordine del {day}"
        for p in results.get('order_skipped_products', []):
            reasons[(p['cod'], p['var'])] = f"Rifiutato da Dropzone all'invio dell'ordine del {day}"
    return reasons


def delivery_shortfalls(storage, db, partner, before):
    """
    {(cod, v): {ordered_on, delivery, colli, expected, arrived}} for products the latest
    delivery meant to bring them brought short or not at all. Compared are the colli
    sent to Dropzone for that delivery date (refused rows excluded) and the pieces on
    that date's DDTs, a linked partner's included. A delivery is judged only when a DDT
    for it is on record. `expected` is None when the pack size is unknown: only a total
    absence counts then.
    """
    try:
        schedule = storage.schedule
    except RestockSchedule.DoesNotExist:
        return {}

    sent = RestockLog.objects.filter(
        storage=storage,
        operation_type='full_restock',
        status='completed',
        order_executed_at__date__gte=before - timedelta(days=WINDOW_DAYS),
    ).exclude(results='').only('results', 'order_executed_at').order_by('-order_executed_at')

    # Two order days can share a delivery day, so colli are summed per delivery
    ordered = defaultdict(dict)
    ordered_on = {}
    for log in sent:
        day = timezone.localdate(log.order_executed_at)
        delivery = schedule.delivery_date_for_order(day)
        if delivery >= before:
            continue
        results = log.get_results()
        refused = {(p['cod'], p['var']) for p in results.get('order_skipped_products', [])}
        for o in results.get('orders', []):
            p = (o['cod'], o['var'])
            if not o.get('qty') or p in refused:
                continue
            ordered[delivery][p] = ordered[delivery].get(p, 0) + int(o['qty'])
            ordered_on.setdefault((delivery, p), day)

    delivered = db.delivered_quantities(storage.settore, set(ordered))
    keys = {p for products in ordered.values() for p in products}
    info = db.products_by_key(keys | {partner[p] for p in keys if p in partner})

    def rapp(p):
        return (info.get(p) or {}).get('rapp') or 1

    shortfalls, judged = {}, set()
    for delivery in sorted(ordered, reverse=True):
        got = delivered.get(delivery)
        for p, colli in ordered[delivery].items():
            if p in judged:
                continue
            judged.add(p)
            if got is None:
                continue
            arrived = sum(got.get(k, 0) * rapp(k) for k in (p, partner.get(p)) if k)
            pack = info.get(p) or {}
            expected = colli * pack['pz_x_collo'] * pack['rapp'] if pack.get('pz_x_collo') and pack.get('rapp') else None
            if arrived > 0 and (expected is None or arrived >= expected):
                continue
            shortfalls[p] = {'ordered_on': ordered_on[(delivery, p)], 'delivery': delivery,
                             'colli': colli, 'expected': expected, 'arrived': arrived}
    return shortfalls


def report_badges(report):
    """
    {(cod, v): {supplier_unavailable, shortfall, twin}} for the critical and understocked
    products of a calibration report, as of its own day: it grades stock just before
    that day's delivery, and a twin counts with its stock at that moment.
    """
    results = report.get_results()
    storage = report.storage
    before = timezone.localdate(report.generated_at)
    partner, twin_role = link_partners(storage.supermarket)

    stored = results.get('shortfalls')
    if stored is not None:
        shortfalls = shortfalls_from_json(stored)
    else:
        # A report saved without them: worked out now, as far back as the DDT ledger reaches
        shortfalls = {}
        try:
            db = DatabaseManager(supermarket_name=storage.supermarket.name)
            try:
                shortfalls = delivery_shortfalls(storage, db, partner, before)
            finally:
                db.close()
        except Exception:
            logger.exception(f"Delivery shortfalls failed for calibration report {report.pk}")

    unavailable = supplier_unavailable(storage, before)
    graded = {
        (p['cod'], p['v']): p
        for outcome in ('critical', 'understocked', 'overstocked', 'ok')
        for p in results.get(outcome, [])
    }
    badges = {}
    for outcome in ('critical', 'understocked'):
        for p in results.get(outcome, []):
            key = (p['cod'], p['v'])
            twin = graded.get(partner.get(key))
            badges[key] = {
                'supplier_unavailable': unavailable.get(key),
                'shortfall': shortfalls.get(key),
                'twin': dict(twin, role=twin_role[key]) if twin and (twin.get('stock') or 0) > 0 else None,
            }
    return badges


def shortfalls_to_json(shortfalls):
    """Shortfalls keyed "cod.v" with ISO dates, to store inside a report."""
    return {
        f"{cod}.{v}": dict(s, ordered_on=s['ordered_on'].isoformat(), delivery=s['delivery'].isoformat())
        for (cod, v), s in shortfalls.items()
    }


def shortfalls_from_json(stored):
    return {
        tuple(int(x) for x in key.split('.')): dict(
            s, ordered_on=date.fromisoformat(s['ordered_on']), delivery=date.fromisoformat(s['delivery']))
        for key, s in stored.items()
    }
