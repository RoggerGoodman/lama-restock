"""
Keep every store's ProductLinks in line with the chain-wide ChainProductLinks.

Per store:
  1. cleanup  - drop links whose sostituito is no longer active, purging the
                sostituito when the link has purge_on_removal
  2. apply    - add chain links whose sostituito is active, unless the store opted out
  3. verify   - verify unverified, orderable subentranti at their current stock

"Active" means in stock or sold within QUIET_DAYS. The add and remove rules
are exact opposites, so a removed link is never re-added while it stays quiet.
QUIET_DAYS also covers the forecast: the sostituito's history is merged into
the subentrante, and after ~4 half-lives (14d each) it no longer matters.
"""
import logging
from dataclasses import dataclass, field
from datetime import timedelta

from django.utils import timezone

from .demo import real_supermarkets
from .models import ChainLinkOptOut, ChainProductLink, ProductLink, ProductLinkNotification
from .scripts.DatabaseManager import DatabaseManager
from .services import delete_blacklist_entries_for_purged

logger = logging.getLogger(__name__)

QUIET_DAYS = 60


@dataclass
class StoreReport:
    supermarket: object
    removed: list = field(default_factory=list)   # [(primary, secondary)]
    added: list = field(default_factory=list)
    verified: list = field(default_factory=list)  # [(cod, v)]
    purged: list = field(default_factory=list)    # [(cod, v)]
    error: str = None

    def lines(self):
        """Human-readable summary, one line per change."""
        if self.error:
            return [f"FAILED: {self.error}"]
        out = [f"remove  {fmt_pair(p)}" for p in self.removed]
        out += [f"add     {fmt_pair(p)}" for p in self.added]
        out += [f"verify  {fmt(p)}" for p in self.verified]
        out += [f"purge   {fmt(p)}" for p in self.purged]
        return out


def fmt(product):
    return f"{product[0]}.{product[1]}"


def fmt_pair(pair):
    return f"{fmt(pair[0])} <- {fmt(pair[1])}"


def _pair(link):
    return (link.primary_cod, link.primary_v), (link.secondary_cod, link.secondary_v)


def _load_states(db, keys):
    """{(cod, v): {stock, verified, disponibilita, sales_sets}} for the products that exist."""
    if not keys:
        return {}
    cur = db.cursor()
    cur.execute("""
        SELECT p.cod, p.v, p.disponibilita, ps.stock, ps.verified, ps.sales_sets
        FROM products p
        LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
        WHERE (p.cod, p.v) IN %s
    """, (tuple(keys),))
    return {(r["cod"], r["v"]): r for r in cur.fetchall()}


def is_active(state):
    if state is None:
        return False
    if (state["stock"] or 0) > 0:
        return True
    recent = (state["sales_sets"] or [])[:QUIET_DAYS + 1]  # slot 0 is today
    return any(s is not None and s > 0 for s in recent)


def sync_store(supermarket, chain_links, cleanup=True, dry_run=False):
    """
    Bring one store in line with chain_links. With cleanup=False only the given
    chain links are applied and only their subentranti verified (used by
    link_products for newly added pairs).
    """
    report = StoreReport(supermarket)
    links = list(ProductLink.objects.filter(supermarket=supermarket))
    opted_out = set(
        ChainLinkOptOut.objects.filter(supermarket=supermarket).values_list('chain_link_id', flat=True)
    )

    keys = set()
    for link in links:
        keys.update(_pair(link))
    for cl in chain_links:
        keys.update(_pair(cl))

    db = DatabaseManager(supermarket_name=supermarket.name)
    try:
        states = _load_states(db, keys)

        kept = []
        purge_results = []
        for link in links:
            pair = _pair(link)
            if cleanup and not is_active(states.get(pair[1])):
                report.removed.append(pair)
                # Unsuppressed, a verified sostituito could be reordered from its monthly history
                state = states.get(pair[1])
                purge = link.purge_on_removal and state is not None and state["verified"] is not None
                if purge:
                    report.purged.append(pair[1])
                if not dry_run:
                    link.delete()
                    if purge:
                        purge_results.append(db.purge_product(*pair[1]))
            else:
                kept.append(pair)
        if purge_results:
            delete_blacklist_entries_for_purged(purge_results, supermarket=supermarket)

        used = {product for pair in kept for product in pair}
        for cl in chain_links:
            primary, secondary = _pair(cl)
            if cl.id is not None and cl.id in opted_out:
                continue
            if primary in used or secondary in used:
                continue
            if not is_active(states.get(secondary)):
                continue
            report.added.append((primary, secondary))
            used.update((primary, secondary))
            if not dry_run:
                fields = dict(
                    supermarket=supermarket,
                    primary_cod=primary[0], primary_v=primary[1],
                    secondary_cod=secondary[0], secondary_v=secondary[1],
                    created_by=cl.created_by,
                )
                ProductLink.objects.create(notes=cl.notes, purge_on_removal=cl.purge_on_removal, **fields)
                ProductLinkNotification.objects.create(**fields)

        to_verify = report.added + (kept if cleanup else [])
        for primary, _ in to_verify:
            state = states.get(primary)
            if state is None or state["verified"] or state["disponibilita"] == "No":
                continue
            report.verified.append(primary)
            if not dry_run:
                db.verify_stock(primary[0], primary[1], max(state["stock"] or 0, 0))
    finally:
        db.close()

    return report


def retire_chain_links(dry_run=False):
    """
    Delete chain links older than QUIET_DAYS that no store carries any more.
    Returns the retired links.
    """
    cutoff = timezone.now() - timedelta(days=QUIET_DAYS)
    stores = real_supermarkets()
    retired = []
    for cl in ChainProductLink.objects.filter(created_at__lt=cutoff):
        in_use = ProductLink.objects.filter(
            supermarket__in=stores,
            primary_cod=cl.primary_cod, primary_v=cl.primary_v,
            secondary_cod=cl.secondary_cod, secondary_v=cl.secondary_v,
        ).exists()
        if not in_use:
            retired.append(cl)
            if not dry_run:
                cl.delete()
    return retired


def sync_all(dry_run=False):
    """Full pass over every real store, then retire unused chain links."""
    chain_links = list(ChainProductLink.objects.select_related('created_by'))
    reports = []
    for sm in real_supermarkets().order_by('name'):
        try:
            reports.append(sync_store(sm, chain_links, cleanup=True, dry_run=dry_run))
        except Exception as e:
            logger.exception(f"[CHAIN LINKS] Sync failed for {sm.name}")
            reports.append(StoreReport(sm, error=str(e)))
    retired = [] if any(r.error for r in reports) else retire_chain_links(dry_run=dry_run)
    return reports, retired
