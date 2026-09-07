"""
Build/refresh the demo account's data. Shared by the `seed_demo` management
command and the on-login lazy refresh (signals.py) — so the demo stays fresh
with ZERO idle server cost: it is rebuilt only when a prospect actually logs in,
and at most once per day.
"""
import logging
import random
from calendar import monthrange
from datetime import timedelta

from django.contrib.auth.models import Group, User
from django.utils import timezone
from psycopg2.extras import Json

from .demo import DEMO_GROUP
from .models import (
    ProductLink,
    ProductLinkNotification,
    RestockLog,
    RestockSchedule,
    Storage,
    Supermarket,
)
from .scripts.DatabaseManager import DatabaseManager

logger = logging.getLogger(__name__)

DEMO_USERNAME = "demo"
DEMO_SUPERMARKET = "Demo Market"
DEFAULT_PASSWORD = "demo1234"

# {storage name: {settore, shelf_life range, {cluster: [product names]}}}
CATALOG = {
    "Demo - Generi Vari": {
        "settore": "GENERI VARI",
        "shelf_life": (150, 540),
        "clusters": {
            "PASTA": ["Pasta Penne", "Pasta Spaghetti", "Pasta Fusilli", "Pasta Rigatoni",
                      "Pasta Integrale", "Pasta all'Uovo", "Gnocchi", "Lasagne"],
            "CONSERVE": ["Pelati", "Passata", "Tonno Olio", "Tonno Naturale", "Mais",
                         "Fagioli", "Ceci", "Piselli"],
            "BEVANDE": ["Acqua Naturale", "Acqua Frizzante", "Cola", "Aranciata",
                        "Succo Pesca", "Succo Arancia", "The Limone", "Birra Lager"],
        },
    },
    "Demo - Deperibili": {
        "settore": "DEPERIBILI",
        "shelf_life": (7, 30),
        "clusters": {
            "LATTICINI": ["Latte Intero", "Latte P.Scremato", "Yogurt Bianco", "Yogurt Frutta",
                          "Mozzarella", "Ricotta", "Burro", "Panna Fresca"],
            "SALUMI": ["Prosciutto Cotto", "Prosciutto Crudo", "Salame", "Mortadella",
                       "Bresaola", "Pancetta", "Wurstel", "Speck"],
        },
    },
    "Demo - Surgelati": {
        "settore": "SURGELATI",
        "shelf_life": (180, 400),
        "clusters": {
            "GELATI": ["Cono Vaniglia", "Cono Cioccolato", "Vaschetta Fiordilatte",
                       "Ghiacciolo Limone", "Biscotto Gelato", "Tartufo", "Cornetto", "Sorbetto"],
            "PIZZE": ["Pizza Margherita", "Pizza Wurstel", "Pizza 4 Formaggi", "Pizza Prosciutto",
                      "Focaccia", "Calzone", "Baguette Farcita", "Pizza Verdure"],
            "VERDURE SURG": ["Piselli Surg", "Spinaci Surg", "Minestrone", "Bastoncini Pesce",
                             "Patatine Fritte", "Verdure Grigliate", "Fagiolini", "Misto Funghi"],
        },
    },
}

# Special product mixes per storage that light up the dashboard badges and the
# "Necessita di verifica" card. Counts are ranges (randomised per storage).
SPECIAL_MIX = {
    "pending": (8, 12),     # verified=FALSE, has recent purchase → "articoli in attesa"
    "negative": (2, 5),     # verified, stock<0 → red "Giacenza anomala"
    "exhausted": (4, 9),    # verified, stock=0, disponibile → yellow "Esauriti"
    "new": (6, 14),         # verified=FALSE, added <7d, no movement → cyan star "nuovi"
}

WEEKDAY_MULT = [0.9, 0.85, 0.9, 1.0, 1.25, 1.4, 0.7]  # Mon..Sun
DAILY_HISTORY = 140  # days of sales_sets (newest first)


def ensure_demo_account(password=None, per_cluster=10, log=None):
    """Create or refresh the demo user, supermarket, storages and seeded data.

    `password` is applied only when given (so the on-login refresh never resets
    it). Returns the demo Supermarket.
    """
    log = log or (lambda *a, **k: None)
    rng = random.Random(42)
    today = timezone.localdate()

    group, _ = Group.objects.get_or_create(name=DEMO_GROUP)
    user, created = User.objects.get_or_create(
        username=DEMO_USERNAME,
        defaults={"first_name": "Demo", "email": "demo@example.com"},
    )
    if created and not password:
        password = DEFAULT_PASSWORD
    if password:
        user.set_password(password)
    user.is_staff = False
    user.is_superuser = False
    user.save()
    user.groups.add(group)

    supermarket, _ = Supermarket.objects.get_or_create(
        name=DEMO_SUPERMARKET,
        defaults={"owner": user, "username": "demo", "password": "demo"},
    )
    if supermarket.owner_id != user.id:
        supermarket.owner = user
    supermarket.last_sales_sync_at = timezone.now()
    supermarket.save()

    for storage_name, spec in CATALOG.items():
        storage, _ = Storage.objects.get_or_create(
            supermarket=supermarket, name=storage_name,
            defaults={"settore": spec["settore"], "minimum_stock": 6},
        )
        storage.settore = spec["settore"]
        storage.last_list_update = timezone.now()
        storage.save()  # post_save signal creates the schema/tables on first save
        _ensure_schedule(storage)

    n, normals, sub_pair = _seed_products(supermarket, rng, today, per_cluster)
    settore_pools = {}
    for cod, var, settore, cluster in normals:
        settore_pools.setdefault(settore, []).append((cod, var))
    _seed_operation_logs(supermarket, rng, settore_pools)
    _seed_substitutions(supermarket, user, sub_pair)
    log(f"Seeded {n} products into schema for '{supermarket.name}'.")
    return supermarket


def refresh_if_stale(log=None):
    """On-login hook: rebuild the demo data only if it isn't already fresh today.
    Swallows all errors so a demo-data problem can never block login."""
    try:
        sm = Supermarket.objects.filter(name=DEMO_SUPERMARKET).first()
        if sm and sm.last_sales_sync_at and \
                sm.last_sales_sync_at.date() == timezone.localdate():
            return  # already refreshed today — do nothing
        ensure_demo_account(log=log)
    except Exception:
        logger.exception("Demo lazy-refresh failed")


def _ensure_schedule(storage):
    sched, _ = RestockSchedule.objects.get_or_create(storage=storage)
    # A plausible Mon/Wed/Fri order week so the agenda looks populated.
    sched.monday = sched.wednesday = sched.friday = True
    sched.tuesday = sched.thursday = sched.saturday = sched.sunday = False
    sched.save()


def _seed_products(supermarket, rng, today, per_cluster):
    """Seed the schema and return (count, normals, sub_pair). normals is a list of
    (cod, var, settore, cluster) for the normal products (used to build plausible
    orders); sub_pair is ((old_cod, old_var), (new_cod, new_var)) for the one fixed
    substitution example."""
    db = DatabaseManager(supermarket_name=supermarket.name)
    try:
        cur = db.cursor()
        for tbl in ("extra_losses", "product_stats", "economics", "products"):
            cur.execute(f"DELETE FROM {tbl}")
        db.conn.commit()

        state = {"cod": 100000}
        months = _month_lengths(today, 24)
        normals = []

        for spec in CATALOG.values():
            settore = spec["settore"]
            clusters = list(spec["clusters"].items())
            for cluster, names in clusters:
                for i in range(per_cluster):
                    cod, var = _emit_product(cur, state, settore, cluster,
                                             f"{names[i % len(names)]} {cluster[:3]}{i:02d}",
                                             rng, today, months, "normal")
                    normals.append((cod, var, settore, cluster))

            # Special products that drive the dashboard badges / verification card.
            for kind, (lo, hi) in SPECIAL_MIX.items():
                for _ in range(rng.randint(lo, hi)):
                    cluster, names = rng.choice(clusters)
                    label = rng.choice(names)
                    _emit_product(cur, state, settore, cluster,
                                  f"{label} {kind[:3].upper()}{state['cod'] % 100:02d}",
                                  rng, today, months, kind)

        # One fixed, clearly-named substitution example (old -> new).
        old_cod, old_var = _emit_product(cur, state, "GENERI VARI", "CONSERVE",
                                         "Sugo al Basilico 190g (vecchia referenza)",
                                         rng, today, months, "normal")
        new_cod, new_var = _emit_product(cur, state, "GENERI VARI", "CONSERVE",
                                         "Sugo al Basilico 190g (nuova referenza)",
                                         rng, today, months, "normal")
        normals.append((old_cod, old_var, "GENERI VARI", "CONSERVE"))
        normals.append((new_cod, new_var, "GENERI VARI", "CONSERVE"))

        db.conn.commit()
        return state["cod"] - 100000, normals, ((old_cod, old_var), (new_cod, new_var))
    finally:
        db.close()


def _emit_product(cur, state, settore, cluster, name, rng, today, months, kind):
    """Insert one product + economics + product_stats (+ losses) shaped for `kind`.
    Returns (cod, var). See SPECIAL_MIX / dashboard queries for what each kind lights up."""
    state["cod"] += 1
    cod = state["cod"]
    # Real catalogue variants are 1-based (never 0); the inventory search form treats
    # variant 0 as "missing" and rejects the search, so keep the demo 1-based too.
    var = rng.choice([1, 1, 1, 2, 3])
    rapp = rng.choice([6, 8, 12])
    shelf_life = rng.randint(*_shelf_range(settore))
    ean = 8000000000000 + cod
    dispo = "Si"
    added = today - timedelta(days=rng.randint(0, 6)) if kind == "new" \
        else today - timedelta(days=rng.randint(30, 400))
    cur.execute(
        """INSERT INTO products
           (cod, v, descrizione, rapp, pz_x_collo, settore,
            disponibilita, cluster, ean, shelf_life_days, first_added_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (cod, var, name, rapp, rapp, settore, dispo, cluster, ean, shelf_life, added),
    )

    iva = 22 if settore == "GENERI VARI" else 10
    price_std = round(rng.uniform(0.8, 4.5), 2)
    net = price_std / (1 + iva / 100.0)
    cost_std = round(net * rapp * rng.uniform(0.62, 0.78), 2)
    cur.execute(
        """INSERT INTO economics (cod, v, price_std, cost_std, category, iva)
           VALUES (%s,%s,%s,%s,%s,%s)""",
        (cod, var, price_std, cost_std, settore, iva),
    )

    rate = round(rng.uniform(0.3, 14.0), 2)
    price_hist = [price_std] * 24
    cost_hist = [cost_std] * 24

    if kind == "new":
        # Brand-new, no movement yet: verified=FALSE, empty history, recently added.
        sales_sets, bought_sets = [0] * 30, [0] * 30
        sold_last_24, bought_last_24 = [0] * 24, [0] * 24
        stock, verified = 0, False
        upd_bought = added
    else:
        sales_sets = _daily_series(rng, rate, today, DAILY_HISTORY)
        bought_sets = _bought_series(rng, rate, rapp, DAILY_HISTORY)
        sold_last_24 = _monthly(rate, months, today)
        bought_last_24 = [round(v) for v in sold_last_24]
        upd_bought = today
        if kind == "pending":
            verified = False
            stock = max(1, round(rate * rng.uniform(2, 5)))
            bought_last_24[0] = max(rapp, round(rate * 3))  # recent unverified delivery
            upd_bought = today - timedelta(days=rng.randint(0, 3))
        elif kind == "negative":
            verified, stock = True, -rng.randint(1, 8)
        elif kind == "exhausted":
            verified, stock = True, 0
        else:  # normal
            verified = True
            stock = max(0, round(rate * rng.uniform(2.5, 6.0)))

    cur.execute(
        """INSERT INTO product_stats
           (cod, v, sold_last_24, bought_last_24, sales_sets, bought_sets,
            stock, verified, minimum_stock, last_update_sold, last_update_bought,
            price_last_24, cost_last_24)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,%s,%s)""",
        (cod, var, Json(sold_last_24), Json(bought_last_24), Json(sales_sets),
         Json(bought_sets), stock, verified, today, upd_bought, price_hist, cost_hist),
    )
    if kind in ("normal", "exhausted"):
        _insert_losses(cur, cod, var, rng, cost_std, today)
    return cod, var


def _shelf_range(settore):
    for spec in CATALOG.values():
        if spec["settore"] == settore:
            return spec["shelf_life"]
    return (60, 300)


def _month_lengths(today, n):
    out = [today.day]
    y, m = today.year, today.month
    for _ in range(1, n):
        m -= 1
        if m == 0:
            m, y = 12, y - 1
        out.append(monthrange(y, m)[1])
    return out


def _daily_series(rng, rate, today, days):
    series = []
    for i in range(days):
        d = today - timedelta(days=i)
        expected = rate * WEEKDAY_MULT[d.weekday()]
        val = max(0, round(rng.gauss(expected, max(0.6, expected * 0.35))))
        if i == 0:  # today only partially elapsed
            val = round(val * 0.45)
        series.append(val)
    return series


def _bought_series(rng, rate, rapp, days):
    series = [0] * days
    i = rng.randint(1, 4)
    while i < days:
        series[i] = max(rapp, round(rate * rng.uniform(3, 5)))
        i += rng.randint(3, 6)
    return series


def _monthly(rate, months, today):
    out = [round(rate * today.day)]
    out += [round(rate * length) for length in months[1:]]
    return out


# Which loss types a product might carry, and how heavy/frequent they are.
# (type, max monthly qty, sparsity = share of months with zero).
_LOSS_KINDS = [
    ("expired", 4, 0.55),
    ("broken", 3, 0.70),
    ("internal", 3, 0.75),
    ("shrinkage", 2, 0.80),
]


def _insert_losses(cur, cod, var, rng, cost_std, today):
    """~60% of products carry some loss history, so the losses analytics has
    plausible content. Stored as [[qty, cost], ...] monthly, index 0 = this month."""
    if rng.random() > 0.6:
        return
    kinds = [k for k in _LOSS_KINDS if rng.random() < 0.6] or [_LOSS_KINDS[0]]
    cols, vals = [], []
    for name, max_q, sparsity in kinds:
        series = [[0 if rng.random() < sparsity else rng.randint(1, max_q), cost_std]
                  for _ in range(24)]
        cols += [name, f"{name}_updated"]
        vals += [Json(series), today]
    placeholders = ",".join(["%s"] * (2 + len(vals)))
    cur.execute(
        f"INSERT INTO extra_losses (cod, v, {', '.join(cols)}) VALUES ({placeholders})",
        [cod, var, *vals],
    )


# A believable operations history per storage. (operation_type, detail dict).
_OP_TEMPLATES = [
    ("full_restock", {"orders": True, "coverage": True}),
    ("ddt_import", {"ddt": True}),
    ("list_update", {}),
    ("verification", {"verify": (20, 120)}),
    ("order_execution", {"orders": True}),
    ("loss_recording", {"loss": True}),
    ("full_restock", {"orders": True, "coverage": True}),
    ("ddt_import", {"ddt": True}),
]


def _build_orders(pool, rng):
    """A real order list ({cod, var, qty, discount}) the detail view enriches from
    the schema, so 'Riepilogo ordine' shows lines, clusters and costs — not zeros."""
    if not pool:
        return []
    k = min(len(pool), rng.randint(14, 30))
    orders = []
    for cod, var in rng.sample(pool, k):
        orders.append({
            "cod": cod, "var": var,
            "qty": rng.randint(1, 6),  # colli
            "discount": rng.choice([None, None, None, None, 10, 20, 30]),
        })
    return orders


def _seed_substitutions(supermarket, user, sub_pair):
    """One fixed substitution example. The dashboard renders Primario -> Secondario,
    so old_cod goes in primary and new_cod in secondary to read 'vecchia -> nuova'."""
    ProductLink.objects.filter(supermarket=supermarket).delete()
    ProductLinkNotification.objects.filter(supermarket=supermarket).delete()
    (old_cod, old_var), (new_cod, new_var) = sub_pair
    fields = dict(
        supermarket=supermarket, primary_cod=old_cod, primary_v=old_var,
        secondary_cod=new_cod, secondary_v=new_var, created_by=user,
    )
    ProductLink.objects.create(notes="Referenza sostituita (demo).", **fields)
    ProductLinkNotification.objects.create(is_read=False, **fields)


def _seed_operation_logs(supermarket, rng, settore_pools):
    """Fabricate a completed operations history so 'Operazioni recenti' looks
    lived-in and each order's 'Riepilogo ordine' has real lines. Plain RestockLog
    rows in the default DB — no task ever runs."""
    RestockLog.objects.filter(storage__supermarket=supermarket).delete()
    now = timezone.now()
    logs = []
    for storage in supermarket.storages.all():
        pool = settore_pools.get(storage.settore, [])
        offset = 1
        for op, spec in _OP_TEMPLATES:
            offset += rng.randint(2, 5)
            started = now - timedelta(days=offset, hours=rng.randint(0, 8),
                                      minutes=rng.randint(0, 59))
            completed = started + timedelta(minutes=rng.randint(3, 14),
                                            seconds=rng.randint(0, 59))
            log = RestockLog(
                storage=storage, operation_type=op, status="completed",
                current_stage="completed", started_at=started, completed_at=completed,
            )
            if spec.get("orders"):
                orders = _build_orders(pool, rng)
                log.set_results({"orders": orders})
                log.products_ordered = len(orders)
                log.total_products = len(pool)
                log.total_packages = sum(o["qty"] for o in orders)
            if spec.get("coverage"):
                log.coverage_used = rng.choice([3, 4, 4, 5, 6])
            if "verify" in spec:
                log.products_ordered = rng.randint(*spec["verify"])
                log.total_products = log.products_ordered + rng.randint(0, 40)
            if spec.get("loss"):
                log.total_products = rng.randint(2, 12)
                log.total_packages = log.total_products
            if spec.get("ddt"):
                updated = rng.randint(15, 60)
                log.set_results({
                    "invoices": [f"{rng.randint(1000, 9999)}/D"],
                    "updated": updated,
                    "not_found": [],
                })
                log.products_ordered = updated
            logs.append(log)
    RestockLog.objects.bulk_create(logs)
