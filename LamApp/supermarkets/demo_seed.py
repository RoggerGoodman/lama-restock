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
from .models import RestockLog, RestockSchedule, Storage, Supermarket
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

    n = _seed_products(supermarket, rng, today, per_cluster)
    _seed_operation_logs(supermarket, rng)
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
    db = DatabaseManager(supermarket_name=supermarket.name)
    try:
        cur = db.cursor()
        for tbl in ("extra_losses", "product_stats", "economics", "products"):
            cur.execute(f"DELETE FROM {tbl}")
        db.conn.commit()

        cod = 100000
        months = _month_lengths(today, 24)
        for spec in CATALOG.values():
            settore = spec["settore"]
            shelf_lo, shelf_hi = spec["shelf_life"]
            for cluster, names in spec["clusters"].items():
                for i in range(per_cluster):
                    cod += 1
                    name = f"{names[i % len(names)]} {cluster[:3]}{i:02d}"
                    rapp = rng.choice([6, 8, 12])
                    shelf_life = rng.randint(shelf_lo, shelf_hi)
                    ean = 8000000000000 + cod
                    cur.execute(
                        """INSERT INTO products
                           (cod, v, descrizione, rapp, pz_x_collo, settore,
                            disponibilita, cluster, ean, shelf_life_days)
                           VALUES (%s,%s,%s,%s,%s,%s,'Si',%s,%s,%s)""",
                        (cod, 0, name, rapp, rapp, settore, cluster, ean, shelf_life),
                    )

                    iva = 22 if settore == "GENERI VARI" else 10
                    price_std = round(rng.uniform(0.8, 4.5), 2)
                    net = price_std / (1 + iva / 100.0)
                    cost_std = round(net * rapp * rng.uniform(0.62, 0.78), 2)
                    cur.execute(
                        """INSERT INTO economics
                           (cod, v, price_std, cost_std, category, iva)
                           VALUES (%s,%s,%s,%s,%s,%s)""",
                        (cod, 0, price_std, cost_std, settore, iva),
                    )

                    rate = round(rng.uniform(0.3, 14.0), 2)
                    sales_sets = _daily_series(rng, rate, today, DAILY_HISTORY)
                    bought_sets = _bought_series(rng, rate, rapp, DAILY_HISTORY)
                    sold_last_24 = _monthly(rate, months, today)
                    bought_last_24 = [round(v) for v in sold_last_24]
                    price_hist = [price_std] * 24
                    cost_hist = [cost_std] * 24
                    stock = max(0, round(rate * rng.uniform(2.5, 6.0)))
                    cur.execute(
                        """INSERT INTO product_stats
                           (cod, v, sold_last_24, bought_last_24, sales_sets,
                            bought_sets, stock, verified, minimum_stock,
                            last_update_sold, last_update_bought,
                            price_last_24, cost_last_24)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE,NULL,%s,%s,%s,%s)""",
                        (cod, 0, Json(sold_last_24), Json(bought_last_24),
                         Json(sales_sets), Json(bought_sets), stock,
                         today, today, price_hist, cost_hist),
                    )
                    _insert_losses(cur, cod, rng, cost_std, today)
        db.conn.commit()
        return cod - 100000
    finally:
        db.close()


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


def _insert_losses(cur, cod, rng, cost_std, today):
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
        [cod, 0, *vals],
    )


# A believable operations history per storage. (operation_type, detail dict).
_OP_TEMPLATES = [
    ("full_restock", {"coverage": True, "ordered": (30, 90), "packages": (40, 130)}),
    ("ddt_import", {"ddt": True}),
    ("list_update", {}),
    ("verification", {"ordered": (20, 120)}),
    ("order_execution", {"ordered": (25, 70), "packages": (35, 100)}),
    ("loss_recording", {"ordered": (1, 12)}),
    ("full_restock", {"coverage": True, "ordered": (30, 90), "packages": (40, 130)}),
    ("ddt_import", {"ddt": True}),
]


def _seed_operation_logs(supermarket, rng):
    """Fabricate a completed operations history so 'Operazioni recenti' looks
    lived-in. Plain RestockLog rows in the default DB — no task ever runs."""
    RestockLog.objects.filter(storage__supermarket=supermarket).delete()
    now = timezone.now()
    logs = []
    for storage in supermarket.storages.all():
        offset = 1
        for i, (op, spec) in enumerate(_OP_TEMPLATES):
            offset += rng.randint(2, 5)
            started = now - timedelta(days=offset, hours=rng.randint(0, 8),
                                      minutes=rng.randint(0, 59))
            completed = started + timedelta(minutes=rng.randint(3, 14),
                                            seconds=rng.randint(0, 59))
            log = RestockLog(
                storage=storage, operation_type=op, status="completed",
                current_stage="completed", started_at=started, completed_at=completed,
            )
            if "ordered" in spec:
                log.products_ordered = rng.randint(*spec["ordered"])
                log.total_products = log.products_ordered + rng.randint(0, 40)
            if "packages" in spec:
                log.total_packages = rng.randint(*spec["packages"])
            if spec.get("coverage"):
                log.coverage_used = rng.choice([3, 4, 4, 5, 6])
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
