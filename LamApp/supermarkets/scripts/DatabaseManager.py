import re
from contextlib import contextmanager
import pandas as pd
import psycopg2
import psycopg2.extras
import os
from psycopg2.extras import Json, execute_values
from datetime import date
import logging

logger = logging.getLogger(__name__)


def _stamp_snapshot(existing, current_value):
    """Set slot [0] of a real[] snapshot array to current_value, keeping older slots."""
    arr = list(existing) if existing else []
    if not arr:
        arr = [None]
    arr[0] = float(current_value) if current_value is not None else None
    return arr


class DatabaseManager:

    # Ceiling on how far back a single losses batch is spread. A client who stops
    # recording for a month would otherwise smear one batch across the whole window.
    LOSS_MAX_SPREAD_DAYS = 7

    # --- Connection & Cursor ---

    def __init__(self, supermarket_name=None):
        if supermarket_name:
            self.schema = self._sanitize_schema_name(supermarket_name)
        else:
            self.schema = "public"

        self.conn = psycopg2.connect(
            host=os.environ.get('PG_HOST'),
            database=os.environ.get('PG_DATABASE'),
            user=os.environ.get('PG_USER'),
            password=os.environ.get('PG_PASSWORD'),
            options=f'-c search_path={self.schema},public'
        )
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def cursor(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def _sanitize_schema_name(self, name):
        clean = re.sub(r'[^\w\s-]', '', name.lower())
        clean = re.sub(r'[-\s]+', '_', clean)
        return clean

    def close(self):
        self.conn.close()

    # --- Schema / DDL ---

    def create_tables(self):
        cur = self.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                cod INTEGER NOT NULL,
                v INTEGER NOT NULL,
                descrizione TEXT NOT NULL,
                rapp INTEGER,
                pz_x_collo INTEGER,
                settore TEXT NOT NULL,
                disponibilita TEXT CHECK(disponibilita IN ('Si','No','N.B.')) DEFAULT 'Si',
                cluster TEXT,
                purge_flag BOOLEAN DEFAULT FALSE,
                ean BIGINT,
                shelf_life_days INTEGER,
                first_added_at DATE DEFAULT CURRENT_DATE,
                PRIMARY KEY (cod, v)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_stats (
                cod INTEGER NOT NULL,
                v INTEGER NOT NULL,
                sold_last_24 JSONB,
                bought_last_24 JSONB,
                sales_sets JSONB,
                bought_sets JSONB,
                stock INTEGER DEFAULT 0,
                verified BOOLEAN DEFAULT FALSE,
                -- No default: NULL means "no per-product override"
                minimum_stock INTEGER,
                -- NULL = no ceiling; see processor_N.apply_max_stock
                max_stock SMALLINT,
                bulk_order BOOLEAN NOT NULL DEFAULT FALSE,
                last_update_sold DATE,
                last_update_bought DATE,
                promo_lifts JSONB,
                price_last_24 real[],
                cost_last_24 real[],
                FOREIGN KEY (cod, v) REFERENCES products (cod, v),
                PRIMARY KEY (cod, v)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS economics (
                cod INTEGER NOT NULL,
                v INTEGER NOT NULL,
                price_std FLOAT NOT NULL,
                cost_std FLOAT NOT NULL,
                price_s FLOAT,
                cost_s FLOAT,
                sale_start DATE,
                sale_end DATE,
                category TEXT NOT NULL,
                iva INTEGER,
                FOREIGN KEY (cod, v) REFERENCES products (cod, v),
                PRIMARY KEY (cod, v)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS extra_losses (
                cod INTEGER NOT NULL,
                v INTEGER NOT NULL,
                broken JSONB,
                broken_updated DATE,
                expired JSONB,
                expired_updated DATE,
                internal JSONB,
                internal_updated DATE,
                stolen JSONB,
                stolen_updated DATE,
                shrinkage JSONB,
                shrinkage_updated DATE,
                FOREIGN KEY (cod, v) REFERENCES products (cod, v),
                PRIMARY KEY (cod, v)
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_products_settore ON products(settore)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_products_cluster ON products(cluster)")

        self.conn.commit()
        print(f"Tables created/verified in schema: {self.schema}")

    # --- Product CRUD ---

    def add_product(self, cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita="Si", ean=None):
        cur = self.cursor()
        cur.execute("""
            INSERT INTO products (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita, ean)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod, v) DO NOTHING
        """, (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita, ean))
        self.conn.commit()

    def init_product_stats(self, cod: int, v: int, sold: list, bought: list, stock: int = 0, verified: bool = False):
        sold = sold if sold else [0]
        bought = bought if bought else [0]
        today = date.today()
        cur = self.cursor()
        cur.execute("""
            INSERT INTO product_stats (
                cod, v, sold_last_24, bought_last_24, stock, verified, last_update_sold,
                minimum_stock
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
            ON CONFLICT (cod, v) DO NOTHING
        """, (cod, v, Json(sold), Json(bought), stock, bool(verified), today))
        self.conn.commit()

    # --- Queries ---

    def get_product_stats(self, cod, v):
        cur = self.cursor()
        cur.execute("SELECT * FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "sold": row["sold_last_24"] or [],
            "bought": row["bought_last_24"] or [],
            "stock": row["stock"] or 0,
            "verified": bool(row["verified"]),
            "last_update_sold": row["last_update_sold"],
        }

    def get_linked_product_stats(self, cod, v):
        """
        Fetch the data needed to handle a product link regardless of settore:
        the stats to merge, plus the availability flags used to decide which
        side of the link is the one to order.
        """
        cur = self.cursor()
        cur.execute("""
            SELECT ps.sales_sets, ps.stock, ps.verified, p.disponibilita, p.purge_flag
            FROM products p
            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.cod = %s AND p.v = %s
        """, (cod, v))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "sales_sets": row["sales_sets"] or [],
            "stock": row["stock"] or 0,
            "verified": row["verified"],
            "disponibilita": row["disponibilita"],
            "purge_flag": row["purge_flag"],
        }

    def get_store_daily_totals(self):
        """
        Element-wise sum of sales_sets across every verified product: one total per
        day slot, newest first. Feeds Helper.closure_day_mask, which uses it to
        spot closures and missed syncs.
        """
        cur = self.cursor()
        cur.execute("""
            SELECT t.ord, SUM((t.elem)::numeric) AS total
            FROM product_stats ps,
                 LATERAL jsonb_array_elements(ps.sales_sets) WITH ORDINALITY AS t(elem, ord)
            WHERE ps.verified = TRUE
              AND ps.sales_sets IS NOT NULL
              AND jsonb_typeof(t.elem) = 'number'
            GROUP BY t.ord
            ORDER BY t.ord
        """)
        return [float(r["total"] or 0) for r in cur.fetchall()]

    def get_promos_ended_days_ago(self, days_ago: int):
        """
        Products whose promotion ended exactly `days_ago` days ago, with the
        sales_sets needed to measure the lift. The exact-day match is what makes
        measurement idempotent — each promo is seen on exactly one nightly sweep.
        """
        cur = self.cursor()
        cur.execute("""
            SELECT e.cod, e.v, e.sale_start, e.sale_end, e.price_std, e.price_s,
                   ps.sales_sets
            FROM economics e
            JOIN product_stats ps ON ps.cod = e.cod AND ps.v = e.v
            WHERE e.sale_start IS NOT NULL
              AND e.sale_end IS NOT NULL
              AND (CURRENT_DATE - e.sale_end) = %s
              AND ps.verified = TRUE
        """, (days_ago,))
        return cur.fetchall()

    def append_promo_lift(self, cod, v, lift, discount, keep=3):
        """Prepend a measured promo lift, keeping only the most recent `keep`."""
        cur = self.cursor()
        cur.execute("SELECT promo_lifts FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
        row = cur.fetchone()
        if not row:
            return False

        lifts = row["promo_lifts"] or []

        # An identical head entry means the nightly task retried — re-writing would
        # evict a genuine older promo from the 3 slots.
        if lifts and isinstance(lifts[0], dict):
            if lifts[0].get("lift") == lift and lifts[0].get("discount") == discount:
                return False

        lifts.insert(0, {"lift": lift, "discount": discount})
        lifts = lifts[:keep]

        cur.execute(
            "UPDATE product_stats SET promo_lifts=%s WHERE cod=%s AND v=%s",
            (Json(lifts), cod, v)
        )
        self.conn.commit()
        return True

    def get_stock(self, cod, v):
        cur = self.cursor()
        cur.execute("SELECT stock FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No product_stats found for {cod}.{v}")
        return row["stock"]

    def get_product_by_ean(self, ean):
        cur = self.cursor()
        cur.execute("""
            SELECT p.cod, p.v, p.descrizione, p.pz_x_collo, p.settore
            FROM products p
            WHERE p.ean = %s
            LIMIT 1
        """, (ean,))
        return cur.fetchone()

    def get_all_stats_by_settore(self, settore):
        cur = self.cursor()
        cur.execute("""
            SELECT
                p.cod,
                p.v,
                p.descrizione,
                p.rapp,
                p.pz_x_collo,
                p.disponibilita,
                ps.sold_last_24,
                ps.bought_last_24,
                ps.stock,
                ps.verified,
                ps.last_update_sold
            FROM products AS p
            LEFT JOIN product_stats AS ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.settore = %s
        """, (settore,))

        results = []
        for row in cur.fetchall():
            results.append({
                "cod": row["cod"],
                "v": row["v"],
                "descrizione": row["descrizione"],
                "rapp": row["rapp"],
                "pz_x_collo": row["pz_x_collo"],
                "disponibilita": row["disponibilita"],
                "sold": row["sold_last_24"] or [],
                "bought": row["bought_last_24"] or [],
                "stock": row["stock"] if row["stock"] is not None else 0,
                "verified": bool(row["verified"]) if row["verified"] is not None else False,
                "last_update_sold": row["last_update_sold"],
            })
        return results

    def get_purge_pending(self):
        """Get all products flagged for purging with stock > 0."""
        cur = self.cursor()
        try:
            cur.execute("""
                SELECT p.cod, p.v, p.descrizione, ps.stock
                FROM products p
                JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                WHERE p.purge_flag = TRUE AND ps.stock > 0
                ORDER BY ps.stock DESC
            """)
            return [
                {'cod': row['cod'], 'v': row['v'], 'name': row['descrizione'], 'stock': row['stock']}
                for row in cur.fetchall()
            ]
        except Exception:
            return []

    # --- Stock Operations ---

    def adjust_stock(self, cod: int, v: int, delta: int):
        """Increment or decrement stock by delta (can be negative)."""
        # Done in SQL, not read-then-write, so a sales sync committing in between is not lost.
        cur = self.cursor()
        cur.execute(
            "UPDATE product_stats SET stock = COALESCE(stock, 0) + %s WHERE cod=%s AND v=%s",
            (delta, cod, v)
        )
        if cur.rowcount == 0:
            logger.warning(f"No product_stats found for {cod}.{v}")
            return
        self.conn.commit()

    def verify_stock(self, cod: int, v: int, new_stock: int, cluster: str = None):
        """
        Called when a human inspects and corrects stock.
        Sets verified=TRUE. Does not change last_update_sold.
        """
        cur = self.cursor()
        if new_stock is not None:
            cur.execute(
                "UPDATE product_stats SET stock=%s, verified=TRUE WHERE cod=%s AND v=%s",
                (new_stock, cod, v)
            )
            if cur.rowcount == 0:
                logger.warning(f"No product_stats found for {cod}.{v}, initializing row")
                self.init_product_stats(cod, v, sold=[0], bought=[0], stock=new_stock, verified=True)

        if cluster is not None:
            cur.execute("UPDATE products SET cluster=%s WHERE cod=%s AND v=%s", (cluster, cod, v))
            if cur.rowcount == 0:
                logger.warning(f"No products found for {cod}.{v}")

        self.conn.commit()

    # --- Data Sync ---

    def _rollover_sales_day(self, cur, sync_date) -> int:
        """
        Close every product's current slot and open a fresh one for `sync_date`.

        Idempotent: products already at `sync_date` are skipped.

        Closing applies the censored-stockout rule — a verified product that ended on zero
        with an empty shelf while the supplier still had stock was unbuyable, not
        demandless, so its slot becomes None and drops out of the averages.
        """
        cur.execute("""
            SELECT ps.cod, ps.v, ps.sales_sets, ps.bought_sets, ps.stock, ps.verified,
                   p.disponibilita
            FROM product_stats ps
            LEFT JOIN products p ON p.cod = ps.cod AND p.v = ps.v
            WHERE ps.last_update_sold IS NULL OR ps.last_update_sold < %s
        """, (sync_date,))
        rows = cur.fetchall()
        if not rows:
            return 0

        updates = []
        for r in rows:
            ss = r["sales_sets"] or []
            if ss and (ss[0] or 0) == 0 and bool(r["verified"]):
                stock_zero = (r["stock"] or 0) == 0
                supplier_oos = r["disponibilita"] == 'No'
                # Look past slot 0 — that is the day being closed, not history
                last_known_sale = next((v for v in ss[1:] if v is not None), None)
                demand_driven = last_known_sale is not None and last_known_sale > 0
                if stock_zero and not supplier_oos and demand_driven:
                    ss[0] = None

            ss.insert(0, 0)
            bs = r["bought_sets"] or []
            bs.insert(0, 0)
            updates.append((r["cod"], r["v"], Json(ss[:60]), Json(bs[:60]), sync_date))

        # Batched: a full pass covers every product in the schema, and one UPDATE each
        # meant thousands of round trips. Alias is `d`, not `v` — product_stats has a
        # column called v and the collision would silently match the wrong rows.
        execute_values(cur, """
            UPDATE product_stats AS ps
            SET sales_sets       = d.sets::jsonb,
                bought_sets      = d.bought::jsonb,
                last_update_sold = d.day::date
            FROM (VALUES %s) AS d(cod, var, sets, bought, day)
            WHERE ps.cod = d.cod::int AND ps.v = d.var::int
        """, updates, page_size=1000)

        self.conn.commit()
        return len(rows)

    def roll_sales_day(self, sync_date) -> int:
        """
        Open slot 0 for `sync_date` across every product, independently of any sync.

        Runs just after midnight so "slot 0 is today" holds from the date change. Left to
        the first sync at 08:30, anything ordering or calibrating before then would slice
        off yesterday as if it were the running day.
        """
        cur = self.cursor()
        return self._rollover_sales_day(cur, sync_date)

    def apply_realtime_sales(self, totals, sync_date, shelf_life_map=None) -> dict:
        """
        Apply running per-product totals for `sync_date` from the Everest till feed.

        `totals` is [(cod, var, sold_today), ...] holding the day's total SO FAR, not an
        increment. Only the difference is booked, so every call is idempotent: a repeated
        payload is a no-op, a missed run is made up by the next, and the store keeps no
        state. sales_sets[0] is rewritten in place; the day boundary is crossed only by
        _rollover_sales_day.
        """
        cur = self.cursor()
        rolled = self._rollover_sales_day(cur, sync_date)

        # Dedupe first: the batched update would count a repeated (cod, var) twice, where
        # the old per-product loop happened to absorb it.
        wanted = {(int(c), int(v)): int(s) for c, v, s in totals}

        applied = 0
        unchanged = 0
        total_delta = 0
        unverified_products = []
        stat_updates = []
        shelf_updates = []

        rows = []
        if wanted:
            # Locked until commit: stock is written back as read-minus-delta, so a stock
            # verification landing in between would otherwise be overwritten. ORDER BY
            # keeps the lock order fixed so two overlapping syncs cannot deadlock.
            cur.execute("""
                SELECT ps.cod, ps.v, ps.sold_last_24, ps.sales_sets, ps.stock, ps.verified,
                       ps.price_last_24, ps.cost_last_24,
                       e.price_std, e.cost_std
                FROM product_stats ps
                JOIN unnest(%s::int[], %s::int[]) AS t(cod, v)
                  ON ps.cod = t.cod AND ps.v = t.v
                LEFT JOIN economics e ON e.cod = ps.cod AND e.v = ps.v
                ORDER BY ps.cod, ps.v
                FOR UPDATE OF ps
            """, ([k[0] for k in wanted], [k[1] for k in wanted]))
            rows = cur.fetchall()

        not_in_db = len(wanted) - len(rows)

        for row in rows:
            cod, var = row["cod"], row["v"]
            sold_today = wanted[(cod, var)]

            verified = bool(row["verified"])
            if not verified:
                unverified_products.append({'cod': cod, 'v': var})

            ss = row["sales_sets"] or [0]
            if not ss:
                ss = [0]

            delta = sold_today - (ss[0] or 0)
            if delta == 0:
                unchanged += 1
                continue

            if shelf_life_map and verified:
                sl = shelf_life_map.get((cod, var))
                if sl is not None:
                    shelf_updates.append((cod, var, int(sl)))

            sold_array = row["sold_last_24"]
            if not isinstance(sold_array, list) or not sold_array:
                sold_array = [0]
            sold_array[0] = (sold_array[0] or 0) + delta

            ss[0] = sold_today
            stock = (row["stock"] or 0) - delta

            price_arr = _stamp_snapshot(row["price_last_24"], row["price_std"])
            cost_arr = _stamp_snapshot(row["cost_last_24"], row["cost_std"])

            stat_updates.append((cod, var, Json(sold_array), Json(ss), stock,
                                 price_arr, cost_arr))
            applied += 1
            total_delta += delta

        if stat_updates:
            execute_values(cur, """
                UPDATE product_stats AS ps
                SET sold_last_24  = d.sold,
                    sales_sets    = d.sets,
                    stock         = d.stock,
                    price_last_24 = d.price,
                    cost_last_24  = d.cost
                FROM (VALUES %s) AS d(cod, var, sold, sets, stock, price, cost)
                WHERE ps.cod = d.cod AND ps.v = d.var
            """, stat_updates,
                template="(%s::int, %s::int, %s::jsonb, %s::jsonb, %s::int, %s::real[], %s::real[])",
                page_size=1000)

        if shelf_updates:
            execute_values(cur, """
                UPDATE products AS p
                SET shelf_life_days = d.sl::int
                FROM (VALUES %s) AS d(cod, var, sl)
                WHERE p.cod = d.cod::int AND p.v = d.var::int
            """, shelf_updates, page_size=1000)

        self.conn.commit()
        logger.info(
            f"[RT SYNC] schema={self.schema} date={sync_date} rolled_over={rolled} "
            f"applied={applied} unchanged={unchanged} not_in_db={not_in_db} "
            f"units={total_delta} unverified={len(unverified_products)}"
        )
        return {
            'applied': applied,
            'unchanged': unchanged,
            'not_in_db': not_in_db,
            'rolled_over': rolled,
            'units_applied': total_delta,
            'unverified_products': unverified_products,
        }

    def apply_history_backfill(self, entries, include_current: bool, history_date) -> dict:
        """
        Seed sold_last_24 and sales_sets from a store's own historical data.

        `entries` is [(cod, var, monthly[24], daily[60]), ...] with index 0 = current
        month / today, matching the arrays' own ordering.

        `include_current` decides who owns index 0. A store whose live sync has never run
        has nothing there worth keeping, so it takes the lot. Once the sync is running,
        `sales_sets[0]` is the base its delta arithmetic works from and overwriting it
        would make the next run re-book the difference, so index 0 is left alone.

        Products in the imported catalogue get a product_stats row created if they lack
        one, since those rows otherwise only appear on first verification. Anything not
        in `products` is ignored: the dump carries the store's whole catalogue.
        """
        cur = self.cursor()

        wanted = {}
        skipped_fractional = 0
        for cod, var, monthly, daily in entries:
            vals = list(monthly) + list(daily)
            if any(v != int(v) for v in vals):
                # Weight-sold goods; the live path skips them for the same reason.
                skipped_fractional += 1
                continue
            wanted[(int(cod), int(var))] = (
                [int(v) for v in monthly][:24],
                [int(v) for v in daily][:60],
            )

        cods = [k[0] for k in wanted]
        vars_ = [k[1] for k in wanted]

        # product_stats rows only appear when a human verifies a product, so a freshly
        # onboarded store has none and there would be nothing to write history onto.
        # Seed them for anything in the imported catalogue: stock 0 and verified False
        # keeps them out of ordering until someone actually counts the shelf, and
        # verify_stock updates in place, so the history survives that.
        created = 0
        if wanted:
            cur.execute("""
                SELECT p.cod, p.v
                FROM products p
                JOIN unnest(%s::int[], %s::int[]) AS t(c, vv)
                  ON p.cod = t.c AND p.v = t.vv
                LEFT JOIN product_stats ps ON ps.cod = p.cod AND ps.v = p.v
                WHERE ps.cod IS NULL
            """, (cods, vars_))
            missing = [(r["cod"], r["v"], history_date) for r in cur.fetchall()]
            if missing:
                # last_update_sold must be the dump's date: left NULL, the next rollover
                # would treat the day as unopened and shift every backfilled value by one.
                execute_values(cur, """
                    INSERT INTO product_stats
                        (cod, v, sold_last_24, bought_last_24, sales_sets, bought_sets,
                         stock, verified, last_update_sold)
                    SELECT d.cod::int, d.var::int, '[0]'::jsonb, '[0]'::jsonb,
                           '[0]'::jsonb, '[0]'::jsonb, 0, FALSE, d.day::date
                    FROM (VALUES %s) AS d(cod, var, day)
                    ON CONFLICT (cod, v) DO NOTHING
                """, missing, page_size=1000)
                created = len(missing)

        rows = []
        if wanted:
            cur.execute("""
                SELECT cod, v, sold_last_24, sales_sets
                FROM product_stats
                JOIN unnest(%s::int[], %s::int[]) AS t(c, vv)
                  ON cod = t.c AND v = t.vv
            """, (cods, vars_))
            rows = cur.fetchall()

        updates = []
        for row in rows:
            monthly, daily = wanted[(row["cod"], row["v"])]
            monthly = (monthly + [0] * 24)[:24]
            daily = (daily + [0] * 60)[:60]

            if not include_current:
                prev_sold = row["sold_last_24"] or [0]
                prev_sets = row["sales_sets"] or [0]
                today_live = (prev_sets[0] if prev_sets else 0) or 0
                # VEMEART is written nightly, so the dump's month excludes today. A store
                # syncing since before this month already holds the larger figure; one that
                # started today holds only today, and needs the dump's earlier days added.
                monthly[0] = max((prev_sold[0] if prev_sold else 0) or 0,
                                 monthly[0] + today_live)
                daily[0] = today_live

            updates.append((row["cod"], row["v"], Json(monthly), Json(daily)))

        if updates:
            execute_values(cur, """
                UPDATE product_stats AS ps
                SET sold_last_24 = d.sold::jsonb,
                    sales_sets   = d.sets::jsonb
                FROM (VALUES %s) AS d(cod, var, sold, sets)
                WHERE ps.cod = d.cod::int AND ps.v = d.var::int
            """, updates, page_size=1000)

        self.conn.commit()
        result = {
            'received': len(entries),
            'applied': len(updates),
            'created': created,
            'not_in_catalogue': len(wanted) - len(rows),
            'skipped_fractional': skipped_fractional,
            'included_current': include_current,
        }
        logger.info(f"[HISTORY] schema={self.schema} {result}")
        return result

    # --- Dropzone document ledger ---

    @contextmanager
    def transaction(self):
        """The connection autocommits; this groups statements into one commit."""
        self.conn.autocommit = False
        try:
            yield self.cursor()
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = True

    def ensure_document_ledger(self) -> bool:
        """
        Every Dropzone document (DDT or credit note) ever seen, keyed by
        "type-year-series-number". Returns True when the table was just created,
        so the caller can seed it at cutover.
        """
        if self.has_document_ledger():
            return False
        cur = self.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dropzone_documents (
                doc_key TEXT PRIMARY KEY,
                doc_type TEXT NOT NULL,
                doc_number TEXT NOT NULL,
                doc_date DATE NOT NULL,
                settore TEXT,
                delivery_date DATE,
                -- pending: delivery not due yet; applied; recorded: credit note handed to the UI;
                -- skipped: no storage, or one with no verified products; legacy: imported
                -- before the ledger existed; manual: loaded by hand from its PDF
                status TEXT NOT NULL,
                lines JSONB,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                applied_at TIMESTAMPTZ
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dropzone_documents_pending
            ON dropzone_documents (settore, delivery_date) WHERE status = 'pending'
        """)
        return True

    def known_document_keys(self, keys) -> set:
        keys = list(keys)
        if not keys:
            return set()
        cur = self.cursor()
        cur.execute("SELECT doc_key FROM dropzone_documents WHERE doc_key = ANY(%s)", (keys,))
        return {r["doc_key"] for r in cur.fetchall()}

    def record_document(self, doc_key, doc_type, doc_number, doc_date, status,
                        settore=None, delivery_date=None, lines=None) -> bool:
        """Insert-once. Returns False when the document was already recorded."""
        cur = self.cursor()
        cur.execute("""
            INSERT INTO dropzone_documents
                (doc_key, doc_type, doc_number, doc_date, settore, delivery_date, status, lines)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (doc_key) DO NOTHING
        """, (doc_key, doc_type, doc_number, doc_date, settore, delivery_date, status,
              Json(lines) if lines is not None else None))
        return cur.rowcount == 1

    def due_delivery_dates(self, settore, today) -> set:
        # Only the importer creates the ledger: it must seed it on creation
        if not self.has_document_ledger():
            return set()
        cur = self.cursor()
        cur.execute("""
            SELECT DISTINCT delivery_date FROM dropzone_documents
            WHERE status = 'pending' AND settore = %s AND delivery_date <= %s
        """, (settore, today))
        return {r["delivery_date"] for r in cur.fetchall()}

    def has_document_ledger(self) -> bool:
        cur = self.cursor()
        cur.execute("SELECT to_regclass('dropzone_documents') AS t")
        return cur.fetchone()["t"] is not None

    def find_ddt(self, settore, number, since):
        """This settore's latest ledger entry for DDT `number` dated on/after `since`, or None."""
        if not self.has_document_ledger():
            return None
        cur = self.cursor()
        cur.execute("""
            SELECT doc_key, status, doc_date, delivery_date, recorded_at, applied_at
            FROM dropzone_documents
            WHERE doc_type = 'BOL' AND settore = %s AND doc_number = %s
              AND doc_date >= %s AND status <> 'skipped'
            ORDER BY doc_date DESC, recorded_at DESC
            LIMIT 1
        """, (settore, number, since))
        return cur.fetchone()

    def claim_manual_ddt(self, settore, number, today) -> int:
        """
        A DDT loaded by hand from its PDF: cancel its automatic booking if it is
        waiting, and leave a marker so the importer never books it later.
        Returns how many pending bookings were cancelled.
        """
        if not self.has_document_ledger():
            return 0
        with self.transaction() as cur:
            cur.execute("""
                UPDATE dropzone_documents SET status = 'manual'
                WHERE doc_type = 'BOL' AND settore = %s AND doc_number = %s AND status = 'pending'
            """, (settore, number))
            cancelled = cur.rowcount
            cur.execute("""
                INSERT INTO dropzone_documents (doc_key, doc_type, doc_number, doc_date, settore, status)
                VALUES (%s, 'BOL', %s, %s, %s, 'manual')
                ON CONFLICT (doc_key) DO NOTHING
            """, (f"MANUAL-{today.year}-{settore}-{number}", number, today, settore))
        return cancelled

    def manual_ddt_claimed(self, settore, number, since) -> bool:
        cur = self.cursor()
        cur.execute("""
            SELECT 1 FROM dropzone_documents
            WHERE doc_type = 'BOL' AND status = 'manual' AND settore = %s AND doc_number = %s
              AND doc_date >= %s
            LIMIT 1
        """, (settore, number, since))
        return cur.fetchone() is not None

    def get_processing_table(self, settore):
        """The stored {DDT weekday: order weekday} for this settore, or None if never learned."""
        cur = self.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ddt_processing_tables (
                settore TEXT PRIMARY KEY,
                -- the agenda order days it was learned for, e.g. "1,3,6"
                agenda TEXT NOT NULL,
                -- NULL when the history fitted more than one reading
                mapping JSONB,
                learned_on DATE NOT NULL
            )
        """)
        cur.execute("SELECT agenda, mapping, learned_on FROM ddt_processing_tables WHERE settore = %s", (settore,))
        return cur.fetchone()

    def save_processing_table(self, settore, agenda, mapping, learned_on):
        cur = self.cursor()
        cur.execute("""
            INSERT INTO ddt_processing_tables (settore, agenda, mapping, learned_on)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (settore) DO UPDATE
            SET agenda = EXCLUDED.agenda, mapping = EXCLUDED.mapping, learned_on = EXCLUDED.learned_on
        """, (settore, agenda, Json(mapping) if mapping is not None else None, learned_on))

    def prune_document_ledger(self, before) -> int:
        """Drop documents dated before `before`, except deliveries still waiting to be booked."""
        if not self.has_document_ledger():
            return 0
        cur = self.cursor()
        cur.execute("DELETE FROM dropzone_documents WHERE doc_date < %s AND status <> 'pending'", (before,))
        return cur.rowcount

    def deliveries_on(self, day) -> list:
        """
        Every product line of the DDTs delivered on `day`, in pieces (qty x rapp, as
        booked). `booked` tells whether it is already counted in stock.
        """
        if not self.has_document_ledger():
            return []
        cur = self.cursor()
        cur.execute("""
            SELECT d.doc_number, d.status = 'applied' AS booked,
                   (l->>'cod')::int AS cod, (l->>'v')::int AS v,
                   (l->>'qty')::int * COALESCE(NULLIF(p.rapp, 0), 1) AS pieces
            FROM dropzone_documents d
            CROSS JOIN LATERAL jsonb_array_elements(d.lines) AS l
            LEFT JOIN products p ON p.cod = (l->>'cod')::int AND p.v = (l->>'v')::int
            WHERE d.doc_type = 'BOL' AND d.delivery_date = %s AND d.status IN ('pending', 'applied')
        """, (day,))
        return cur.fetchall()

    def has_verified_products(self, settore) -> bool:
        cur = self.cursor()
        cur.execute("""
            SELECT 1 FROM product_stats ps
            JOIN products p ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.settore = %s AND ps.verified = TRUE
            LIMIT 1
        """, (settore,))
        return cur.fetchone() is not None

    def skip_due_deliveries(self, settore, today) -> int:
        cur = self.cursor()
        cur.execute("""
            UPDATE dropzone_documents SET status = 'skipped'
            WHERE status = 'pending' AND settore = %s AND delivery_date <= %s
        """, (settore, today))
        return cur.rowcount

    def apply_due_deliveries(self, settore, today) -> list:
        """
        Book every pending DDT of this settore whose delivery date has come.
        Each document commits on its own, together with its ledger status, so a
        crash can never leave a delivery half-applied or applied twice.
        """
        cur = self.cursor()
        cur.execute("""
            SELECT doc_key FROM dropzone_documents
            WHERE status = 'pending' AND settore = %s AND delivery_date <= %s
            ORDER BY delivery_date, doc_key
        """, (settore, today))
        keys = [r["doc_key"] for r in cur.fetchall()]

        applied = []
        for key in keys:
            with self.transaction() as tcur:
                # SKIP LOCKED: an overlapping run on the same supermarket moves on
                tcur.execute("""
                    SELECT doc_key, doc_number, delivery_date, lines FROM dropzone_documents
                    WHERE doc_key = %s AND status = 'pending'
                    FOR UPDATE SKIP LOCKED
                """, (key,))
                doc = tcur.fetchone()
                if doc is None:
                    continue
                report = self._book_delivery(tcur, doc["lines"] or [], doc["delivery_date"], today)
                tcur.execute("""
                    UPDATE dropzone_documents SET status = 'applied', applied_at = now()
                    WHERE doc_key = %s
                """, (key,))
            logger.info(f"[DDT] {key} booked for {doc['delivery_date']}: updated={report['updated']} "
                        f"not_found={len(report['not_found'])} unverified={len(report['unverified_products'])}")
            applied.append({"doc_key": key, "doc_number": doc["doc_number"],
                            "delivery_date": doc["delivery_date"], "report": report})
        return applied

    def _book_delivery(self, cur, lines: list, delivery_date, today) -> dict:
        """
        Add each line's qty x rapp to stock, and to the bought_sets day and
        bought_last_24 month the delivery belongs to — not always slot 0, since a
        late-published DDT can describe goods that arrived days ago.
        """
        updated = 0
        not_found = []
        unverified_products = []

        for line in lines:
            cod, v, qty = line["cod"], line["v"], line["qty"]
            cur.execute("""
                SELECT ps.bought_last_24, ps.bought_sets, ps.stock, ps.last_update_bought,
                       ps.last_update_sold, ps.verified, p.descrizione, p.rapp
                FROM product_stats ps
                JOIN products p ON p.cod = ps.cod AND p.v = ps.v
                WHERE ps.cod = %s AND ps.v = %s
                FOR UPDATE OF ps
            """, (cod, v))
            row = cur.fetchone()
            if not row:
                not_found.append({"cod": cod, "v": v, "descrizione": line.get("descrizione", "")})
                continue

            actual_qty = qty * int(row["rapp"] or 1)

            bought_array = row["bought_last_24"] if isinstance(row["bought_last_24"], list) else []
            last_bought = row["last_update_bought"]
            if not last_bought or (last_bought.year, last_bought.month) != (today.year, today.month):
                bought_array.insert(0, 0)
            months_back = (today.year - delivery_date.year) * 12 + today.month - delivery_date.month
            if months_back < 24:
                bought_array.extend([0] * (months_back + 1 - len(bought_array)))
                bought_array[months_back] = (bought_array[months_back] or 0) + actual_qty
            bought_array = bought_array[:24]

            # Slot 0 of bought_sets is the day of last_update_sold, same as sales_sets
            day0 = row["last_update_sold"] or today
            day_slot = max(0, (day0 - delivery_date).days)
            bought_sets = row["bought_sets"] or []
            if day_slot < 60:
                bought_sets.extend([0] * (day_slot + 1 - len(bought_sets)))
                bought_sets[day_slot] = (bought_sets[day_slot] or 0) + actual_qty

            stock = int(row["stock"] or 0) + actual_qty
            cur.execute("""
                UPDATE product_stats
                SET bought_last_24 = %s, bought_sets = %s, stock = %s, last_update_bought = %s
                WHERE cod = %s AND v = %s
            """, (Json(bought_array), Json(bought_sets), stock, today, cod, v))
            updated += 1

            if not row["verified"]:
                unverified_products.append({"cod": cod, "v": v, "descrizione": row["descrizione"], "qty": actual_qty})

        return {"updated": updated, "not_found": not_found, "errors": [],
                "unverified_products": unverified_products}

    def verified_pairs(self, settore, pairs) -> set:
        """The (cod, v) pairs among `pairs` that are verified products of this settore."""
        pairs = list(pairs)
        if not pairs:
            return set()
        cur = self.cursor()
        placeholders = ','.join(['(%s,%s)'] * len(pairs))
        cur.execute(f"""
            SELECT ps.cod, ps.v FROM product_stats ps
            JOIN products p ON p.cod = ps.cod AND p.v = ps.v
            WHERE (ps.cod, ps.v) IN ({placeholders})
              AND p.settore = %s AND ps.verified = TRUE
        """, [x for pair in pairs for x in pair] + [settore])
        return {(r["cod"], r["v"]) for r in cur.fetchall()}

    def deduct_credit_note(self, lines) -> int:
        """Take each approved credit-note qty off stock, all or nothing."""
        with self.transaction() as cur:
            for cod, v, qty in lines:
                cur.execute(
                    "UPDATE product_stats SET stock = COALESCE(stock, 0) - %s WHERE cod = %s AND v = %s",
                    (qty, cod, v),
                )
        return len(lines)

    def rollover_bought_last_24(self) -> int:
        """
        On month rollover: prepend a 0 to bought_last_24 for every product
        whose last_update_bought is in a previous month, and set
        last_update_bought = today so subsequent deliveries this month
        correctly accumulate into slot [0].
        Returns the number of rows updated.
        """
        today = date.today()
        cur = self.cursor()
        cur.execute("""
            UPDATE product_stats
            SET
                bought_last_24 = jsonb_build_array(0) || COALESCE(
                    jsonb_path_query_array(bought_last_24, '$[0 to 22]'),
                    '[]'::jsonb
                ),
                last_update_bought = %s
            WHERE last_update_bought IS NOT NULL
              AND EXTRACT(MONTH FROM last_update_bought) != EXTRACT(MONTH FROM CURRENT_DATE)
              AND bought_last_24 IS NOT NULL
              AND jsonb_typeof(bought_last_24) = 'array'
              AND verified = TRUE
        """, (today,))
        updated = cur.rowcount
        self.conn.commit()
        return updated

    def rollover_sold_last_24(self) -> int:
        """
        On month rollover: prepend a 0 to sold_last_24 for every product with
        sales history, opening a fresh slot for the new month.
        """
        cur = self.cursor()
        cur.execute("""
            UPDATE product_stats
            SET sold_last_24 = jsonb_build_array(0) || COALESCE(
                    jsonb_path_query_array(sold_last_24, '$[0 to 22]'),
                    '[]'::jsonb
                ),
                price_last_24 = (
                    ARRAY[(SELECT e.price_std FROM economics e
                           WHERE e.cod = product_stats.cod AND e.v = product_stats.v)::real]
                    || COALESCE(price_last_24, ARRAY[]::real[])
                )[1:24],
                cost_last_24 = (
                    ARRAY[(SELECT e.cost_std FROM economics e
                           WHERE e.cod = product_stats.cod AND e.v = product_stats.v)::real]
                    || COALESCE(cost_last_24, ARRAY[]::real[])
                )[1:24]
            WHERE sold_last_24 IS NOT NULL
              AND jsonb_typeof(sold_last_24) = 'array'
        """)
        updated = cur.rowcount
        self.conn.commit()
        return updated

    # --- Losses ---

    def get_cod_v_by_ean(self, ean: str):
        """Returns dict with cod, v, settore, descrizione for the given EAN, or None if not found."""
        cur = self.cursor()
        cur.execute("SELECT cod, v, settore, descrizione FROM products WHERE ean=%s", (ean,))
        row = cur.fetchone()
        return dict(row) if row else None

    def register_losses(self, cod: int, v: int, delta: int, type: str, spread_days: int = 1):
        """
        Register a loss event (broken, expired, internal, stolen, shrinkage).
        Stores [[qty, cost], ...] arrays in extra_losses, max 24 months.
        Auto-creates the extra_losses row if missing.

        spread_days applies only to type="internal", the one loss that counts as
        depletion through use and so reaches sales_sets. Pass the gap between this
        rilevazione and the previous one, since a batch covers several days; callers
        correcting a single product leave it at 1.
        """
        allowed = ("broken", "expired", "internal", "stolen", "shrinkage")
        delta = int(delta)
        if type not in allowed:
            raise ValueError(f"Invalid type '{type}'. Allowed: {allowed}")

        cur = self.cursor()

        if type == "internal":
            cur.execute("SELECT sales_sets FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
            ss_row = cur.fetchone()
            if ss_row:
                sales_sets = ss_row["sales_sets"] or []
                # Start at slot 1: slot 0 is today and each sync rewrites it wholesale.
                days = max(1, min(int(spread_days), self.LOSS_MAX_SPREAD_DAYS))
                while len(sales_sets) < 1 + days:
                    sales_sets.append(0)
                base, rem = divmod(delta, days)
                for i in range(days):
                    # Remainder lands on the most recent days
                    sales_sets[1 + i] += base + (1 if i < rem else 0)
                cur.execute(
                    "UPDATE product_stats SET sales_sets=%s WHERE cod=%s AND v=%s",
                    (Json(sales_sets), cod, v)
                )

        cur.execute("SELECT 1 FROM products WHERE cod=%s AND v=%s", (cod, v))
        if cur.fetchone() is None:
            raise ValueError(f"Product {cod}.{v} not found in products table")

        cur.execute("SELECT cost_std FROM economics WHERE cod=%s AND v=%s", (cod, v))
        cost_row = cur.fetchone()
        current_cost = float(cost_row['cost_std']) if cost_row and cost_row['cost_std'] else 0.0

        cur.execute(
            f"SELECT {type}, {type}_updated FROM extra_losses WHERE cod=%s AND v=%s",
            (cod, v)
        )
        row = cur.fetchone()
        today = date.today()

        if row is None:
            cur.execute(
                f"INSERT INTO extra_losses (cod, v, {type}, {type}_updated) VALUES (%s, %s, %s, %s)",
                (cod, v, Json([[delta, current_cost]]), today)
            )
            self.conn.commit()
            self.adjust_stock(cod, v, -delta)
            return {"action": "new_entry", "cod": cod, "v": v, "delta": delta, "cost": current_cost}

        existing_json = row[type]
        existing_updated = row[f"{type}_updated"]

        if not existing_json or existing_updated is None:
            cur.execute(
                f"UPDATE extra_losses SET {type}=%s, {type}_updated=%s WHERE cod=%s AND v=%s",
                (Json([[delta, current_cost]]), today, cod, v)
            )
            self.conn.commit()
            self.adjust_stock(cod, v, -delta)
            return {"action": "initialized_null", "cod": cod, "v": v, "delta": delta, "cost": current_cost}

        arr = existing_json
        if not isinstance(arr, list):
            raise ValueError(f"extra_losses.{type} for {cod}.{v} is not a JSON array")

        if not isinstance(existing_updated, date):
            raise ValueError(f"extra_losses.{type}_updated for {cod}.{v} has unexpected type")

        months_passed = (today.year - existing_updated.year) * 12 + (today.month - existing_updated.month)

        if months_passed == 0:
            old_qty = arr[0][0] if arr and isinstance(arr[0], list) else arr[0]
            arr[0] = [(arr[0][0] if isinstance(arr[0], list) else arr[0]) + delta, current_cost]
            self.adjust_stock(cod, v, -int(delta))
            cur.execute(
                f"UPDATE extra_losses SET {type}=%s, {type}_updated=%s WHERE cod=%s AND v=%s",
                (Json(arr[:24]), today, cod, v)
            )
            self.conn.commit()
            return {"action": "same_month_update", "cod": cod, "v": v, "old_qty": old_qty, "change": delta, "cost": current_cost}

        # New month(s): convert old format entries, prepend zeros for skipped months
        converted_arr = [
            item if (isinstance(item, list) and len(item) == 2) else [item, current_cost]
            for item in arr
        ]
        zeros = [[0, current_cost] for _ in range(max(0, months_passed - 1))]
        new_arr = [[delta, current_cost]] + zeros + converted_arr
        new_arr = new_arr[:24]

        cur.execute(
            f"UPDATE extra_losses SET {type}=%s, {type}_updated=%s WHERE cod=%s AND v=%s",
            (Json(new_arr), today, cod, v)
        )
        self.conn.commit()
        self.adjust_stock(cod, v, -delta)
        return {
            "action": "months_passed_insert",
            "cod": cod,
            "v": v,
            "months_passed": months_passed,
            "new_arr_length": len(new_arr),
            "cost": current_cost,
        }

    def prepend_monthly_loss_zeros(self):
        """
        Prepend [0, 0] to every non-null loss array in extra_losses and update the
        corresponding _updated date. Called on the 1st of every month at 00:30 via Celery Beat.
        """
        cur = self.cursor()
        today = date.today()
        loss_types = ['broken', 'expired', 'internal', 'stolen', 'shrinkage']
        total_updated = 0

        for loss_type in loss_types:
            try:
                cur.execute(f"SELECT cod, v, {loss_type} FROM extra_losses WHERE {loss_type} IS NOT NULL")
                rows = cur.fetchall()

                for row in rows:
                    arr = row[loss_type]
                    if not isinstance(arr, list):
                        continue
                    new_arr = [[0, 0]] + arr
                    new_arr = new_arr[:24]
                    cur.execute(
                        f"UPDATE extra_losses SET {loss_type}=%s, {loss_type}_updated=%s WHERE cod=%s AND v=%s",
                        (Json(new_arr), today, row['cod'], row['v'])
                    )

                total_updated += len(rows)
                self.conn.commit()
                logger.info(f"Prepended monthly zero for {loss_type}: {len(rows)} rows")

            except Exception as e:
                logger.warning(f"Could not prepend zeros for {loss_type}: {e}")
                continue

        return total_updated

    # --- Catalogue Updates ---

    def import_from_CSV(self, file_path: str, settore: str):
        """
        Import products from a CSV file into the given settore.
        Updates existing entries or inserts new ones.
        """
        print(f"Importing from '{file_path}' into settore '{settore}'...")

        df = pd.read_csv(file_path, sep=";", encoding="utf-8")

        COD_COLS  = "Code"
        V_COLS    = "Variant"
        DESC_COLS = "Description"
        RAPP_COLS = "Multiplier"
        PZ_COLS   = "Package"
        DISP_COLS = "Availability"
        COST_COLS = "Cost"
        PRICE_COLS = "Price"
        REP_COLS  = "Category"
        IVA_COLS  = "Iva"

        df = df[pd.to_numeric(df[COD_COLS], errors="coerce").notna()]
        df[COD_COLS] = df[COD_COLS].astype(int)
        df[V_COLS]   = df[V_COLS].fillna(0).astype(int)
        df = df.drop_duplicates(subset=[COD_COLS, V_COLS], keep="first")

        prod_rows = []
        econ_rows = []
        for _, row in df.iterrows():
            cod         = int(row[COD_COLS])
            v           = int(row[V_COLS]) if not pd.isna(row[V_COLS]) else 0
            descrizione = str(row[DESC_COLS]).strip() if DESC_COLS in df.columns else ""
            pz_x_collo  = int(row[PZ_COLS]) if PZ_COLS in df.columns and not pd.isna(row[PZ_COLS]) else None
            disponibilita = str(row[DISP_COLS]).strip() if DISP_COLS in df.columns else "Si"
            cost        = float(row[COST_COLS]) if COST_COLS in df.columns else None
            price       = float(row[PRICE_COLS]) if PRICE_COLS in df.columns else None
            category    = str(row[REP_COLS]).strip() if REP_COLS in df.columns else ""
            iva         = int(float(row[IVA_COLS])) if IVA_COLS in df.columns and not pd.isna(row[IVA_COLS]) else None

            rapp = None
            if RAPP_COLS in df.columns and not pd.isna(row[RAPP_COLS]):
                val = row[RAPP_COLS]
                try:
                    num = float(val)
                    if not num.is_integer():
                        print(f"Warning: float value {val} in RAPP_COLS for code {cod}. Skipping.")
                        continue
                    rapp = int(num)
                except ValueError:
                    print(f"Warning: invalid RAPP_COLS value '{val}' for code {cod}. Skipping.")
                    continue

            prod_rows.append((cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita))
            econ_rows.append((cod, v, price, cost, None, None, None, None, category, iva))

        cur = self.cursor()
        cur.executemany("""
            INSERT INTO products (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(cod, v) DO UPDATE SET
                descrizione   = excluded.descrizione,
                rapp          = excluded.rapp,
                pz_x_collo    = excluded.pz_x_collo,
                disponibilita = excluded.disponibilita,
                first_added_at = CASE
                    WHEN products.disponibilita = 'No' AND excluded.disponibilita = 'Si'
                    THEN CURRENT_DATE
                    ELSE products.first_added_at
                END
        """, prod_rows)

        cur.executemany("""
            INSERT INTO economics
                (cod, v, price_std, cost_std, price_s, cost_s, sale_start, sale_end, category, iva)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(cod, v) DO UPDATE SET
                price_std = CASE
                    WHEN economics.sale_start IS NOT NULL
                     AND economics.sale_end   IS NOT NULL
                     AND CURRENT_DATE <= economics.sale_end
                    THEN economics.price_std
                    ELSE excluded.price_std
                END,
                cost_std = CASE
                    WHEN economics.sale_start IS NOT NULL
                     AND economics.sale_end   IS NOT NULL
                     AND CURRENT_DATE <= economics.sale_end
                    THEN economics.cost_std
                    ELSE excluded.cost_std
                END,
                category = excluded.category,
                iva = excluded.iva
        """, econ_rows)

        # Products absent from today's list are no longer available from the supplier.
        absent_count = 0
        if prod_rows:
            imported_keys = tuple((row[0], row[1]) for row in prod_rows)
            cur.execute("""
                UPDATE products
                SET disponibilita = 'No'
                WHERE settore = %s
                  AND (cod, v) NOT IN %s
                  AND disponibilita != 'No'
            """, (settore, imported_keys))
            absent_count = cur.rowcount

        self.conn.commit()
        print(f"Imported {len(prod_rows)} products into settore '{settore}'.")
        if absent_count:
            print(f"Marked {absent_count} products as unavailable (absent from new list) in settore '{settore}'.")

    def update_promos(self, promo_list):
        """
        promo_list: list of tuples (cod, v, cost_s, price_s, sale_start, sale_end)
        Returns how many items matched a product of this supermarket.
        """
        if not promo_list:
            logger.warning("[PROMOS] Empty promo_list received")
            return 0

        logger.info(f"[PROMOS] Received {len(promo_list)} items. First 3: {promo_list[:3]}")

        cur = self.cursor()
        cur.execute("SELECT cod, v FROM economics")
        existing = set((int(r["cod"]), int(r["v"])) for r in cur.fetchall())
        logger.info(f"[PROMOS] Found {len(existing)} products in economics table")

        filtered_list = [r for r in promo_list if (int(r[0]), int(r[1])) in existing]
        logger.info(f"[PROMOS] After filtering: {len(filtered_list)} items match")

        if not filtered_list:
            sample_parsed = [(r[0], r[1]) for r in promo_list[:5]]
            sample_existing = list(existing)[:5] if existing else []
            logger.warning(f"[PROMOS] No matches! Parsed sample: {sample_parsed}, DB sample: {sample_existing}")
            return 0

        cur.executemany("""
            INSERT INTO economics (cod, v, cost_s, price_s, sale_start, sale_end, price_std, cost_std, category)
            VALUES (%s, %s, %s, %s, %s, %s, 0, 0, 0)
            ON CONFLICT (cod, v) DO UPDATE SET
                price_s = EXCLUDED.price_s,
                cost_s  = EXCLUDED.cost_s,
                sale_start = CASE
                    WHEN CURRENT_DATE BETWEEN economics.sale_start AND economics.sale_end
                    THEN economics.sale_start
                    ELSE EXCLUDED.sale_start
                END,
                sale_end = CASE
                    WHEN CURRENT_DATE BETWEEN economics.sale_start AND economics.sale_end
                    THEN GREATEST(economics.sale_end, EXCLUDED.sale_end)
                    ELSE EXCLUDED.sale_end
                END
        """, filtered_list)

        self.conn.commit()
        return len(filtered_list)

    # --- Purge / Cleanup ---

    def flag_for_purge(self, cod: int, v: int):
        """
        If stock > 0: set purge_flag=TRUE and wait for stock to reach 0.
        If stock = 0: delete immediately via purge_product().
        The Django view handles adding to the "In fase di eliminazione" blacklist.
        """
        cur = self.cursor()
        cur.execute("SELECT ps.stock FROM product_stats ps WHERE ps.cod=%s AND ps.v=%s", (cod, v))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Product {cod}.{v} not found in database")

        stock = row['stock'] if row['stock'] is not None else 0

        if stock > 0:
            cur.execute("UPDATE products SET purge_flag=TRUE WHERE cod=%s AND v=%s", (cod, v))
            self.conn.commit()
            return {
                'action': 'flagged',
                'cod': cod,
                'v': v,
                'stock': stock,
                'message': f'Product {cod}.{v} flagged for purging (current stock: {stock})'
            }
        else:
            return self.purge_product(cod, v)

    def purge_product(self, cod: int, v: int):
        """
        Clear a product's operational data (product_stats, economics).
        The products row and extra_losses are kept: losses are permanent economic records
        that fade naturally over time via prepend_monthly_loss_zeros.
        """
        cur = self.cursor()
        deleted_from = []

        for table in ('product_stats', 'economics'):
            cur.execute(f"DELETE FROM {table} WHERE cod=%s AND v=%s", (cod, v))
            if cur.rowcount > 0:
                deleted_from.append(table)

        cur.execute("UPDATE products SET purge_flag=FALSE WHERE cod=%s AND v=%s", (cod, v))
        self.conn.commit()

        return {
            'action': 'purged',
            'cod': cod,
            'v': v,
            'deleted_from': deleted_from,
            'message': f'Product {cod}.{v} data cleared from: {", ".join(deleted_from)}'
        }

    def check_and_purge_flagged(self):
        """Purge all flagged products whose stock has reached (or dropped below) 0."""
        cur = self.cursor()
        cur.execute("""
            SELECT p.cod, p.v
            FROM products p
            JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.purge_flag = TRUE AND ps.stock <= 0
        """)
        return [self.purge_product(row['cod'], row['v']) for row in cur.fetchall()]

    def purge_obsolete_products(self):
        """
        Delete products that are confirmed gone:
          - verified=FALSE (never confirmed in stock)
          - disponibilita='No' (unavailable from supplier)
          - stock<=0

        Called after list updates so that disponibilita is fresh.
        """
        cur = self.cursor()
        cur.execute("""
            SELECT p.cod, p.v
            FROM products p
            JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE ps.verified = FALSE
              AND p.disponibilita = 'No'
              AND ps.stock <= 0
        """)
        return [self.purge_product(row['cod'], row['v']) for row in cur.fetchall()]
