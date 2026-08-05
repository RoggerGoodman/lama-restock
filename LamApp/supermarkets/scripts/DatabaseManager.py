import re
import pandas as pd
import psycopg2
import psycopg2.extras
import os
from psycopg2.extras import Json, execute_values
from datetime import date
import logging

logger = logging.getLogger(__name__)


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
                last_update_sold DATE,
                last_update_bought DATE,
                promo_lifts JSONB,
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

    def get_category_stock_value(self, category: str):
        cur = self.cursor()
        cur.execute("""
            SELECT e.cod, e.v, e.cost_std, ps.stock
            FROM economics e
            JOIN product_stats ps ON e.cod = ps.cod AND e.v = ps.v
            WHERE e.category = %s
        """, (category,))

        total_value = 0.0
        for cod, v, cost_std, stock in cur.fetchall():
            if cost_std is None or stock is None:
                continue
            total_value += float(cost_std) * int(stock)
        return round(total_value, 2)

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
        cur = self.cursor()
        cur.execute("SELECT stock FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
        row = cur.fetchone()
        if not row:
            logger.warning(f"No product_stats found for {cod}.{v}")
            return

        new_stock = (int(row["stock"]) if row["stock"] is not None else 0) + delta
        cur.execute(
            "UPDATE product_stats SET stock=%s WHERE cod=%s AND v=%s",
            (new_stock, cod, v)
        )
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
            cur.execute("""
                SELECT ps.cod, ps.v, ps.sold_last_24, ps.sales_sets, ps.stock, ps.verified
                FROM product_stats ps
                JOIN unnest(%s::int[], %s::int[]) AS t(cod, v)
                  ON ps.cod = t.cod AND ps.v = t.v
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

            stat_updates.append((cod, var, Json(sold_array), Json(ss), stock))
            applied += 1
            total_delta += delta

        if stat_updates:
            execute_values(cur, """
                UPDATE product_stats AS ps
                SET sold_last_24 = d.sold::jsonb,
                    sales_sets   = d.sets::jsonb,
                    stock        = d.stock::int
                FROM (VALUES %s) AS d(cod, var, sold, sets, stock)
                WHERE ps.cod = d.cod::int AND ps.v = d.var::int
            """, stat_updates, page_size=1000)

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

    def apply_invoice_deliveries(self, cod_v_dict: dict) -> dict:
        """
        For each (cod, v) in cod_v_dict, add the delivered quantity to:
        - bought_last_24[0] (current month total)
        - stock

        Does NOT touch sold_last_24 or sales_sets.
        """
        today = date.today()
        current_month = today.month
        updated = 0
        not_found = []
        errors = []
        unverified_products = []

        for (cod, v), item in cod_v_dict.items():
            qty = item["qty"] if isinstance(item, dict) else item
            descrizione_invoice = item.get("descrizione", "") if isinstance(item, dict) else ""
            product_key = f"{cod}.{v}"
            try:
                cur = self.cursor()
                cur.execute(
                    "SELECT ps.bought_last_24, ps.bought_sets, ps.stock, ps.last_update_bought, ps.verified, p.descrizione, p.rapp "
                    "FROM product_stats ps "
                    "JOIN products p ON p.cod = ps.cod AND p.v = ps.v "
                    "WHERE ps.cod=%s AND ps.v=%s",
                    (cod, v)
                )
                row = cur.fetchone()
                if not row:
                    logger.debug(f"apply_invoice_deliveries: {product_key} not in DB")
                    not_found.append({"cod": cod, "v": v, "descrizione": descrizione_invoice})
                    continue

                bought_array = row["bought_last_24"] or [0]
                if not isinstance(bought_array, list):
                    bought_array = [0]
                stock = int(row["stock"] or 0)
                last_update_bought = row["last_update_bought"]
                last_month = last_update_bought.month if last_update_bought else None
                verified = bool(row["verified"])
                descrizione = row["descrizione"]
                rapp = int(row["rapp"] or 1)
                actual_qty = qty * rapp

                if last_month == current_month:
                    bought_array[0] = (bought_array[0] or 0) + actual_qty
                else:
                    bought_array.insert(0, actual_qty)
                    bought_array = bought_array[:24]

                bought_sets = row["bought_sets"] or []
                if not bought_sets:
                    bought_sets = [0]
                bought_sets[0] = (bought_sets[0] or 0) + actual_qty

                cur.execute(
                    "UPDATE product_stats SET bought_last_24=%s, bought_sets=%s, stock=%s, last_update_bought=%s WHERE cod=%s AND v=%s",
                    (Json(bought_array), Json(bought_sets), stock + actual_qty, today, cod, v)
                )
                self.conn.commit()
                updated += 1
                logger.info(f"apply_invoice_deliveries: {product_key} +{qty}×{rapp}={actual_qty} → stock={stock+actual_qty}")

                if not verified:
                    unverified_products.append({"cod": cod, "v": v, "descrizione": descrizione, "qty": actual_qty})

            except Exception as e:
                logger.error(f"apply_invoice_deliveries: failed for {product_key}: {e}")
                errors.append({"cod": cod, "v": v, "error": str(e)})

        logger.info(
            f"apply_invoice_deliveries: updated={updated} "
            f"not_found={len(not_found)} errors={len(errors)} "
            f"unverified={len(unverified_products)}"
        )
        return {
            "updated": updated,
            "not_found": not_found,
            "errors": errors,
            "unverified_products": unverified_products,
        }

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
            )
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
            econ_rows.append((cod, v, price, cost, None, None, None, None, category))

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
                (cod, v, price_std, cost_std, price_s, cost_s, sale_start, sale_end, category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                category = excluded.category
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
        promo_list: list of tuples (cod, v, price_s, cost_s, sale_start, sale_end)
        """
        if not promo_list:
            logger.warning("[PROMOS] Empty promo_list received")
            return

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
            return

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
