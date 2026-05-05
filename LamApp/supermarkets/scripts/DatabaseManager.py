import re
import pandas as pd
import psycopg2
import psycopg2.extras
import os
from psycopg2.extras import Json
from datetime import date, timedelta
from .helpers import Helper
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:

    # --- Connection & Cursor ---

    def __init__(self, helper: Helper, supermarket_name=None):
        self.helper = helper

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
                disponibilita TEXT CHECK(disponibilita IN ('Si','No')) DEFAULT 'Si',
                cluster TEXT,
                purge_flag BOOLEAN DEFAULT FALSE,
                ean BIGINT,
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
                stock INTEGER DEFAULT 0,
                verified BOOLEAN DEFAULT FALSE,
                minimum_stock INTEGER DEFAULT 6,
                last_update_sold DATE,
                last_update_bought DATE,
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
                cod, v, sold_last_24, bought_last_24, stock, verified, last_update_sold
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
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

    def apply_daily_vensetar_sales(self, daily_sales, sync_date):
        """
        Apply one day's sold quantities from the VENSETAR sync (runs at 06:00, data = yesterday).

        - Skips products not present in product_stats.
        - Idempotent: skips if last_update_sold already equals sync_date.
        - Same month: adds sold_qty to sold_last_24[0].
        - New month: prepends sold_qty, trims to 24.
        - sales_sets: inserts sold_qty at front, trims to 30.
        - stock: decremented by sold_qty.
        - Does NOT touch bought_last_24.
        """
        cur = self.cursor()
        updated = 0
        skipped_already = 0
        skipped_not_found = 0
        unverified_updated = 0
        unverified_products = []

        for cod, var, sold_qty in daily_sales:
            cur.execute("""
                SELECT sold_last_24, sales_sets, stock, last_update_sold, verified
                FROM product_stats
                WHERE cod=%s AND v=%s
            """, (cod, var))
            row = cur.fetchone()

            if not row:
                skipped_not_found += 1
                continue

            last_update_sold = row["last_update_sold"]

            if last_update_sold == sync_date:
                skipped_already += 1
                continue

            sold_array = row["sold_last_24"]
            sales_sets = row["sales_sets"] or []
            stock = row["stock"] or 0
            verified = bool(row["verified"])

            if not isinstance(sold_array, list):
                sold_array = [0]

            same_month = (
                last_update_sold is not None
                and last_update_sold.month == sync_date.month
                and last_update_sold.year == sync_date.year
            )
            if same_month:
                sold_array[0] = (sold_array[0] or 0) + sold_qty
            else:
                sold_array.insert(0, sold_qty)
                sold_array = sold_array[:24]

            sales_sets.insert(0, sold_qty)
            sales_sets = sales_sets[:30]

            cur.execute("""
                UPDATE product_stats
                SET sold_last_24=%s, sales_sets=%s, stock=%s, last_update_sold=%s
                WHERE cod=%s AND v=%s
            """, (Json(sold_array), Json(sales_sets), stock - sold_qty, sync_date, cod, var))

            if sold_qty > 0:
                updated += 1
            if not verified:
                unverified_updated += 1
                unverified_products.append({'cod': cod, 'v': var})

        # For verified products absent from today's VENSETAR payload, insert 0 into
        # sales_sets so the weighted-average algorithm sees the non-selling day correctly.
        payload_keys = {(cod, var) for cod, var, _ in daily_sales}
        cur.execute("""
            SELECT cod, v, sales_sets
            FROM product_stats
            WHERE verified = TRUE
              AND (last_update_sold IS NULL OR last_update_sold < %s)
        """, (sync_date,))
        for absent in cur.fetchall():
            if (absent['cod'], absent['v']) in payload_keys:
                continue
            ss = absent['sales_sets'] or []
            ss.insert(0, 0)
            ss = ss[:30]
            cur.execute("""
                UPDATE product_stats SET sales_sets=%s, last_update_sold=%s
                WHERE cod=%s AND v=%s
            """, (Json(ss), sync_date, absent['cod'], absent['v']))

        self.conn.commit()
        logger.info(
            f"[VENSETAR SYNC] schema={self.schema} "
            f"applied={updated} already_synced={skipped_already} not_in_db={skipped_not_found} "
            f"sold_but_unverified={unverified_updated}"
        )
        return {
            'applied': updated,
            'already_synced': skipped_already,
            'not_in_db': skipped_not_found,
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
                    "SELECT ps.bought_last_24, ps.stock, ps.last_update_bought, ps.verified, p.descrizione, p.rapp "
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

                cur.execute(
                    "UPDATE product_stats SET bought_last_24=%s, stock=%s, last_update_bought=%s WHERE cod=%s AND v=%s",
                    (Json(bought_array), stock + actual_qty, today - timedelta(days=1), cod, v)
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

    # --- Losses ---

    def get_cod_v_by_ean(self, ean: str):
        """Returns dict with cod, v, settore, descrizione for the given EAN, or None if not found."""
        cur = self.cursor()
        cur.execute("SELECT cod, v, settore, descrizione FROM products WHERE ean=%s", (ean,))
        row = cur.fetchone()
        return dict(row) if row else None

    def register_losses(self, cod: int, v: int, delta: int, type: str):
        """
        Register a loss event (broken, expired, internal, stolen, shrinkage).
        Stores [[qty, cost], ...] arrays in extra_losses, max 24 months.
        Auto-creates the extra_losses row if missing.
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
                sales_sets = ss_row["sales_sets"] or [0]
                if not sales_sets:
                    sales_sets = [0]
                sales_sets[0] += delta
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
        existing_updated: date = row[f"{type}_updated"]

        if existing_json is None:
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

        self.conn.commit()
        print(f"Imported {len(prod_rows)} products into settore '{settore}'.")

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
        Clear a product's operational data (product_stats, economics, extra_losses).
        The products row is kept to preserve first_added_at; it will be restored on
        the next list update if the product reappears.
        """
        cur = self.cursor()
        deleted_from = []

        for table in ('product_stats', 'economics', 'extra_losses'):
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
        """Purge all flagged products whose stock has reached 0."""
        cur = self.cursor()
        cur.execute("""
            SELECT p.cod, p.v
            FROM products p
            JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.purge_flag = TRUE AND ps.stock = 0
        """)
        return [self.purge_product(row['cod'], row['v']) for row in cur.fetchall()]

    def purge_obsolete_products(self):
        """
        Delete products that are confirmed gone:
          - verified=FALSE (never confirmed in stock)
          - disponibilita='No' (unavailable from supplier)
          - stock=0

        Called after list updates so that disponibilita is fresh.
        """
        cur = self.cursor()
        cur.execute("""
            SELECT p.cod, p.v
            FROM products p
            JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE ps.verified = FALSE
              AND p.disponibilita = 'No'
              AND ps.stock = 0
        """)
        return [self.purge_product(row['cod'], row['v']) for row in cur.fetchall()]
