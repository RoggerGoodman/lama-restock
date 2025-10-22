import sqlite3
import json
import pandas as pd
from datetime import datetime
from calendar import monthrange
import sqlite3
import json
from datetime import datetime
from helpers import Helper

class DatabaseManager:
    def __init__(self, helper: Helper, db_path=r"C:\Users\Ruggero\Documents\GitHub\lama-restock\Database\supermarket.db"):
        self.conn = sqlite3.connect(db_path)
        self.helper = helper
        self.conn.row_factory = sqlite3.Row  # allow dict-like access
        self.conn.execute("PRAGMA foreign_keys = ON")  # enforce relationships

    # ---------- TABLE CREATION ----------

    def create_tables(self):
        cur = self.conn.cursor()

        # Main product table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                cod INTEGER NOT NULL,
                v INTEGER NOT NULL,
                descrizione TEXT NOT NULL,
                rapp INTEGER,
                pz_x_collo INTEGER,
                settore TEXT NOT NULL,
                disponibilita TEXT CHECK(disponibilita IN ('Si','No')) DEFAULT 'Si',
                PRIMARY KEY (cod, v)
            )
        """)

        # Stats table linked by cod + v
        # No ON DELETE CASCADE — orphans will be cleaned manually
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_stats (
                cod INTEGER NOT NULL,
                v INTEGER NOT NULL,
                sold_last_24 TEXT CHECK(json_valid(sold_last_24)),
                bought_last_24 TEXT CHECK(json_valid(bought_last_24)),
                stock INTEGER DEFAULT 0,
                verified BOOLEAN DEFAULT 0,
                last_update_month INTEGER CHECK(last_update_month BETWEEN 1 AND 12),
                FOREIGN KEY (cod, v) REFERENCES products (cod, v),
                PRIMARY KEY (cod, v)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS economics (
                cod INTEGER NOT NULL,
                v INTEGER NOT NULL,
                value INTEGER NOT NULL,
                on_sale BOOLEAN DEFAULT 0,
                category TEXT NOT NULL,
                FOREIGN KEY (cod, v) REFERENCES products (cod, v),
                PRIMARY KEY (cod, v)
            )
        """)

        # helpful indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_products_settore ON products(settore)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stats_last_month ON product_stats(last_update_month)")

        self.conn.commit()
        print("Tables created or verified.")

    def close(self):
        self.conn.close()

    # ---------- PRODUCT MANAGEMENT ----------

    def add_product(self, cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita="Si"):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO products (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita))
        self.conn.commit()

    def init_product_stats(self, cod, v, sold=None, bought=None, stock=None, verified=False):
        """
        Initialize with variable-length arrays (empty if not provided).
        """
        sold_json = json.dumps(sold or [])
        bought_json = json.dumps(bought or [])
        month = datetime.now().month

        cur = self.conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO product_stats (cod, v, sold_last_24, bought_last_24, stock, verified, last_update_month)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (cod, v, sold_json, bought_json, stock, int(verified), month))
        self.conn.commit()

    def purge_settore(self, settore):
        """
        Permanently deletes all products belonging to the given settore.
        """
        cur = self.conn.cursor()
        cur.execute("DELETE FROM products WHERE settore = ?", (settore,))
        self.conn.commit()
        
    
    def purge_orphan_stats(self):
        """
        Permanently deletes all stats belonging to products without a math in the products table.
        """
        cur = self.conn.cursor()
        cur.execute("""
            DELETE FROM product_stats
            WHERE (cod, v) NOT IN (SELECT cod, v FROM products)
        """)
        self.conn.commit()

    # ---------- ARRAY UPDATE LOGIC ----------

    def _update_array_variable_length(self, array_data, current_value, previous_value, current_month, last_month):
        """
        Updates a variable-length monthly array (max 24 elements).
        """
        if last_month == current_month:
            # Same month → update only first value
            if array_data:
                array_data[0] = current_value
            else:
                array_data = [current_value]
        else:
            # New month → prepend and trim
            array_data.insert(0, current_value)
            array_data = array_data[:24]

            # Update previous month if different
            if len(array_data) > 1 and array_data[1] != previous_value:
                array_data[1] = previous_value

        return array_data

    def update_product_stats(self, cod, v, sold_update=None, bought_update=None):
        """
        Minimal, robust updater.

        - sold_update / bought_update: list/tuple with length 1 or 2:
            [current_value]  OR  [current_value, previous_value]
        - Month is always 'now' (datetime.now().month).
        - If product_stats row does not exist, it will be initialized using the provided arrays.
        - Delta logic and array-shift use the existing behavior (no full recompute).
        """
        from datetime import datetime

        if sold_update is None and bought_update is None:
            raise ValueError("At least one of sold_update or bought_update must be provided.")

        # current month (always)
        current_month = datetime.now().month

        # Normalize incoming small updates to tuples (cur_val, prev_val, month=current_month)
        def _prepare_packet(pkt):
            cur_val = pkt[0]
            prev_val = pkt[1] if len(pkt) >= 2 else None
            return (cur_val, prev_val)

        sold_pkt = _prepare_packet(sold_update)
        bought_pkt = _prepare_packet(bought_update)

        cur = self.conn.cursor()
        cur.execute("""
            SELECT sold_last_24, bought_last_24, stock, last_update_month
            FROM product_stats
            WHERE cod=? AND v=?
        """, (cod, v))
        row = cur.fetchone()

        # Load current arrays and metadata (old values)
        sold_array = json.loads(row["sold_last_24"]) if row["sold_last_24"] else []
        bought_array = json.loads(row["bought_last_24"]) if row["bought_last_24"] else []
        stock = int(row["stock"]) if row["stock"] is not None else 0
        last_month = int(row["last_update_month"])

        # --- Compute deltas using old arrays (before modification) ---
        sold_delta = 0
        bought_delta = 0

        # SOLD delta
        if sold_pkt is not None:
            cur_sold_val, prev_sold_val = sold_pkt
            if last_month == current_month:
                old_current = sold_array[0]
                sold_delta = cur_sold_val - old_current
            else:
                old_previous_stored = sold_array[0]
                sold_delta = cur_sold_val + (prev_sold_val - old_previous_stored)

        # BOUGHT delta
        if bought_pkt is not None:
            cur_bought_val, prev_bought_val = bought_pkt
            if last_month == current_month:
                old_current = bought_array[0]
                bought_delta = cur_bought_val - old_current
            else:
                old_previous_stored = bought_array[0]
                bought_delta = cur_bought_val + (prev_bought_val - old_previous_stored)

        # --- Apply array modifications using helper (preserves your shift/replace logic) ---
        
        if sold_pkt is not None:
            cur_sold_val, prev_sold_val = sold_pkt
            sold_array = self._update_array_variable_length(sold_array, cur_sold_val, prev_sold_val, current_month, last_month)
            
        if bought_pkt is not None:
            cur_bought_val, prev_bought_val = bought_pkt
            bought_array = self._update_array_variable_length(bought_array, cur_bought_val, prev_bought_val, current_month, last_month)
            
        # --- Adjust stock incrementally ---
        stock = stock + int(bought_delta) - int(sold_delta)

        # Persist changes
        cur.execute("""
            UPDATE product_stats
            SET sold_last_24=?, bought_last_24=?, stock=?, last_update_month=?
            WHERE cod=? AND v=?
        """, (json.dumps(sold_array), json.dumps(bought_array), stock, current_month, cod, v))
        self.conn.commit()

    # ---------- GETTERS ----------

    def get_product_stats(self, cod, v):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT *
            FROM product_stats
            WHERE cod=? AND v=?
        """, (cod, v))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "sold": json.loads(row["sold_last_24"]),
            "bought": json.loads(row["bought_last_24"]),
            "stock": row["stock"],
            "verified": bool(row["verified"]),
            "last_update_month": row["last_update_month"],
        }
    
    def get_stock(self, cod, v):
        cur = self.conn.cursor()
        cur.execute("SELECT stock FROM product_stats WHERE cod=? AND v=?", (cod, v))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No product_stats found for {cod}.{v}")
        return row["stock"]
    
    def get_all_stats_by_settore(self, settore):
        """
        Returns all product stats for a given 'settore'.
        Joins 'products' and 'product_stats' tables to include product details.

        Args:
            settore (str): The sector/category name.

        Returns:
            list[dict]: Each entry contains product details and corresponding stats.
        """
        cur = self.conn.cursor()
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
                ps.last_update_month
            FROM products AS p
            LEFT JOIN product_stats AS ps
                ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.settore = ?
        """, (settore,))

        rows = cur.fetchall()
        results = []

        for row in rows:
            results.append({
                "cod": row["cod"],
                "v": row["v"],
                "descrizione": row["descrizione"],
                "rapp": row["rapp"],
                "pz_x_collo": row["pz_x_collo"],
                "disponibilita": row["disponibilita"],
                "sold": json.loads(row["sold_last_24"]) if row["sold_last_24"] else [],
                "bought": json.loads(row["bought_last_24"]) if row["bought_last_24"] else [],
                "stock": row["stock"] if row["stock"] is not None else 0,
                "verified": bool(row["verified"]) if row["verified"] is not None else False,
                "last_update_month": row["last_update_month"],
            })

        return results
        
    # ---------- SETTERS ----------

    def set_stock(self, cod:int, v:int, new_stock:int):
        """
        Update the stock quantity without changing the 'verified' flag.
        Use verify_stock() if this is a human-confirmed correction.
        """
        cur = self.conn.cursor()
        cur.execute("UPDATE product_stats SET stock=? WHERE cod=? AND v=?", new_stock, cod, v)
        if cur.rowcount == 0:
            raise ValueError(f"No product_stats found for {cod}.{v}")
        self.conn.commit()

    def adjust_stock(self, cod:int, v:int, delta:int):
        """
        Increment or decrement the stock by 'delta' (can be negative).
        Also sets verified = True.
        """
        cur = self.conn.cursor()
        # Fetch current stock
        cur.execute("SELECT stock FROM product_stats WHERE cod=? AND v=?", (cod, v))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No product_stats found for {cod}.{v}")

        current_stock = int(row["stock"]) if row["stock"] is not None else 0
        new_stock = current_stock + delta

        # Update stock and mark as verified
        cur.execute(
            "UPDATE product_stats SET stock=?, verified=1 WHERE cod=? AND v=?",
            (new_stock, cod, v)
        )
        self.conn.commit()

    def verify_stock(self, cod:int, v:int, new_stock:int):
        """
        Called when a human inspects and corrects stock. Optionally set a new stock value.
        This sets verified = True. (Does not change last_update_month.)
        """
        cur = self.conn.cursor()
        cur.execute("UPDATE product_stats SET stock=?, verified=1 WHERE cod=? AND v=?", (new_stock, cod, v))
        if cur.rowcount == 0:
            raise ValueError(f"No product_stats found for {cod}.{v}")
        self.conn.commit()

        
    def import_from_excel(self, file_path: str, settore: str):
        """
        Imports products from an Excel file into the database for the given settore.
        Updates existing entries or inserts new ones.
        """
        print(f"Importing from '{file_path}' into settore '{settore}'...")

        # Step 1: Purge old entries for this settore
        # self.purge_settore(settore)

        # Step 2: Load Excel data
        df = pd.read_excel(file_path)

        # Expected column names (first occurrence if duplicates exist)
        COD_COLS = "Cod."
        V_COLS = "V."
        DESC_COLS = "Articolo"
        RAPP_COLS = "Rapp"
        PZ_COLS = "Pz.x.Collo"
        DISP_COLS = "Disponibilita"

    # Skip rows without a numeric Cod.
        df = df[pd.to_numeric(df[COD_COLS], errors="coerce").notna()]

        # Convert Cod. and V. to integers
        df[COD_COLS] = df[COD_COLS].astype(int)
        df[V_COLS] = df[V_COLS].fillna(0).astype(int)

        # Drop duplicates
        df = df.drop_duplicates(subset=[COD_COLS, V_COLS], keep="first")

        # Step 4: Prepare rows for bulk insert
        rows = []
        for _, row in df.iterrows():
            cod = int(row[COD_COLS])
            v = int(row[V_COLS]) if not pd.isna(row[V_COLS]) else 0
            descrizione = str(row[DESC_COLS]).strip() if DESC_COLS in df.columns else ""
            rapp = int(row[RAPP_COLS]) if RAPP_COLS in df.columns and not pd.isna(row[RAPP_COLS]) else None
            pz_x_collo = int(row[PZ_COLS]) if PZ_COLS in df.columns and not pd.isna(row[PZ_COLS]) else None
            disponibilita = str(row[DISP_COLS]).strip() if DISP_COLS in df.columns else "Si"

            rows.append((cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita))

        # Step 5: Insert or update all at once
        cur = self.conn.cursor()
        cur.executemany("""
            INSERT INTO products 
            (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cod, v) DO UPDATE SET
                descrizione = excluded.descrizione,
                rapp = excluded.rapp,
                pz_x_collo = excluded.pz_x_collo,
                disponibilita = excluded.disponibilita
        """, rows)

        self.conn.commit()

        # Step 6: Remove stats of deleted products
        # self.purge_orphan_stats()

        print(f"Imported {len(rows)} products into settore '{settore}'.")

    def estimate_and_update_stock_for_settore(self, settore, batch_commit=100):
        """
        For every product in `settore`:
        - compute estimated stock using helper functions
        - update product_stats.stock and set verified = 0 (since it's an automatic estimate)
        Args:
        settore (str): sector to process
        helper: object exposing the methods you provided:
            calculate_weighted_avg_sales(array),
            detect_dead_periods(bought_array, sold_array) -> (bought_array, sold_array),
            calculate_stock(final_array_sold=..., final_array_bought=...),
            calculate_stock_oscillation(final_array_bought, final_array_sold, avg_daily_sales),
            calculate_max_stock(bought_slice, sold_slice)
        batch_commit (int): number of rows to update before committing (default 100)
        Returns:
        dict report with counts and simple stats
        """
        cur = self.conn.cursor()
        cur.execute("""
            SELECT p.cod, p.v, ps.sold_last_24, ps.bought_last_24, ps.stock
            FROM products p
            LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.settore = ?
        """, (settore,))
        rows = cur.fetchall()

        report = {
            "total_products": len(rows),
            "updated": 0,
            "skipped_no_stats": 0,
            "errors": 0
        }

        to_commit = 0

        for r in rows:
            cod = r["cod"]
            v = r["v"]
            stock = r["stock"]
            if stock != None: 
                continue
            try:
                sold_json = r["sold_last_24"]
                bought_json = r["bought_last_24"]

                # If there are no arrays at all, skip (nothing to estimate)
                if not sold_json and not bought_json:
                    report["skipped_no_stats"] += 1
                    continue

                sold = json.loads(sold_json) if sold_json else []
                bought = json.loads(bought_json) if bought_json else []

                # ensure lists
                sold = list(sold)
                bought = list(bought)

                # 1) weighted average daily sales
                try:
                    if len(sold) > 0:
                        avg_daily_sales = self.helper.calculate_weighted_avg_sales(sold)
                except Exception as e:
                    # if helper fails, skip this product
                    print(f"Helper calculate_weighted_avg_sales failed for {cod}.{v}: {e}")
                    report["errors"] += 1
                    continue

                # 2) detect dead periods if we have enough history
                if len(bought) > 3 and len(sold) > 3:
                    try:
                        bought, sold = self.helper.detect_dead_periods(bought, sold)
                        # ensure lists after detection
                        bought = list(bought)
                        sold = list(sold)
                    except Exception as e:
                        print(f"Helper detect_dead_periods failed for {cod}.{v}: {e}")
                        report["errors"] += 1
                        continue

                # 3) choose calculation method
                try:
                    if len(bought) <= 15:
                        stock_est = self.helper.calculate_stock(final_array_sold=sold, final_array_bought=bought)
                    else:
                        so = self.helper.calculate_stock_oscillation(bought, sold, avg_daily_sales)
                        # take recent 9 months (or fewer if not available) for max stock calc
                        bought_recent = bought[:9]
                        sold_recent = sold[:9]
                        ms = self.helper.calculate_max_stock(bought_recent, sold_recent)
                        stock_est = max(so, ms)
                except Exception as e:
                    print(f"Stock calculation failed for {cod}.{v}: {e}")
                    report["errors"] += 1
                    continue

                # sanitize result -> integer, non-negative
                if stock_est is None:
                    stock_est = 0
                stock_est = max(0, stock_est)

                # 4) update DB (set verified = 0 because this is an automatic estimate)
                cur.execute("""
                    UPDATE product_stats
                    SET stock = ?, verified = 0
                    WHERE cod = ? AND v = ?
                """, (stock_est, cod, v))
                to_commit += 1
                report["updated"] += 1

                # commit in batches
                if to_commit >= batch_commit:
                    self.conn.commit()
                    to_commit = 0

            except Exception as e:
                print(f"Unexpected error processing {cod}.{v}: {e}")
                report["errors"] += 1
                continue

        # final commit
        if to_commit > 0:
            self.conn.commit()

        return report