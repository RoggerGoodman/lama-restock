import re
import pandas as pd
import psycopg2
import psycopg2.extras
import os
from psycopg2.extras import Json
from datetime import date
from .helpers import Helper

class DatabaseManager:
    def __init__(self, helper: Helper, supermarket_name=None):
        """
        Initialize DatabaseManager with PostgreSQL.
        
        Args:
            helper: Helper instance
            db_path: (deprecated) kept for compatibility
            supermarket_name: Name of supermarket (determines schema)
        """
        self.helper = helper
        
        # Determine schema name
        if supermarket_name:
            self.schema = self._sanitize_schema_name(supermarket_name)
        else:
            self.schema = "public"
        
        self.conn = psycopg2.connect(
            host=os.environ.get('PG_HOST', 'localhost'),
            database=os.environ.get('PG_DATABASE', 'lamarestock_products'),
            user=os.environ.get('PG_USER', 'lamauser'),
            password=os.environ.get('PG_PASSWORD', ''),
            options=f'-c search_path={self.schema},public'
        )
        
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def cursor(self):
        """Get a RealDictCursor for dict-like row access"""
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def _sanitize_schema_name(self, name):
        """Convert name to valid PostgreSQL schema name."""
        clean = re.sub(r'[^\w\s-]', '', name.lower())
        clean = re.sub(r'[-\s]+', '_', clean)
        return clean

    # ---------- TABLE CREATION ----------

    def create_tables(self):
        cur = self.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
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
                cluster TEXT,
                purge_flag BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (cod, v)
            )
        """)

        # Stats table - Remove json_valid, PostgreSQL validates JSON automatically
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_stats (
                cod INTEGER NOT NULL,
                v INTEGER NOT NULL,
                sold_last_24 JSONB,
                bought_last_24 JSONB,
                sales_sets JSONB,
                stock INTEGER DEFAULT 0,
                verified BOOLEAN DEFAULT FALSE,
                last_update DATE,
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
                FOREIGN KEY (cod, v) REFERENCES products (cod, v),
                PRIMARY KEY (cod, v)
            )
        """)
        # Indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_products_settore ON products(settore)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_products_cluster ON products(cluster)")

        self.conn.commit()
        print(f"Tables created/verified in schema: {self.schema}")


    def close(self):
        self.conn.close()

    # ---------- PRODUCT MANAGEMENT ----------

    def add_product(self, cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita="Si"):
        cur = self.cursor()
        cur.execute("""
            INSERT INTO products (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod, v) DO NOTHING
        """, (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita))
        self.conn.commit()

    def init_product_stats(self, cod, v, sold=None, bought=None, stock=None, verified=False):
        """
        Initialize with variable-length arrays (empty if not provided).
        """
        sold_array = sold or []
        bought_array = bought or []
        today = date.today() 

        cur = self.cursor()
        cur.execute("""
            INSERT INTO product_stats (
                cod, v, sold_last_24, bought_last_24, stock, verified, last_update
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod, v) DO NOTHING
        """, (cod, v, sold_array, bought_array, stock, bool(verified), today))

        self.conn.commit()

    # ---------- ARRAY UPDATE LOGIC ----------

    def _update_array_variable_length(self, array_data:list, current_value, previous_value, current_month, last_update_month):
        """
        Updates a variable-length monthly array (max 24 elements).
        """
        if last_update_month == current_month:
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
        - last_update is always 'today' (DATE).
        - If product_stats row does not exist, it will be initialized using the provided arrays.
        - Delta logic and array-shift use the existing behavior (no full recompute).
        """

        if sold_update is None and bought_update is None:
            raise ValueError("At least one of sold_update or bought_update must be provided.")

        # --- Prepare helpers ---
        def _prepare_packet(pkt):
            cur_val = pkt[0]
            prev_val = pkt[1] if len(pkt) >= 2 else None
            return (cur_val, prev_val)

        sold_pkt = _prepare_packet(sold_update)
        bought_pkt = _prepare_packet(bought_update)

        cur = self.cursor()
        cur.execute("""
            SELECT sold_last_24, bought_last_24, stock, last_update, sales_sets
            FROM product_stats
            WHERE cod=%s AND v=%s
        """, (cod, v))
        row = cur.fetchone()

        if not row:
            # Auto-initialize if missing
            self.init_product_stats(cod, v, sold=[], bought=[], stock=0, verified=False)
            cur.execute("""
                SELECT sold_last_24, bought_last_24, stock, last_update, sales_sets
                FROM product_stats
                WHERE cod=%s AND v=%s
            """, (cod, v))
            row = cur.fetchone()

        # --- Load arrays and previous metadata ---
        sold_array = row["sold_last_24"] or []
        bought_array = row["bought_last_24"] or []
        stock = int(row["stock"]) if row["stock"] is not None else 0
        sales_sets = row["sales_sets"] or []
        last_update_date = row["last_update"]
        current_date = date.today()

        # Use month numbers for continuity in your delta logic
        last_update_month = last_update_date.month
        current_month = current_date.month

        # --- Compute deltas using old arrays ---
        sold_delta = 0
        bought_delta = 0

        if sold_pkt is not None:
            days_since = (current_date - last_update_date).days if last_update_date else 0
            cur_sold_val, prev_sold_val = sold_pkt
            if last_update_month == current_month:
                old_current = sold_array[0] if sold_array else 0
                sold_delta = cur_sold_val - old_current
            else:
                old_previous_stored = sold_array[0] if sold_array else 0
                sold_delta = cur_sold_val + (prev_sold_val - old_previous_stored)
           
            if days_since > 0:
                pair = [int(sold_delta), int(days_since)]
                sales_sets.insert(0, pair)
                # Keep last 10
                sales_sets = sales_sets[-10:]

            

        if bought_pkt is not None:
            cur_bought_val, prev_bought_val = bought_pkt
            if last_update_month == current_month:
                old_current = bought_array[0] if bought_array else 0
                bought_delta = cur_bought_val - old_current
            else:
                old_previous_stored = bought_array[0] if bought_array else 0
                bought_delta = cur_bought_val + (prev_bought_val - old_previous_stored)

        # --- Apply array modifications using your existing helper ---
        if sold_pkt is not None:
            cur_sold_val, prev_sold_val = sold_pkt
            sold_array = self._update_array_variable_length(
                sold_array, cur_sold_val, prev_sold_val, current_month, last_update_month
            )

        if bought_pkt is not None:
            cur_bought_val, prev_bought_val = bought_pkt
            bought_array = self._update_array_variable_length(
                bought_array, cur_bought_val, prev_bought_val, current_month, last_update_month
            )

        # --- Adjust stock incrementally ---
        stock = stock + int(bought_delta) - int(sold_delta)

        # --- Persist changes ---
        cur.execute("""
            UPDATE product_stats
            SET sold_last_24=%s, bought_last_24=%s, stock=%s, last_update=%s, sales_sets=%s
            WHERE cod=%s AND v=%s
        """, (
            Json(sold_array),
            Json(bought_array),
            stock,
            current_date,
            Json(sales_sets),
            cod, v
        ))

        self.conn.commit()

    # ---------- GETTERS ----------

    def get_product_stats(self, cod, v):
        cur = self.cursor()
        cur.execute("""
            SELECT *
            FROM product_stats
            WHERE cod=%s AND v=%s
        """, (cod, v))
        row = cur.fetchone()
        if not row:
            return None

        return {
            "sold": row["sold_last_24"] or [],
            "bought": row["bought_last_24"] or [],
            "stock": row["stock"] or 0,
            "verified": bool(row["verified"]),
            "last_update": row["last_update"],
        }
    
    def get_stock(self, cod, v):
        cur = self.cursor()
        cur.execute("SELECT stock FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
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
                ps.last_update
            FROM products AS p
            LEFT JOIN product_stats AS ps
                ON p.cod = ps.cod AND p.v = ps.v
            WHERE p.settore = %s
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
                "sold": row["sold_last_24"] or [],
                "bought": row["bought_last_24"] or [],
                "stock": row["stock"] if row["stock"] is not None else 0,
                "verified": bool(row["verified"]) if row["verified"] is not None else False,
                "last_update": row["last_update"],
            })

        return results
    
    def get_category_stock_value(self, category: str):
        cur = self.cursor()
        cur.execute("""
            SELECT e.cod, e.v, e.cost_std, ps.stock
            FROM economics e
            JOIN product_stats ps
            ON e.cod = ps.cod AND e.v = ps.v
            WHERE e.category = %s;
        """, (category,))
        
        rows = cur.fetchall()

        total_value = 0.0
        for cod, v, cost_std, stock in rows:
            if cost_std is None or stock is None:
                continue

            total_value += float(cost_std) * int(stock)

        return round(total_value, 2)

        
    # ---------- SETTERS ----------

    def set_stock(self, cod:int, v:int, new_stock:int):
        """
        Update the stock quantity without changing the 'verified' flag.
        Use verify_stock() if this is a human-confirmed correction.
        """
        cur = self.cursor()
        cur.execute("UPDATE product_stats SET stock=%s WHERE cod=%s AND v=%s", new_stock, cod, v)
        if cur.rowcount == 0:
            raise ValueError(f"No product_stats found for {cod}.{v}")
        self.conn.commit()

    def set_verified_false(self, cod:int, v:int):
        """
        Update the 'verified' flag to false.
        """
        cur = self.cursor()
        cur.execute("UPDATE product_stats SET verified=FALSE WHERE cod=%s AND v=%s", (cod, v))
        if cur.rowcount == 0:
            raise ValueError(f"No product_stats found for {cod}.{v}")
        self.conn.commit()

    def register_losses(self, cod: int, v: int, delta: int, type: str):
        """
        Registers a type of loss (broken, expired, internal).
        AUTO-CREATES extra_losses entry if missing.
        """
        allowed = ("broken", "expired", "internal")
        delta = int(delta)
        if type not in allowed:
            raise ValueError(f"Invalid type '{type}'. Allowed: {allowed}")

        cur = self.cursor()

        # 1) Check product exists in products table
        cur.execute("SELECT 1 FROM products WHERE cod=%s AND v=%s", (cod, v))
        if cur.fetchone() is None:
            raise ValueError(f"Product {cod}.{v} not found in products table")

        # 2) Get or create extra_losses entry
        cur.execute(
            f"SELECT {type}, {type}_updated FROM extra_losses WHERE cod=%s AND v=%s",
            (cod, v)
        )
        row = cur.fetchone()

        today = date.today()


        # If no entry exists, create it
        if row is None:
            json_array = Json([delta])
            cur.execute(
                f"""
                INSERT INTO extra_losses (cod, v, {type}, {type}_updated)
                VALUES (%s, %s, %s, %s)
                """,
                (cod, v, Json([delta]), today)
            )
            self.conn.commit()
            self.adjust_stock(cod, v, -delta)
            return {"action": "new_entry", "cod": cod, "v": v, "delta": delta}

        # 3) Entry exists - get current data
        existing_json = row[type]
        existing_updated:date = row[f"{type}_updated"]

        # Handle case where column exists but is NULL
        if existing_json is None:
            # Column is NULL - treat as new entry
            json_array = Json([delta])
            cur.execute(
                f"UPDATE extra_losses SET {type} = %s, {type}_updated = %s WHERE cod = %s AND v = %s",
                (json_array, today, cod, v)
            )
            self.conn.commit()
            self.adjust_stock(cod, v, -delta)
            return {"action": "initialized_null", "cod": cod, "v": v, "delta": delta}

        # Parse existing JSON
        arr = existing_json
        if not isinstance(arr, list):
            raise ValueError(f"extra_losses.{type} for {cod}.{v} is not a JSON array")

        # Parse last update date
        if isinstance(existing_updated, date):
            last_update = existing_updated
        else:
            raise ValueError(f"extra_losses.{type}_updated for {cod}.{v} has unexpected type")

        # Calculate months passed
        months_passed = (today.year - last_update.year) * 12 + (today.month - last_update.month)

        # SAME MONTH: Overwrite first element
        if months_passed == 0:
            old_first = arr[0]
            new_first = delta
            
            if old_first != new_first:
                arr[0] = new_first
                difference = new_first - int(old_first)
                self.adjust_stock(cod, v, -int(difference))
                
                json_out = Json(arr[:24])
                cur.execute(
                    f"UPDATE extra_losses SET {type} = %s, {type}_updated = %s WHERE cod = %s AND v = %s",
                    (json_out, today, cod, v)
                )
                self.conn.commit()
                
                return {
                    "action": "same_month_overwrite",
                    "cod": cod,
                    "v": v,
                    "old_first": old_first,
                    "new_first": new_first,
                    "difference": difference
                }
        
        # NEW MONTH(S): Insert new month(s) with zeros for skipped months
        else:
            new_delta = delta
            zeros = [0] * max(0, months_passed - 1)
            new_arr = [new_delta] + zeros + arr
            new_arr = new_arr[:24]  # Trim to 24 months
            
            json_out = Json(new_arr)
            cur.execute(
                f"UPDATE extra_losses SET {type} = %s, {type}_updated = %s WHERE cod = %s AND v = %s",
                (json_out, today, cod, v)
            )
            self.conn.commit()
            
            self.adjust_stock(cod, v, -new_delta)
            
            return {
                "action": "months_passed_insert",
                "cod": cod,
                "v": v,
                "months_passed": months_passed,
                "new_arr_length": len(new_arr)
            }

    def adjust_stock(self, cod:int, v:int, delta:int):
        """
        Increment or decrement the stock by 'delta' (can be negative).
        """
        cur = self.cursor()
        # Fetch current stock
        cur.execute("SELECT stock FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
        row = cur.fetchone()
        if not row:
            print(f"No product_stats found for {cod}.{v}")
            return

        current_stock = int(row["stock"]) if row["stock"] is not None else 0
        new_stock = current_stock + delta

        # Update stock and mark as verified
        cur.execute(
            "UPDATE product_stats SET stock=%s WHERE cod=%s AND v=%s",
            (new_stock, cod, v)
        )
        self.conn.commit()

    def verify_stock(self, cod:int, v:int, new_stock:int, cluster:str = None):
        """
        Called when a human inspects and corrects stock. Optionally set a new stock value.
        This sets verified = True. (Does not change last_update.)
        """
        cur = self.cursor()
        cur.execute("UPDATE product_stats SET stock=%s, verified=TRUE WHERE cod=%s AND v=%s", (new_stock, cod, v))
        if cur.rowcount == 0:
            raise ValueError(f"No product_stats found for {cod}.{v}")
        
        if cluster != None:
            cur.execute("UPDATE products SET cluster = %s WHERE cod=%s AND v=%s", (cluster, cod, v))
            if cur.rowcount == 0:
                raise ValueError(f"No products found for {cod}.{v}")
        
        self.conn.commit()

    def register_internal_sales(self, delta, cod, v):
        
        cur = self.cursor()
        cur.execute("""
            SELECT sold_last_24
            FROM product_stats
            WHERE cod=%s AND v=%s
        """, (cod, v))
        row = cur.fetchone()

        if row:
            sold_json = row['sold_last_24']
            if sold_json:
                sold_arr = sold_json
            else:
                sold_arr = []

            # Ensure list has at least one entry
            if not sold_arr:
                sold_arr = [0]

            # Add delta to the first element (most recent month)
            sold_arr[0] += delta

            # Update database
            cur.execute("""
                UPDATE product_stats
                SET sold_last_24 = %s
                WHERE cod=%s AND v=%s
            """, (Json(sold_arr), cod, v))

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
        COST_COLS = "Costo"
        PRICE_COLS = "Vendita"
        REP_COLS = "Reparto"


    # Skip rows without a numeric Cod.
        df = df[pd.to_numeric(df[COD_COLS], errors="coerce").notna()]

        # Convert Cod. and V. to integers
        df[COD_COLS] = df[COD_COLS].astype(int)
        df[V_COLS] = df[V_COLS].fillna(0).astype(int)

        # Drop duplicates
        df = df.drop_duplicates(subset=[COD_COLS, V_COLS], keep="first")

        # Step 4: Prepare rows for bulk insert
        prod_rows = []
        econ_rows = []
        for _, row in df.iterrows():
            cod = int(row[COD_COLS])
            v = int(row[V_COLS]) if not pd.isna(row[V_COLS]) else 0
            descrizione = str(row[DESC_COLS]).strip() if DESC_COLS in df.columns else ""
            pz_x_collo = int(row[PZ_COLS]) if PZ_COLS in df.columns and not pd.isna(row[PZ_COLS]) else None
            disponibilita = str(row[DISP_COLS]).strip() if DISP_COLS in df.columns else "Si"
            cost = float(row[COST_COLS]) if COST_COLS in df.columns else None
            price = float(row[PRICE_COLS]) if PRICE_COLS in df.columns else None
            category = str(row[REP_COLS]).strip() if REP_COLS in df.columns else ""

            rapp = None
            if RAPP_COLS in df.columns and not pd.isna(row[RAPP_COLS]):
                val = row[RAPP_COLS]
                try:
                    num = float(val)
                    if not num.is_integer():
                        print(f"⚠️ Warning: Float value {val} found in RAPP_COLS for code {cod}. Skipping row.")
                        continue
                    rapp = int(num)
                except ValueError:
                    print(f"⚠️ Warning: Invalid RAPP_COLS value '{val}' for code {cod}. Skipping row.")
                    continue

            prod_rows.append((cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita))
            econ_rows.append((cod, v, price, cost, None, None, None, None, category))


        # Step 5: Insert or update all at once
        cur = self.cursor()
        cur.executemany("""
            INSERT INTO products 
            (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(cod, v) DO UPDATE SET
                descrizione = excluded.descrizione,
                rapp = excluded.rapp,
                pz_x_collo = excluded.pz_x_collo,
                disponibilita = excluded.disponibilita
        """, prod_rows)

        cur.executemany("""
            INSERT INTO economics
            (cod, v, price_std, cost_std, price_s, cost_s, sale_start, sale_end, category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(cod, v) DO UPDATE SET
                price_std = CASE
                    WHEN excluded.sale_start IS NOT NULL
                    AND excluded.sale_end IS NOT NULL
                    AND CURRENT_DATE BETWEEN excluded.sale_start AND excluded.sale_end
                    THEN economics.price_std    -- keep existing
                    ELSE excluded.price_std     -- update
                END,
                cost_std = CASE
                    WHEN excluded.sale_start IS NOT NULL
                    AND excluded.sale_end IS NOT NULL
                    AND CURRENT_DATE BETWEEN excluded.sale_start AND excluded.sale_end
                    THEN economics.cost_std
                    ELSE excluded.cost_std
                END,
                category = excluded.category
        """, econ_rows)

        self.conn.commit()

        # Step 6: Remove stats of deleted products
        # self.purge_orphan_stats()

        print(f"Imported {len(prod_rows)} products into settore '{settore}'.")
    
    def update_promos(self, promo_list):
        """
        promo_list: list of tuples in the form
        (cod, v, price_s, cost_s, sale_start, sale_end)
        """

        if not promo_list:
            return  # nothing to do

        cur = self.cursor()

        # Step 1: get all existing (cod, v) combinations in the DB
        cur.execute("SELECT cod, v FROM economics")
        existing = set((int(row["cod"]), int(row["v"])) for row in cur.fetchall())

        # Filter promo_list using same type normalization
        filtered_list = [
            row for row in promo_list
            if (int(row[0]), int(row[1])) in existing
        ]

        if not filtered_list:
            return  # nothing to update

        # Step 3: perform the upsert on the filtered list
        cur.executemany("""
            INSERT INTO economics (cod, v, price_s, cost_s, sale_start, sale_end, price_std, cost_std, category)
            VALUES (%s, %s, %s, %s, %s, %s, 0, 0, 0)
            ON CONFLICT(cod, v) DO UPDATE SET
                price_s   = excluded.price_s,
                cost_s    = excluded.cost_s,
                sale_start = excluded.sale_start,
                sale_end   = excluded.sale_end
        """, filtered_list)

        self.conn.commit()

    # ---------- Cleaners ----------

    def flag_for_purge(self, cod: int, v: int):
        """
        Mark a product for purging.
        - If stock > 0: Add to blacklist and set purge_flag
        - If stock = 0: Delete immediately
        """
        cur = self.cursor()
        
        # Check if product exists and get stock
        cur.execute("""
            SELECT ps.stock 
            FROM product_stats ps
            WHERE ps.cod = %s AND ps.v = %s
        """, (cod, v))
        
        row = cur.fetchone()
        
        if not row:
            raise ValueError(f"Product {cod}.{v} not found in database")
        
        stock = row['stock'] if row['stock'] is not None else 0
        
        if stock > 0:
            # Has stock - flag for purging
            # First, check if purge_flag column exists, add if not
            try:
                cur.execute("ALTER TABLE products ADD COLUMN purge_flag BOOLEAN DEFAULT FALSE")
                self.conn.commit()
            except:
                pass  # Column already exists
            
            # Set purge flag
            cur.execute("""
                UPDATE products 
                SET purge_flag = 1 
                WHERE cod = %s AND v = %s
            """, (cod, v))
            
            # Set verified to false so it doesn't get ordered
            cur.execute("""
                UPDATE product_stats
                SET verified = FALSE
                WHERE cod = %s AND v = %s
            """, (cod, v))
            
            self.conn.commit()
            
            return {
                'action': 'flagged',
                'cod': cod,
                'v': v,
                'stock': stock,
                'message': f'Product {cod}.{v} flagged for purging (current stock: {stock})'
            }
        else:
            # No stock - delete immediately
            return self.purge_product(cod, v)
        
    def purge_product(self, cod: int, v: int):
        """
        Permanently delete a product from all tables.
        Returns dict with deletion details.
        """
        cur = self.cursor()
        
        deleted_from = []
        
        # Delete from product_stats
        cur.execute("DELETE FROM product_stats WHERE cod = %s AND v = %s", (cod, v))
        if cur.rowcount > 0:
            deleted_from.append('product_stats')
        
        # Delete from economics
        cur.execute("DELETE FROM economics WHERE cod = %s AND v = %s", (cod, v))
        if cur.rowcount > 0:
            deleted_from.append('economics')
        
        # Delete from extra_losses
        cur.execute("DELETE FROM extra_losses WHERE cod = %s AND v = %s", (cod, v))
        if cur.rowcount > 0:
            deleted_from.append('extra_losses')
        
        # Delete from products (main table)
        cur.execute("DELETE FROM products WHERE cod = %s AND v = %s", (cod, v))
        if cur.rowcount > 0:
            deleted_from.append('products')
        
        self.conn.commit()
        
        return {
            'action': 'purged',
            'cod': cod,
            'v': v,
            'deleted_from': deleted_from,
            'message': f'Product {cod}.{v} permanently deleted from: {", ".join(deleted_from)}'
        }
    
    def check_and_purge_flagged(self):
        """
        Check all flagged products and purge those with stock = 0.
        Call this periodically or after stock adjustments.
        Returns list of purged products.
        """
        cur = self.cursor()
        
        # Check if purge_flag column exists
        try:
            cur.execute("""
                SELECT p.cod, p.v, ps.stock
                FROM products p
                JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                WHERE p.purge_flag = 1 AND ps.stock = 0
            """)
        except:
            # purge_flag column doesn't exist yet
            return []
        
        flagged_products = cur.fetchall()
        purged = []
        
        for row in flagged_products:
            cod = row['cod']
            v = row['v']
            
            result = self.purge_product(cod, v)
            purged.append(result)
        
        return purged
    
    def get_purge_pending(self):
        """Get all products flagged for purging (with stock > 0)"""
        cur = self.cursor()
        
        try:
            cur.execute("""
                SELECT p.cod, p.v, p.descrizione, ps.stock
                FROM products p
                JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                WHERE p.purge_flag = 1 AND ps.stock > 0
                ORDER BY ps.stock DESC
            """)
            
            results = []
            for row in cur.fetchall():
                results.append({
                    'cod': row['cod'],
                    'v': row['v'],
                    'name': row['descrizione'],
                    'stock': row['stock']
                })
            
            return results
        except:
            return []