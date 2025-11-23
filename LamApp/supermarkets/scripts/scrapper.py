from datetime import datetime
from .constants import PASSWORD, USERNAME
from .DatabaseManager import DatabaseManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, UnexpectedAlertPresentException
from .helpers import Helper
import pdfplumber
import re


class Scrapper:

    def __init__(self, helper: Helper, db: DatabaseManager) -> None:
        # Set up the Selenium WebDriver (Ensure to have the correct browser driver installed)
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        # chrome_options.add_argument("--no-sandbox")  # Required for some environments
        # chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        # chrome_options.add_argument("--disable-gpu")  # Applicable only if you are running on Windows

        self.helper = helper
        self.driver = webdriver.Chrome(options=chrome_options)
        self.actions = ActionChains(self.driver)
        self.db = db
        self.offers_path=r"C:\Users\rugge\Documents\GitHub\lama-restock\Offers"
        self.current_day = datetime.now().day

    def login(self):

        self.driver.get('https://www.pac2000a.it/PacApplicationUserPanel/faces/home.jsf')
    

        # Wait for the username field to be present, indicating that the page has loaded
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        # Log in by entering the username and password, then clicking the login button
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "Password")
        login_button = self.driver.find_element(By.CLASS_NAME, "btn-primary")

        username_field.send_keys(USERNAME)
        password_field.send_keys(PASSWORD)
        login_button.click()

    def navigate(self):
        self.login()
        # Wait for the page to load after login
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//a[contains(text(), "eMarket")]'))
        )

        # Locate the "eMarket" link by its text
        emarket_link = self.driver.find_element(By.XPATH, '//a[contains(text(), "eMarket")]')
        emarket_link.click()

        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//a[@title="Statistiche Articolo"]'))
        )

        stat_link = self.driver.find_element(By.XPATH, '//a[@title="Statistiche Articolo"]')
        stat_link.click()

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "ifStatistiche Articolo"))
        )

    def init_product_stats_for_settore(self, settore):
        """
        For every product in `products` with given settore:
        - navigate (assumes driver is already on the stats page where cod_art and var_art exist)
        - enter cod and var, hit enter, wait for window.str_qta_vend / str_qta_acq
        - clean/prepare arrays via helper, then call db.init_product_stats(cod, v, sold=..., bought=...)
            - if product_stats exists and force==False -> skip
            - if product_stats exists and force==True -> update the existing row
        Returns: dict with counts
        """
        
        timeout=10
        cur = self.db.conn.cursor()
        if settore == "#RIANO GENERI VARI":
            cur.execute("SELECT ps.cod, ps.v FROM 'product_stats' ps LEFT JOIN 'products' p ON ps.cod = p.cod AND ps.v = p.v WHERE p.settore = ? AND ps.verified = 1", (settore,))
        else :
            cur.execute("SELECT cod, v FROM products WHERE settore = ?", (settore,))

        products = cur.fetchall()

        report = {
            "total": len(products),
            "processed": 0,
            "initialized": 0,
            "updated": 0,
            "skipped_exists": 0,
            "empty": 0,
            "errors": 0
        }

        for row in products:
            cod = row["cod"]
            v = row["v"]
            report["processed"] += 1

            iframe = self.driver.find_element(By.ID, "ifStatistiche Articolo")
            self.driver.switch_to.frame(iframe)

            try:
                # --- fill fields and submit (your snippet adapted) ---
                cod_art_field = self.driver.find_element(By.NAME, "cod_art")
                var_art_field = self.driver.find_element(By.NAME, "var_art")

                # clear and send
                cod_art_field.clear()
                var_art_field.clear()
                cod_art_field.send_keys(str(cod))
                var_art_field.send_keys(str(v))

                # press enter (use actions for reliability)
                self.actions.send_keys(Keys.ENTER).perform()

                # Wait until the JS arrays are available
                WebDriverWait(self.driver, timeout).until(
                    lambda d: d.execute_script("return typeof window.str_qta_vend !== 'undefined' && window.str_qta_vend !== null")
                )
                sold_quantities = self.driver.execute_script("return window.str_qta_vend;")

                WebDriverWait(self.driver, timeout).until(
                    lambda d: d.execute_script("return typeof window.str_qta_acq !== 'undefined' && window.str_qta_acq !== null")
                )
                bought_quantities = self.driver.execute_script("return window.str_qta_acq;")

                self.driver.back()
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "ifStatistiche Articolo"))
                )

                # --- split current year and last year as in your snippet ---
                sold_q_current = sold_quantities[::2]
                sold_q_last = sold_quantities[1::2]
                bought_q_current = bought_quantities[::2]
                bought_q_last = bought_quantities[1::2]

                # --- clean and convert using helper ---
                cleaned_current_year_sold = self.helper.clean_convert_reverse(sold_q_current)
                cleaned_last_year_sold = self.helper.clean_convert_reverse(sold_q_last)
                cleaned_current_year_bought = self.helper.clean_convert_reverse(bought_q_current)
                cleaned_last_year_bought = self.helper.clean_convert_reverse(bought_q_last)

                # If any of the cleaned lists is falsy -> skip
                if not cleaned_current_year_sold or not cleaned_last_year_sold or not cleaned_current_year_bought or not cleaned_last_year_bought:
                    # skip this product (bad/invalid format)
                    report["errors"] += 1
                    continue

                # combine arrays as you described (current year first, then last year)
                final_array_sold = cleaned_current_year_sold + cleaned_last_year_sold
                final_array_bought = cleaned_current_year_bought + cleaned_last_year_bought

                # call helper.prepare_array (your helper returns (bought, sold))
                final_array_bought, final_array_sold = self.helper.prepare_array(final_array_bought, final_array_sold)

                if len(final_array_bought) == 0 and len(final_array_sold) == 0 :
                    report["empty"] += 1

                # --- write to DB ---
                # If row exists and force=True -> update; else init (INSERT)
                cur.execute("SELECT 1 FROM product_stats WHERE cod=? AND v=?", (cod, v))
                exists = cur.fetchone() is not None

                if not exists:
                    # init_product_stats expects Python lists
                    self.db.init_product_stats(cod, v, sold=final_array_sold, bought=final_array_bought, stock=0, verified=False)
                    report["initialized"] += 1
                else:
                     # pass first two elements
                    self.db.update_product_stats(cod, v, sold_update=final_array_sold[:2], bought_update=final_array_bought[:2])
                    report["updated"] += 1

            except UnexpectedAlertPresentException:
                self.actions.send_keys(Keys.ENTER)
                report["errors"] += 1
                continue  # Skip to the next iteration of the loop

            except TimeoutException:
                # timed out waiting for JS vars; skip this product
                report["errors"] += 1
                continue

            except Exception as e:
                # generic exception - log and continue
                print(f"Error initializing stats for {cod}.{v}: {e}")
                report["errors"] += 1
                continue

        print(report)

    def init_products_and_stats_from_list(self, product_list:list[tuple], settore:str):
        """
        Same as init_product_stats_for_settore(), but takes a Python list of (cod, v) tuples
        instead of querying the database.

        Args:
            product_list (list[tuple]): List of (cod, v) pairs to process.
            settore (str) The sector of the products in the list

        Returns:
            dict: Summary report.
        """
        rapp = 1
        disponibilita = "No"

        timeout = 10
        report = {
            "settore": settore,
            "total": len(product_list),
            "processed": 0,
            "initialized": 0,
            "updated": 0,
            "skipped_exists": 0,
            "empty": 0,
            "errors": 0
        }

        for cod, v in product_list:
            report["processed"] += 1

            iframe = self.driver.find_element(By.ID, "ifStatistiche Articolo")
            self.driver.switch_to.frame(iframe)

            try:
                # --- fill fields and submit (your snippet adapted) ---
                cod_art_field = self.driver.find_element(By.NAME, "cod_art")
                var_art_field = self.driver.find_element(By.NAME, "var_art")

                # clear and send
                cod_art_field.clear()
                var_art_field.clear()
                cod_art_field.send_keys(str(cod))
                var_art_field.send_keys(str(v))

                # press enter (use actions for reliability)
                self.actions.send_keys(Keys.ENTER).perform()

                # Wait until the JS arrays are available
                WebDriverWait(self.driver, timeout).until(
                    lambda d: d.execute_script("return typeof window.str_qta_vend !== 'undefined' && window.str_qta_vend !== null")
                )
                sold_quantities = self.driver.execute_script("return window.str_qta_vend;")

                WebDriverWait(self.driver, timeout).until(
                    lambda d: d.execute_script("return typeof window.str_qta_acq !== 'undefined' && window.str_qta_acq !== null")
                )
                bought_quantities = self.driver.execute_script("return window.str_qta_acq;")

                # locate the <td> with text 'Articolo', then go to the next <td> in that row
                articolo_td = self.driver.find_element(By.XPATH, "//td[normalize-space(text())='Articolo']")
                descrizione_td = articolo_td.find_element(By.XPATH, "./following-sibling::td")

                # get the full text and clean it up
                full_text = descrizione_td.text.strip()

                # optional: remove the leading code (everything before the first '-')
                descrizione = full_text.split("-", 1)[1].strip() if "-" in full_text else full_text

                self.driver.back()
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "ifStatistiche Articolo"))
                )

                # --- split current year and last year as in your snippet ---
                sold_q_current = sold_quantities[::2]
                sold_q_last = sold_quantities[1::2]
                bought_q_current = bought_quantities[::2]
                bought_q_last = bought_quantities[1::2]

                # --- clean and convert using helper ---
                cleaned_current_year_sold = self.helper.clean_convert_reverse(sold_q_current)
                cleaned_last_year_sold = self.helper.clean_convert_reverse(sold_q_last)
                cleaned_current_year_bought = self.helper.clean_convert_reverse(bought_q_current)
                cleaned_last_year_bought = self.helper.clean_convert_reverse(bought_q_last)

                # If any of the cleaned lists is falsy -> skip
                if not cleaned_current_year_sold or not cleaned_last_year_sold or not cleaned_current_year_bought or not cleaned_last_year_bought:
                    # skip this product (bad/invalid format)
                    report["errors"] += 1
                    continue

                # combine arrays as you described (current year first, then last year)
                final_array_sold = cleaned_current_year_sold + cleaned_last_year_sold
                final_array_bought = cleaned_current_year_bought + cleaned_last_year_bought

                # call helper.prepare_array (your helper returns (bought, sold))
                final_array_bought, final_array_sold = self.helper.prepare_array(final_array_bought, final_array_sold)

                if len(final_array_bought) == 0 and len(final_array_sold) == 0 :
                    report["empty"] += 1

                pz_x_collo = self.determine_pz_x_collo(final_array_bought)

                cur = self.db.conn.cursor()
                cur.execute("""
                    INSERT INTO products 
                    (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (cod, v, descrizione, rapp, pz_x_collo, settore, disponibilita))

                self.db.conn.commit()

                # --- write to DB ---
                # If row exists and force=True -> update; else init (INSERT)
                cur.execute("SELECT 1 FROM product_stats WHERE cod=? AND v=?", (cod, v))
                exists = cur.fetchone() is not None

                if not exists:
                    # init_product_stats expects Python lists
                    self.db.init_product_stats(cod, v, sold=final_array_sold, bought=final_array_bought, stock=0, verified=False)
                    report["initialized"] += 1

            except UnexpectedAlertPresentException:
                self.actions.send_keys(Keys.ENTER)
                report["errors"] += 1
                continue  # Skip to the next iteration of the loop

            except TimeoutException:
                # timed out waiting for JS vars; skip this product
                report["errors"] += 1
                continue

            except Exception as e:
                # generic exception - log and continue
                print(f"Error initializing stats for {cod}.{v}: {e}")
                report["errors"] += 1
                continue

        print(report)

    def determine_pz_x_collo(self, final_array_bought:list):
        """
        Determine 'pz_x_collo' based on buying pattern.

        Rules:
        - Default = 12
        - Remove all zeros.
        - Find the smallest non-zero value.
        - If it divides all others perfectly → use it.
        - Else, try dividing it by 2 once:
            - If not an integer → default
            - If it divides all others perfectly → use it
            - Else → default
        """

        DEFAULT = 12
        values = [x for x in final_array_bought if x != 0]
        if not values:
            return DEFAULT

        smallest = min(values)

        # Step 1: try with smallest
        if all(v % smallest == 0 for v in values):
            return smallest

        # Step 2: try with half
        half = smallest / 2
        if half % 1 != 0:
            return DEFAULT  # not an integer

        if all(v % half == 0 for v in values):
            return int(half)
        else:
            return DEFAULT
        
    def parse_promo_pdf(self, file_path):
        data = []
        sale_start = None
        sale_end = None

        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()

                # --- Extract sale_start / sale_end ---
                m = re.search(r"Pubblico Dal (\d{2}/\d{2}/\d{4}) al (\d{2}/\d{2}/\d{4})", text)
                if m:
                    sale_start = datetime.strptime(m.group(1), "%d/%m/%Y").date().isoformat()
                    sale_end = datetime.strptime(m.group(2), "%d/%m/%Y").date().isoformat()

                # --- Extract table rows ---
                table = page.extract_table()
                if not table:
                    continue

                for row in table:
                    # Skip headers or empty rows
                    if not row or row[1] == "Codice Art." or row[1] == None:
                        continue

                    codice = row[1]     # e.g. "1729.01"
                    cess = row[5]       # cost_s
                    pubb = row[6]       # price_s

                    # Convert codice
                    if codice:
                        parts = codice.split(".")
                        cod = int(parts[0])
                        v = int(parts[1])

                    # Convert prices
                    try:
                        cost_s = float(cess.replace(",", ".")) if cess else None
                    except:
                        cost_s = None

                    try:
                        price_s = float(pubb.replace(",", ".")) if pubb else None
                    except:
                        price_s = None

                    data.append((cod, v, price_s, cost_s, sale_start, sale_end))

        return data
