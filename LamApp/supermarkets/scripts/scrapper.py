from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, UnexpectedAlertPresentException
from .DatabaseManager import DatabaseManager
from .helpers import Helper
import os, uuid, shutil


class Scrapper:

    def __init__(self, username: str, password: str, helper: Helper, db: DatabaseManager) -> None:
        """
        Initialize scrapper with credentials.
        
        Args:
            username: PAC2000A username
            password: PAC2000A password
            helper: Helper instance
            db: DatabaseManager instance
        """
        self.username = username
        self.password = password
        self.helper = helper
        self.db = db

        self.user_data_dir = f"/tmp/chrome-{uuid.uuid4()}"
        os.makedirs(self.user_data_dir, exist_ok=True)

        os.environ["HOME"] = self.user_data_dir
        os.environ["XDG_RUNTIME_DIR"] = self.user_data_dir

        # Set up the Selenium WebDriver
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/google-chrome"
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument('--log-level=3')

        # Set a writable directory for Chrome to use
        chrome_options.add_argument(f"--user-data-dir={self.user_data_dir}")

        service = Service("/usr/bin/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        self.actions = ActionChains(self.driver)
        self.offers_path = r"C:\Users\rugge\Documents\GitHub\lama-restock\Offers"
        self.current_day = datetime.now().day

    def login(self):
        """Login to PAC2000A"""
        self.driver.get('https://www.pac2000a.it/PacApplicationUserPanel/faces/home.jsf')
    
        # Wait for the username field to be present
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        # Log in
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "Password")
        login_button = self.driver.find_element(By.CLASS_NAME, "btn-primary")

        username_field.send_keys(self.username)
        password_field.send_keys(self.password)
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

    def init_product_stats_for_settore(self, settore, full:bool = True):
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
        cur = self.db.cursor()
        if full == False:
            cur.execute("""
                SELECT ps.cod, ps.v
                FROM product_stats ps
                LEFT JOIN products p
                ON ps.cod = p.cod
                AND ps.v   = p.v
                AND p.settore = %s
                WHERE ps.verified = TRUE
            """, (settore,))
        else :
            cur.execute("SELECT cod, v FROM products WHERE settore = %s", (settore,))

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
                cur.execute("SELECT 1 FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
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
        self.driver.quit()
        shutil.rmtree(self.user_data_dir, ignore_errors=True)