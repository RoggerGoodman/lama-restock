# LamApp/supermarkets/scripts/scrapper.py - ENHANCED WITH NEWLY ADDED TRACKING

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
import logging
import os, uuid, shutil

# Use Django's logging system
logger = logging.getLogger(__name__)


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
        os.chmod(self.user_data_dir, 0o700)

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

        chrome_options.add_argument(f"--user-data-dir={self.user_data_dir}")

        service = Service("/usr/local/bin/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        self.actions = ActionChains(self.driver)
        self.offers_path = r"C:\Users\rugge\Documents\GitHub\lama-restock\Offers"
        self.current_day = datetime.now().day

    def login(self):
        """Login to PAC2000A"""
        self.driver.get('https://www.pac2000a.it/PacApplicationUserPanel/faces/home.jsf')
    
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "Password")
        login_button = self.driver.find_element(By.CLASS_NAME, "btn-primary")

        username_field.send_keys(self.username)
        password_field.send_keys(self.password)
        login_button.click()

    def navigate(self):
        self.login()
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//a[contains(text(), "eMarket")]'))
        )

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
        - navigate and scrape sales data
        - track "newly added" products (ordered but not yet sold/verified)
        
        Returns: dict with counts + list of newly_added products
        """
        
        timeout=10
        cur = self.db.cursor()
        if full == False:
            cur.execute("""
                SELECT ps.cod, ps.v, ps.verified
                FROM product_stats ps
                LEFT JOIN products p
                ON ps.cod = p.cod
                AND ps.v   = p.v
                AND p.settore = %s
                WHERE ps.verified = TRUE
            """, (settore,))
        else:
            cur.execute("""
                SELECT p.cod, p.v, ps.verified, p.pz_x_collo
                FROM products p
                LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                WHERE p.settore = %s
            """, (settore,))

        products = cur.fetchall()

        product_tuples = [
            (
                row["cod"],
                row["v"],
                row.get("verified", False),
                row.get("pz_x_collo", 12),
            )
            for row in products
        ]
        self.process_products(product_tuples)

    def process_products(self, product_tuples):
        """
        Process a list of products.

        products: iterable of tuples
            (cod, v, verified, pz_x_collo)

        Returns:
            report dict
        """
        timeout=10
        cur = self.db.cursor()
        report = {
            "total": len(product_tuples),
            "processed": 0,
            "initialized": 0,
            "updated": 0,
            "skipped_exists": 0,
            "empty": 0,
            "errors": 0,
            "newly_added": [],
        }

        for cod, v, is_verified, package_size in product_tuples:

            report["processed"] += 1

            try:
                iframe = self.driver.find_element(By.ID, "ifStatistiche Articolo")
                self.driver.switch_to.frame(iframe)

                cod_art_field = self.driver.find_element(By.NAME, "cod_art")
                var_art_field = self.driver.find_element(By.NAME, "var_art")

                cod_art_field.clear()
                var_art_field.clear()
                cod_art_field.send_keys(str(cod))
                var_art_field.send_keys(str(v))

                self.actions.send_keys(Keys.ENTER).perform()

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

                sold_q_current = sold_quantities[::2]
                sold_q_last = sold_quantities[1::2]
                bought_q_current = bought_quantities[::2]
                bought_q_last = bought_quantities[1::2]

                cleaned_current_year_sold = self.helper.clean_convert_reverse(sold_q_current)
                cleaned_last_year_sold = self.helper.clean_convert_reverse(sold_q_last)
                cleaned_current_year_bought = self.helper.clean_convert_reverse(bought_q_current)
                cleaned_last_year_bought = self.helper.clean_convert_reverse(bought_q_last)
                empty_list = [0,0,0,0,0,0,0,0,0,0,0,0]
                # ✅ NEW: Detect "newly added" products
                if (
                        cleaned_current_year_bought != empty_list and
                        cleaned_current_year_sold  == empty_list and
                        cleaned_last_year_bought   == empty_list
                    ):

                    print(f"Newly added product detected: {cod}.{v}")

                    # Only add to list if NOT verified
                    if is_verified == False:
                        report["newly_added"].append({
                            'cod': cod,
                            'var': v,
                            'package_size': package_size,
                            'reason': 'Product ordered but not yet sold (needs verification)'
                        })

                if cleaned_current_year_bought is None or cleaned_last_year_bought is None or cleaned_current_year_sold is None or cleaned_last_year_sold is None:
                    report["errors"] += 1
                    continue

                final_array_sold = (cleaned_current_year_sold) + (cleaned_last_year_sold)
                final_array_bought = (cleaned_current_year_bought) + (cleaned_last_year_bought)

                final_array_bought, final_array_sold = self.helper.prepare_array(
                    final_array_bought,
                    final_array_sold
                )

                if len(final_array_bought) == 0 and len(final_array_sold) == 0:
                    report["empty"] += 1

                cur.execute("SELECT 1 FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
                exists = cur.fetchone() is not None

                if not exists:
                    self.db.init_product_stats(cod, v, sold=final_array_sold, bought=final_array_bought, stock=0, verified=False)
                    report["initialized"] += 1
                else:
                    self.db.update_product_stats(cod, v, sold_update=final_array_sold[:2], bought_update=final_array_bought[:2])
                    report["updated"] += 1

            except UnexpectedAlertPresentException:
                self.actions.send_keys(Keys.ENTER)
                report["errors"] += 1
                try:
                    self.driver.switch_to.default_content()
                    self.driver.back()
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.ID, "ifStatistiche Articolo"))
                    )
                except Exception:
                    pass
                continue

            except TimeoutException:
                report["errors"] += 1
                try:
                    self.driver.switch_to.default_content()
                    self.driver.back()
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.ID, "ifStatistiche Articolo"))
                    )
                except Exception:
                    pass
                continue

            except Exception as e:
                print(f"Error initializing stats for {cod}.{v}: {e}")
                report["errors"] += 1
                try:
                    self.driver.switch_to.default_content()
                    self.driver.back()
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.ID, "ifStatistiche Articolo"))
                    )
                except Exception:
                    pass
                continue

        logger.info(report)
        print(f"Found {len(report['newly_added'])} newly added products needing verification")
        
        self.driver.quit()
        shutil.rmtree(self.user_data_dir, ignore_errors=True)
        
        return report