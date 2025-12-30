# LamApp/supermarkets/scripts/orderer.py - WITH SKIP TRACKING
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
import time
import re
import os, uuid, shutil
from .logger import logger


class Orderer:

    def __init__(self, username: str, password: str) -> None:
        """
        Initialize orderer with credentials.
        
        Args:
            username: PAC2000A username
            password: PAC2000A password
        """
        self.username = username
        self.password = password
        
        # Set up the Selenium WebDriver
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/google-chrome"
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument('--log-level=3')

        # Set a writable directory for Chrome to use
        self.user_data_dir = f"/tmp/chrome-{uuid.uuid4()}"
        os.makedirs(self.user_data_dir, exist_ok=True)
        chrome_options.add_argument(f"--user-data-dir={self.user_data_dir}")

        service = Service("/usr/bin/chromedriver")

        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 300)
        self.actions = ActionChains(self.driver)
        
        # Track products skipped during ordering phase
        self.order_skipped_products = []
        
    def login(self):
        """Login to PAC2000A"""
        self.driver.get('https://dropzone.pac2000a.it/')

        # Wait for the page to fully load
        self.wait.until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        # Login
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "password")
        username_field.send_keys(self.username)
        password_field.send_keys(self.password)
        self.actions.send_keys(Keys.ENTER)
        self.actions.perform()

        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta31"))
        )

        orders_menu = self.driver.find_element(By.ID, "carta31")
        orders_menu.click()

        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta139"))
        )

        orders_menu1 = self.driver.find_element(By.ID, "carta139")
        orders_menu1.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])

        self.wait.until(
            EC.presence_of_element_located((By.ID, "Ordini"))
        )

        time.sleep(1) 

        orders_menu1 = self.driver.find_element(By.ID, "Ordini")
        orders_menu1.click()

        lista_link = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='./lista']"))
        )

        lista_link.click()

        modal = self.wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "modal-content"))
        )

        self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//button[text()='Chiudi']"))
        )

        time.sleep(1)

        close_button = self.driver.find_element(By.XPATH, "//button[text()='Chiudi']")
        close_button.click()

        time.sleep(1)

    def make_orders(self, storage: str, order_list: tuple):
        """
        Make orders with skip tracking.
        Returns: (successful_orders, order_skipped_products)
        
        NOTE: These are products skipped DURING ORDER EXECUTION (e.g., system disabled)
        This is different from decision_maker's skipped_products (skipped during calculation)
        """

        desired_value = re.sub(r'^\d+\s+', '', storage)

        self.wait.until(
            EC.presence_of_element_located((By.ID, "newRowButtonMenuSopra"))
        )

        order_button = self.driver.find_element(By.ID, "newRowButtonMenuSopra")
        order_button.click()

        time.sleep(2)

        # Step 1: Wait for the modal to appear
        modal_container = self.wait.until(
            EC.visibility_of_element_located((By.ID, "finestraInsertOrdini"))
        )

        # Step 2: Wait for the button inside the modal using XPath
        button_inside_modal = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='IDCodiceClienteBis']/div[1]/div/div[1]/span[2]"))
        )

        button_inside_modal.click()

        xpath = "/html/body/div[2]/div[2]/div[5]/div/div/div/div[2]/div/form/div[1]/div/smart-combo-box/div[1]/div/div[2]/smart-list-box/div[1]/div[2]/div[2]/smart-list-item"

        element = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )

        element.click()

        time.sleep(1)

        button_inside_modal2 = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='magazziniInsert']/div[1]/div/div[1]/span[2]"))
        )

        button_inside_modal2.click()

        time.sleep(1)

        combo_box_element = self.driver.find_element(By.ID, "magazziniInsert")
        self.actions.send_keys(Keys.ARROW_DOWN)
        self.actions.perform()
        time.sleep(0.5)
        self.actions.send_keys(Keys.ARROW_DOWN)
        self.actions.perform()
        time.sleep(0.5)
        self.actions.send_keys(Keys.ARROW_UP)
        self.actions.perform()
        time.sleep(0.5)
        self.actions.send_keys(Keys.ARROW_UP)
        self.actions.perform()
        time.sleep(0.5)
        
        while True:
            input_value = combo_box_element.get_attribute("value")
            if input_value == desired_value:
                self.actions.send_keys(Keys.ESCAPE)
                self.actions.perform()
                break
            self.actions.send_keys(Keys.ARROW_DOWN)
            self.actions.perform()
            time.sleep(0.5)
            
        time.sleep(1)

        confirm_button = self.driver.find_element(By.XPATH, '//*[@id="confermaInsertTestata"]')
        confirm_button.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 2
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])

        new_order_button = self.driver.find_element(By.ID, "addButtonT")
        new_order_button.click()

        time.sleep(1)

        parent_div1 = self.driver.find_element(By.ID, "codArt")
        parent_div2 = self.driver.find_element(By.ID, "varArt")
        parent_div3 = self.driver.find_element(By.ID, "w_Quantita")
        
        cod_art_field = parent_div1.find_element(By.TAG_NAME, "input")
        var_art_field = parent_div2.find_element(By.TAG_NAME, "input")
        stock_size = parent_div3.find_element(By.TAG_NAME, "input")
        search_button = self.driver.find_element(By.XPATH, '/html/body/div[66]/div[2]/form/div[4]/div[2]/div[3]/div')
        
        successful_orders = []
        
        for cod_part, var_part, qty_part in order_list:
            cod_art_field.clear()
            var_art_field.clear()

            cod_art_field.send_keys(cod_part)
            var_art_field.send_keys(var_part)

            time.sleep(0.4)

            search_button.click()
            
            time.sleep(0.4)
            
            # Check if product doesn't accept orders (disabled by system)
            is_off = self.driver.find_elements(
                By.XPATH,
                "//div[contains(@class, 'jqx-switchbutton-label-off') and contains(@style, 'visibility: visible')]"
            )
            if is_off:
                logger.info(f"Article {cod_part}.{var_part} doesn't accept orders (disabled in ordering system)")
                
                # Track as skipped during ORDER EXECUTION
                self.order_skipped_products.append({
                    'cod': cod_part,
                    'var': var_part,
                    'qty': qty_part,
                    'reason': 'Product disabled in ordering system (cannot place order)'
                })
                continue

            stock_size.clear()
            stock_size.send_keys(qty_part)

            time.sleep(0.5)

            confirm_button_order = self.driver.find_element(By.ID, "okModificaRiga")
            confirm_button_order.click()
            
            # Track successful order
            successful_orders.append((cod_part, var_part, qty_part))

        # Switch back
        #self.driver.switch_to.window(self.driver.window_handles[-2])
        
        logger.info(f"Order execution complete: {len(successful_orders)} successful, {len(self.order_skipped_products)} skipped during ordering")
        self.driver.quit()
        shutil.rmtree(self.user_data_dir, ignore_errors=True)
        return successful_orders, self.order_skipped_products