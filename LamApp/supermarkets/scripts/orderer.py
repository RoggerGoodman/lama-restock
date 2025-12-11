# LamApp/supermarkets/scripts/orderer.py - WITH SKIP TRACKING
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import re
from .constants import PASSWORD, USERNAME
from .logger import logger


class Orderer:

    def __init__(self) -> None:
        #Set up the Selenium WebDriver (Ensure to have the correct browser driver installed)
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        chrome_options.add_argument("--no-sandbox")  # Required for some environments
        chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        chrome_options.add_argument("--disable-gpu")  # Applicable only if you are running on Windows
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument('--log-level=3')  # Suppress console logs

        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 300)
        self.actions = ActionChains(self.driver)
        
        # NEW: Track skipped products during order execution
        self.skipped_products = []
        
    # Load the webpage
    def login(self):
        self.driver.get('https://dropzone.pac2000a.it/')

        # Wait for the page to fully load
        self.wait.until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        # Login
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "password")
        username_field.send_keys(USERNAME)
        password_field.send_keys(PASSWORD)
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

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        self.wait.until(
            EC.presence_of_element_located((By.ID, "Ordini"))
        )

        time.sleep(1) 

        orders_menu1 = self.driver.find_element(By.ID, "Ordini")
        orders_menu1.click()

        lista_link = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='./lista']"))
        )

        # Click the "Lista" link
        lista_link.click()

        # Wait for the modal to be present
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
        Returns: (successful_orders, skipped_products)
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
            EC.visibility_of_element_located((By.ID, "finestraInsertOrdini"))  # Ensure the modal is visible
        )

        # Step 2: Wait for the button inside the modal using XPath
        button_inside_modal = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='IDCodiceClienteBis']/div[1]/div/div[1]/span[2]"))
        )

        # Step 3: Click the button
        button_inside_modal.click()

        # Full XPath to locate the element
        xpath = "/html/body/div[2]/div[2]/div[5]/div/div/div/div[2]/div/form/div[1]/div/smart-combo-box/div[1]/div/div[2]/smart-list-box/div[1]/div[2]/div[2]/smart-list-item"

        # Step 1: Wait for the element to be clickable using full XPath
        element = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )

        # Step 2: Click the element 
      
        element.click()

        time.sleep(1)

        # Step 2: Wait for the button inside the modal using XPath 
        button_inside_modal2 = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='magazziniInsert']/div[1]/div/div[1]/span[2]"))
        )

        # Step 3: Click the button
        button_inside_modal2.click()

        time.sleep(1)

        # Find all smart-list-item elements inside the dropdown
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
        # Loop through all the items and check for the matching label
        while True:
            input_value = combo_box_element.get_attribute("value")
            if input_value == desired_value:
                self.actions.send_keys(Keys.ESCAPE)
                self.actions.perform()
                break
            self.actions.send_keys(Keys.ARROW_DOWN)
            self.actions.perform()
            time.sleep(0.5)
            # Arrivato alla fine della dropdown list non torna su, bisogna fixare forse, dipende da come vengono processati i file dalla cartella in cui sono salvate le liste
            
        time.sleep(1)

        # Find the 'Conferma' button using its class or smart-id
        confirm_button = self.driver.find_element(By.XPATH, '//*[@id="confermaInsertTestata"]')

        # Click the 'Conferma' button
        confirm_button.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 2
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        new_order_button = self.driver.find_element(By.ID, "addButtonT")
        new_order_button.click()

        time.sleep(1)

        # Locate the parent div element by its ID
        parent_div1 = self.driver.find_element(By.ID, "codArt")
        parent_div2 = self.driver.find_element(By.ID, "varArt")
        parent_div3 = self.driver.find_element(By.ID, "w_Quantita")
        # Find the input element within the parent div
        cod_art_field = parent_div1.find_element(By.TAG_NAME, "input")
        var_art_field = parent_div2.find_element(By.TAG_NAME, "input")
        stock_size = parent_div3.find_element(By.TAG_NAME, "input")
        search_button = self.driver.find_element(By.XPATH, '/html/body/div[66]/div[2]/form/div[4]/div[2]/div[3]/div')
        successful_orders = []
        
        for cod_part, var_part, qty_part in order_list:
            # Clear the input field and insert the desired number
            cod_art_field.clear()
            var_art_field.clear()

            cod_art_field.send_keys(cod_part)
            var_art_field.send_keys(var_part)

            time.sleep(0.4)

            search_button.click()
            
            time.sleep(0.4)
            # Check if product doesn't accept orders
            is_off = self.driver.find_elements(
                By.XPATH,
                "//div[contains(@class, 'jqx-switchbutton-label-off') and contains(@style, 'visibility: visible')]"
            )
            if is_off:
                logger.info(f"Article {cod_part}.{var_part} doesn't accept orders. Skipping.")
                
                # NEW: Track skipped product with reason
                self.skipped_products.append({
                    'cod': cod_part,
                    'var': var_part,
                    'qty': qty_part,
                    'reason': 'Product does not accept orders (disabled in system)'
                })
                continue

            
            stock_size.clear()
            stock_size.send_keys(qty_part)

            time.sleep(0.5)

            # Locate the button using its ID
            confirm_button_order = self.driver.find_element(By.ID, "okModificaRiga")

            # Click the button
            confirm_button_order.click()
            
            # Track successful order
            successful_orders.append((cod_part, var_part, qty_part))


        # Switch to the previous tab
        self.driver.switch_to.window(self.driver.window_handles[-2])
        
        logger.info(f"Order execution complete: {len(successful_orders)} successful, {len(self.skipped_products)} skipped")
        
        return successful_orders, self.skipped_products