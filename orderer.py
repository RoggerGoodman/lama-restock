from ssl import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
from config import TEST_MODE
from credentials import PASSWORD, USERNAME
from logger import logger


class Orderer:

    def __init__(self) -> None:
        # Set up the Selenium WebDriver (Ensure to have the correct browser driver installed)
        # chrome_options = Options()
        # chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        # chrome_options.add_argument("--no-sandbox")  # Required for some environments
        # chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        # chrome_options.add_argument("--disable-gpu")  # Applicable only if you are running on Windows

        self.driver = webdriver.Chrome()
        self.actions = ActionChains(self.driver)
        
    # Load the webpage
    def login(self):
        self.driver.get('https://dropzone.pac2000a.it/')

        # Wait for the page to fully load
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        # Login
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "password")
        username_field.send_keys(USERNAME)
        password_field.send_keys(PASSWORD)
        self.actions.send_keys(Keys.ENTER)
        self.actions.perform()

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "carta31"))
        )

        orders_menu = self.driver.find_element(By.ID, "carta31")
        orders_menu.click()

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "carta139"))
        )

        orders_menu1 = self.driver.find_element(By.ID, "carta139")
        orders_menu1.click()

        WebDriverWait(self.driver, 10).until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "Ordini"))
        )

        time.sleep(1) 

        orders_menu1 = self.driver.find_element(By.ID, "Ordini")
        orders_menu1.click()

        lista_link = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='./lista']"))
        )

        # Click the "Lista" link
        lista_link.click()

        # Wait for the modal to be present
        modal = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "modal-content"))
)

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//button[text()='Chiudi']"))
        )

        time.sleep(1)

        close_button = self.driver.find_element(By.XPATH, "//button[text()='Chiudi']")
        close_button.click()


        time.sleep(1)

    def make_orders(self, storage: str, order_list: list):

        desired_value = storage

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "newRowButtonMenuSopra"))
        )

        order_button = self.driver.find_element(By.ID, "newRowButtonMenuSopra")
        order_button.click()

        time.sleep(2)

        # Step 1: Wait for the modal to appear
        modal_container = WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.ID, "finestraInsertOrdini"))  # Ensure the modal is visible
        )

        # Step 2: Wait for the button inside the modal using XPath
        button_inside_modal = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='IDCodiceClienteBis']/div[1]/div/div[1]/span[2]"))
        )

        # Step 3: Click the button
        button_inside_modal.click()

        # Full XPath to locate the element
        xpath = "/html/body/div[2]/div[2]/div[5]/div/div/div/div[2]/div/form/div[1]/div/smart-combo-box/div[1]/div/div[2]/smart-list-box/div[1]/div[2]/div[2]/smart-list-item"

        # Step 1: Wait for the element to be clickable using full XPath
        element = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )

        # Step 2: Click the element 
      
        element.click()

        time.sleep(1)

        # Step 2: Wait for the button inside the modal using XPath 
        button_inside_modal2 = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='magazziniInsert']/div[1]/div/div[1]/span[2]"))
        )

        # Step 3: Click the button
        button_inside_modal2.click()

        time.sleep(1)

        # Find all smart-list-item elements inside the dropdown
        list_items = self.driver.find_elements(By.XPATH, "//smart-list-item")

        # Loop through all the items and check for the matching label
        for item in list_items:
            label = item.find_element(By.XPATH, ".//span[@class='smart-content-label']").text
            if desired_value in label:
                # Once the item is found, click it
                item.click()
                break

        # Find the 'Conferma' button using its class or smart-id
        confirm_button = self.driver.find_element(By.XPATH, '//*[@id="confermaInsertTestata"]')

        # Click the 'Conferma' button
        confirm_button.click()

        WebDriverWait(self.driver, 10).until(
            lambda driver: len(driver.window_handles) > 2
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "addButtonT"))
        )

        new_order_button = self.driver.find_element(By.ID, "addButtonT")
        new_order_button.click()

        time.sleep(1)

        for element in order_list:
            # Split the element by the dot character
            parts = element.split('.')

            # Assign each part to part1, part2, and part3
            part1, part2, part3 = map(int, parts)  # Convert parts to integers

            # Locate the parent div element by its ID
            parent_div1 = self.driver.find_element(By.ID, "codArt")
            parent_div2 = self.driver.find_element(By.ID, "varArt")

            # Find the input element within the parent div
            cod_art_field = parent_div1.find_element(By.TAG_NAME, "input")
            var_art_field = parent_div2.find_element(By.TAG_NAME, "input")

            # Clear the input field and insert the desired number
            cod_art_field.clear()  # If you need to clear any existing value
            var_art_field.clear()

            cod_art_field.send_keys(part1)
            var_art_field.send_keys(part2)

            time.sleep(0.8)

            parent_div3 = self.driver.find_element(By.ID, "w_Quantita")
            stock_size = parent_div3.find_element(By.TAG_NAME, "input")
            stock_size.clear()
            stock_size.send_keys(part3)

            time.sleep(0.5)

            # Locate the button using its ID
            confirm_button_order = self.driver.find_element(By.ID, "okModificaRiga")

            # Click the button
            confirm_button_order.click()

            time.sleep(1)

        # Switch to the previous tab
        self.driver.switch_to.window(self.driver.window_handles[-2])

    def lists_combiner(self, storage_list, orders_list):
        for storage, order_list in zip(storage_list, orders_list):
            if not TEST_MODE:
                storage = storage.split(' ', 1)[1]
                self.make_orders(storage, order_list)
            else:
                logger.info(f'We made orders')
                break

'''WebDriverWait(self.driver, 10).until(
                EC.text_to_be_present_in_element(
                    (By.ID, "jqxNotificationDefaultContainer-top-right"),
                    "Articolo"
                )
            )'''           