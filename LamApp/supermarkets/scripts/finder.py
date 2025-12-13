from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time


class Finder:
    """
    Discovers available storages for a supermarket
    FIXED: Now accepts credentials as parameters
    """
    
    def __init__(self, username: str, password: str):
        """
        Initialize finder with credentials.
        
        Args:
            username: PAC2000A username
            password: PAC2000A password
        """
        self.username = username
        self.password = password
        
        # Set up Chrome
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument('--log-level=3')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        self.actions = ActionChains(self.driver)
    
    def login(self):
        """Login to PAC2000A"""
        self.driver.get('https://dropzone.pac2000a.it/')
        
        self.wait.until(
            EC.presence_of_element_located((By.ID, "username"))
        )
        
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "password")
        username_field.send_keys(self.username)
        password_field.send_keys(self.password)
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

    def find_storages(self):

        storages = []

        WebDriverWait(self.driver, 10).until(
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
        # Get the value of the 'value' attribute
        # Arrivato alla fine della dropdown list non torna su, bisogna fixare forse, dipende da come vengono processati i file dalla cartella in cui sono salvate le liste

        # Loop through all the items and check for the matching label
        while True:
            input_value = combo_box_element.get_attribute("value")
            if input_value in storages:
                break
            else:
                storages.append(input_value)
            self.actions.send_keys(Keys.ARROW_DOWN)
            self.actions.perform()
            time.sleep(0.5)
        time.sleep(1)

        return storages