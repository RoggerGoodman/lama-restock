from ssl import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time

class Finder:

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
    def login(self, USERNAME, PASSWORD):
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
            EC.presence_of_element_located((By.ID, "carta32"))
        )

        orders_menu1 = self.driver.find_element(By.ID, "carta32")
        orders_menu1.click()

        WebDriverWait(self.driver, 10).until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        self.actions.click()
        self.actions.send_keys(Keys.ESCAPE)
        self.actions.perform()

        time.sleep(1)

    def find_storages(self):

        storages = []

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "addButtonT"))
        )

        order_button = self.driver.find_element(By.ID, "addButtonT")
        order_button.click()

        time.sleep(2)

        dropdown1 = self.driver.find_element(By.ID, "dropdownlistArrowclienteCombo")
        dropdown1.click()

        time.sleep(1)

        self.actions.send_keys(Keys.ARROW_DOWN)
        self.actions.send_keys(Keys.ENTER)
        self.actions.perform()

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "dropdownlistArrowmagazzini"))
        )

        dropdown1 = self.driver.find_element(By.ID, "dropdownlistArrowmagazzini")
        dropdown1.click()

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="dropdownlistContentmagazzini"]/input'))
        )

        # Locate the input field using XPath
        input_field = self.driver.find_element(
            By.XPATH, '//*[@id="dropdownlistContentmagazzini"]/input')

        while True:
            self.actions.send_keys(Keys.ARROW_DOWN)
            self.actions.perform()
            time.sleep(0.5)
            # Get the value of the 'value' attribute
            # Arrivato alla fine della dropdown list non torna su, bisogna fixare forse, dipende da come vengono processati i file dalla cartella in cui sono salvate le liste
            input_value = input_field.get_attribute("value")
            if input_value in storages:
                break
            else:
                storages.append(input_value)

        return storages