import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
from pywinauto import Application, Desktop
from selenium.webdriver.chrome.options import Options
from credentials import PASSWORD, USERNAME
storages = ["01 RIANO GENERI VARI", "23 S.PALOMBA SURGELATI", "02 POMEZIA DEPERIBILI"]
desired_value = "02 POMEZIA DEPERIBILI"
save_path = r"C:\Users\Ruggero\Documents\GitHub\lama-restock\Database"
full_file_path = rf"{save_path}\{desired_value}"
filters = True

class Updater :
    def __init__(self) -> None:
        # Set up the Selenium WebDriver (Ensure to have the correct browser driver installed)
        chrome_options = Options()
        chrome_options.add_argument("--kiosk-printing")  # Automatically handles print dialogs
        # chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        # chrome_options.add_argument("--no-sandbox")  # Required for some environments
        # chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        # chrome_options.add_argument("--disable-gpu")  # Applicable only if you are running on Windows

        # Define the set of numbers to check
        self.target_filters1 = {"100", "300", "325", "350", "370", "600", "620", "820"}
        self.target_filters3 = {"300", "350"}
        # Initialize WebDriver with the options
        self.driver = webdriver.Chrome(options=chrome_options)
        self.actions = ActionChains(self.driver)

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
            EC.presence_of_element_located((By.ID, "carta80"))
        )

        list_menu = self.driver.find_element(By.ID, "carta80")
        list_menu.click()

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "carta84"))
        )

        list_menu1 = self.driver.find_element(By.ID, "carta84")
        list_menu1.click()

        WebDriverWait(self.driver, 10).until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        time.sleep(2)

    def selector(self):
        client_field = self.driver.find_element(By.ID, "dropdownlistContentF_clienti")
        client_field.click()
        self.actions.send_keys("MD")
        self.actions.perform()
        time.sleep(1)
        client_field.click()
        self.actions.send_keys(Keys.ARROW_DOWN)
        self.actions.perform()
        self.actions.send_keys(Keys.ENTER)
        self.actions.perform()
        self.actions.send_keys(Keys.TAB)
        self.actions.perform()

        time.sleep(1)
        input_field = self.driver.find_element(By.XPATH, '//*[@id="dropdownlistContentF_magazzini"]/input')

        while True:
            self.actions.send_keys(Keys.ARROW_DOWN)
            self.actions.perform()
            time.sleep(0.5)
            # Get the value of the 'value' attribute
            # Arrivato alla fine della dropdown list non torna su, bisogna fixare forse, dipende da come vengono processati i file dalla cartella in cui sono salvate le liste
            input_value = input_field.get_attribute("value")
            if input_value == desired_value:
                self.actions.send_keys(Keys.ENTER)
                self.actions.perform()
                break
        time.sleep(1)

        if filters:
            match desired_value:
                case "02 POMEZIA DEPERIBILI":
                    self.target_numbers = self.target_filters3
                case "01 RIANO GENERI VARI":
                    self.target_numbers = self.target_filters1

            filtersDropdown = self.driver.find_element(By.ID, "dropdownlistArrowF_reparto")
            filtersDropdown.click()

            # Locate the input element
            input_element = self.driver.find_element(By.XPATH, "//div[@id='dropdownlistContentF_reparto']//input")

            selected_options = 0
            target_options = len(self.target_numbers)
            while selected_options < target_options:
                self.actions.send_keys(Keys.ARROW_DOWN)
                self.actions.send_keys(Keys.SPACE)
                self.actions.perform()
                # Get the value of the 'value' attribute
                current_value = input_element.get_attribute("value")
                match = re.search(r'\d+$', current_value.strip())
                if match:
                    last_number = match.group()
                if last_number in self.target_numbers:
                    selected_options += 1
                else:
                    self.actions.send_keys(Keys.SPACE).perform()

        searchButton = self.driver.find_element(By.ID, "AvviaRicerca")
        searchButton.click()
        
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "printButtonT"))
        )
        printer = self.driver.find_element(By.ID, "printButtonT")
        printer.click() 
        grid = self.driver.find_element(By.ID, "griglia")
        grid.click()
        
        # Immediately switch to the last opened window
        self.driver.switch_to.window(self.driver.window_handles[-1])

        self.actions.key_down(Keys.CONTROL).send_keys('a').send_keys('c').key_up(Keys.CONTROL).perform()
        self.driver.quit()

    def print(self):
        # Start OpenOffice Calc
        app = Application(backend="uia").start(r"C:\Program Files (x86)\OpenOffice 4\program\soffice.exe --calc")

        # Wait for Calc to open and create a new spreadsheet
        time.sleep(3)  # Adjust based on system speed
        # Attach to the currently active window
        windows = Desktop(backend="win32").windows()

        # Loop through the windows and find the active one
        calc = None
        for window in windows:
            if window.is_active():
                calc = window
                break
        calc.type_keys("{DOWN}")  # Simulates pressing the down arrow key
        calc.type_keys("{ENTER}")  # Simulates pressing the Enter key
        time.sleep(2)

        # Paste the copied table into the spreadsheet
        calc.type_keys("^v")  # Simulates Ctrl + V to paste

        # Open the Save As dialog
        calc.type_keys("^s")  # Simulates Ctrl + S
        time.sleep(1)  # Ensure the Save As dialog has time to open

        # Get the current focused window after triggering Ctrl + S
        save_as = Desktop(backend="win32").window(title="Salva con nome")

        # Type the desired file name and path
        save_as.type_keys(full_file_path, with_spaces=True)

        # Press Enter to save the file
        save_as.type_keys("{ENTER}")  # Hit Enter to save the file
        time.sleep(10)
        calc.close()
