import re
import os
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementClickInterceptedException
import time
from pywinauto import Application, Desktop
from selenium.webdriver.chrome.options import Options
from credentials import PASSWORD, USERNAME
storages = ["01 RIANO GENERI VARI", "23 S.PALOMBA SURGELATI", "02 POMEZIA DEPERIBILI"]
desired_value = "01 RIANO GENERI VARI"
save_path = r"C:\Users\Ruggero\Documents\GitHub\lama-restock\Database"
full_file_path = rf"{save_path}\{desired_value}"
filters = False

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
        self.target_filters3 = {"300", "350", "600"}
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
            EC.presence_of_element_located((By.ID, "carta31"))
        )

        list_menu = self.driver.find_element(By.ID, "carta31")
        list_menu.click()

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "carta139"))
        )

        list_menu1 = self.driver.find_element(By.ID, "carta139")
        list_menu1.click()

        WebDriverWait(self.driver, 10).until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "linkListino"))
        )

        time.sleep(1) 

        orders_menu1 = self.driver.find_element(By.ID, "linkListino")
        orders_menu1.click()
    
        time.sleep(1)

    def selector(self):

        storage = re.sub(r'^\d+\s+', '', desired_value)
        input_element = self.driver.find_element(By.XPATH, "//*[@id='idMagConsegnaActionButton']/input")
        input_element.clear()
        input_element.send_keys(storage)
        drop_down_arrow = self.driver.find_element(By.XPATH, "//*[@id='idMagConsegna']/div[1]/div/div[1]/span[2]")
        drop_down_arrow.click()
        time.sleep(1)
        element = self.driver.find_element(By.XPATH, f'//smart-list-item[@label="{storage}"]')
        element.click()
        time.sleep(1)
        try:
            confirm_button = self.driver.find_element(By.XPATH, "//*[@id='confermaFooterFiltro']/button")
            confirm_button.click()
        except ElementClickInterceptedException:
            # Find all buttons with that ID and click the visible one (top of stack)
            close_buttons = self.driver.find_elements(By.XPATH, "//*[@id='chiudifooterFiltro']/button")
            
            for button in close_buttons:
                for button in close_buttons:
                    if button.is_displayed() and button.accessible_name == 'CHIUDI':
                        try:
                            button.click()
                        except ElementClickInterceptedException:
                            continue
                        break
                break
            
            # Wait a moment for modal to close
            time.sleep(0.5)
            
            # Try clicking the confirm button again
            confirm_button = self.driver.find_element(By.XPATH, "//*[@id='confermaFooterFiltro']/button")
            confirm_button.click()
            time.sleep(10)


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


    def download(self):
        menu_element = self.driver.find_element(By.XPATH, '//*[@id="menuStrumenti"]')
        self.actions.move_to_element(menu_element).perform()

        # Wait a moment for the submenu to appear (optional but recommended)
        time.sleep(0.3)

        # Click the export option
        export_element = self.driver.find_element(By.XPATH, '//*[@id="exportGridStrumenti"]')
        export_element.click()
        time.sleep(0.3)
        exl_button = self.driver.find_element(By.XPATH, '//*[@id="xlsxBtn"]')
        exl_button.click()
        # Wait for download to complete
        time.sleep(10)  # Adjust based on file size

        # Get the download folder (default Chrome/Firefox download location)
        download_folder = os.path.join(os.path.expanduser('~'), 'Downloads')
        source_file = os.path.join(download_folder, 'SmartGrid.xlsx')

        # Define destination folder and new filename
        destination_folder = './Lists/'
        new_filename = f"{desired_value}.xlsx"
        destination_file = os.path.join(destination_folder, new_filename)

        # Wait until file exists
        while not os.path.exists(source_file):
            time.sleep(0.5)

        # Move and rename the file
        shutil.move(source_file, destination_file)