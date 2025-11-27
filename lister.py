import re
import os
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException, TimeoutException
import time
from selenium.webdriver.chrome.options import Options
from LamApp.supermarkets.scripts.constants import PASSWORD, USERNAME
storages = ["01 RIANO GENERI VARI", "23 S.PALOMBA SURGELATI", "02 POMEZIA DEPERIBILI"]
desired_value = "01 RIANO GENERI VARI"
filters = True

class Lister :
    def __init__(self) -> None:
        # Set up the Selenium WebDriver (Ensure to have the correct browser driver installed)
        chrome_options = Options()
        chrome_options.add_argument("--kiosk-printing")  # Automatically handles print dialogs
        # chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        # chrome_options.add_argument("--no-sandbox")  # Required for some environments
        # chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        # chrome_options.add_argument("--disable-gpu")  # Applicable only if you are running on Windows

        # Define the set of numbers to check
        self.target_filters1 = {"100", "600", "620", "800", "820"}
        self.target_filters3 = {"300", "350", "600"}
        # Initialize WebDriver with the options
        self.driver = webdriver.Chrome(options=chrome_options)
        self.actions = ActionChains(self.driver)
        self.wait = WebDriverWait(self.driver, 300)

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

        list_menu = self.driver.find_element(By.ID, "carta31")
        list_menu.click()

        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta139"))
        )

        list_menu1 = self.driver.find_element(By.ID, "carta139")
        list_menu1.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        self.wait.until(
            EC.presence_of_element_located((By.ID, "linkListino"))
        )

        time.sleep(1) 

        orders_menu1 = self.driver.find_element(By.ID, "linkListino")
        orders_menu1.click()
    
        time.sleep(1)

    def close_ordini_popup(self):
        try:
            # Check presence of the popup by its title
            popup_title = self.driver.find_element(By.XPATH, "//h4[normalize-space()='Elenco Ordini In Corso']")
            
            # If found, wait for the Chiudi button and click it
            chiudi_btn = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[2]/div[8]/div/div/div/div[3]/div/smart-button"))
            )
            chiudi_btn.click()
            print("Popup 'Elenco Ordini In Corso' found and closed.")

        except NoSuchElementException:
            # Popup not present → do nothing
            pass
        except TimeoutException:
            # Popup present but button didn't load → fail silently or handle as needed
            print("Popup detected but Chiudi button not clickable.")

    def selector(self):

        storage = re.sub(r'^\d+\s+', '', desired_value)
        input_element = self.driver.find_element(By.XPATH, "//*[@id='idMagConsegnaActionButton']/input")
        input_element.clear()
        input_element.send_keys(storage)
        time.sleep(1)
        element = self.driver.find_element(By.XPATH, f'//smart-list-item[@label="{storage}"]')
        element.click()
        time.sleep(1)
        self.close_ordini_popup()
        drop_down_arrow_1 = self.driver.find_element(By.XPATH, "//*[@id='idMagStatoAssort']/div[1]/div/div[1]/span[2]")
        drop_down_arrow_1.click()
        time.sleep(1)
        state_elemnt = self.driver.find_element(By.XPATH, "//span[normalize-space()='SOSPESO LISTINO/NO COMUNIC.VAR']")
        state_elemnt.click()
        drop_down_arrow_2 = self.driver.find_element(By.XPATH, "//*[@id='idRepartoFilter']/div[1]/div/div[1]/span[2]")
        drop_down_arrow_2.click()
        time.sleep(1)
        if storage == "S.PALOMBA SURGELATI":
            filters = False
        if filters:
            match storage:
                case "RIANO GENERI VARI":
                    target_filters = {"100", "600", "620", "800", "820"} #Sala Generi Vari, Ortofrutta, Frutta secca, Non-Food, Extra-Alimentare
                case "POMEZIA DEPERIBILI":
                    target_filters = {"300", "350", "600"} #Murale Salumi/Latticini, Pane, Ortofrutta
                

            list_items = self.driver.find_elements(By.CSS_SELECTOR, "smart-list-item")

            for item in list_items:
                label = item.get_attribute("label")   # e.g. "100 - SALA GENERI VARI"

                if not label:
                    continue

                code = label.split(" - ")[0].strip()

                if code in target_filters:
                    # Scroll into view
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item)

                    # Small delay to let the scroll finish (optional but helps with smart widgets)
                    time.sleep(0.2)
                    # Click it
                    item.click()
        
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
        self.wait.until(
            EC.invisibility_of_element_located((By.ID, "loadingWindow"))
        )

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