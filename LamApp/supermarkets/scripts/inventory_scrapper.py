from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import date, timedelta
import time
import os
import pygetwindow as gw
import pyautogui
from .constants import PASSWORD, USERNAME
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

# Save path for loss files (ROTTURE, SCADUTO, UTILIZZO INTERNO)
save_path = str(settings.LOSSES_FOLDER)


class Inventory_Scrapper:

    def __init__(self) -> None:
        # Set up the Selenium WebDriver
        chrome_options = Options()
        #chrome_options.add_argument("--headless")  # Uncomment for headless mode
        #chrome_options.add_argument("--no-sandbox")
        #chrome_options.add_argument("--disable-dev-shm-usage")
        #chrome_options.add_argument("--disable-gpu")
        
        prefs = {
            "download.prompt_for_download": True,   # Ask where to save
            "download.default_directory": "",       # Empty disables forced folder
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        self.actions = ActionChains(self.driver)
        
    def inventory(self):
        """Navigate to inventory section"""
        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta105"))
        )

        inventory_menu = self.driver.find_element(By.ID, "carta105")
        inventory_menu.click()

    def login(self):
        """Login to PAC2000A"""
        logger.info("Logging in to PAC2000A...")
        self.driver.get('https://dropzone.pac2000a.it/')

        # Wait for login page
        self.wait.until(
            EC.presence_of_element_located((By.ID, "username"))
        )

        # Enter credentials
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "password")
        username_field.send_keys(USERNAME)
        password_field.send_keys(PASSWORD)
        self.actions.send_keys(Keys.ENTER)
        self.actions.perform()
        
        logger.info("✓ Login successful")

    def clean_up(self, target):
        """Clean up old inventory entries (not used for loss recording)"""
        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta1126"))
        )

        inventory_new = self.driver.find_element(By.ID, "carta1126")
        inventory_new.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])

        self.wait.until(
            EC.presence_of_element_located((By.ID, "sidebarButton"))
        )

        sidebar = self.driver.find_element(By.ID, "sidebarButton")
        sidebar.click()

        time.sleep(1) 

        lista_link = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='./rilevazione']"))
        )
        lista_link.click()

        time.sleep(1)

        xpath_description = '/html/body/div[2]/div[2]/div[3]/div/div/div/div[2]/div/form/div[3]/div/smart-text-box/div[1]/div/input'
        description_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_description)))
        description_input.click()
        description_input.send_keys(target)

        xpath_date = '/html/body/div[2]/div[2]/div[3]/div/div/div/div[2]/div/form/div[4]/div[1]/div/smart-date-time-picker/div/div/input'

        date_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_date)))

        # Yesterday's date
        yesterday = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")

        date_input.click()
        date_input.send_keys(Keys.CONTROL, 'a')
        date_input.send_keys(yesterday)

        xpath_confirm_button = '/html/body/div[2]/div[2]/div[3]/div/div/div/div[3]/div/smart-button[2]/button'
        confirm_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_confirm_button)))
        confirm_button.click()

        xpath_menu = '//*[@id="grid"]/div[1]/div[5]/div[2]/div[2]/smart-grid-row[1]/div[1]/smart-grid-cell[2]/div/div[5]'
        menu_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_menu)))
        menu_button.click()

        xpath_erase = '/html/body/div[2]/div[2]/smart-menu/div[1]/div[2]/smart-menu-item[5]/div/div/span'
        erase_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_erase)))
        erase_button.click()

        xpath_confirm_button2 = '/html/body/div[2]/div[2]/div[6]/div/div/div/div[3]/div/smart-button[2]/button'
        confirm_button2 = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_confirm_button2)))
        confirm_button2.click()

        self.close_current_tab_and_switch()

    def inventory_creator(self, target):
        """Create new inventory entry (not used for loss recording)"""
        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta1126"))
        )

        inventory_new = self.driver.find_element(By.ID, "carta1126")
        inventory_new.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])

        self.wait.until(
            EC.presence_of_element_located((By.ID, "sidebarButton"))
        )

        sidebar = self.driver.find_element(By.ID, "sidebarButton")
        sidebar.click()

        time.sleep(1)

        lista_link = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='./rilevazione']"))
        )
        lista_link.click()

        time.sleep(1)

        xpath_confirm_button = '/html/body/div[2]/div[2]/div[3]/div/div/div/div[3]/div/smart-button[2]/button'
        confirm_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_confirm_button)))
        confirm_button.click()

        add_button = self.wait.until(EC.element_to_be_clickable((By.ID, "newRowButtonMenuSopra")))
        add_button.click()

        xpath_description = '/html/body/div[2]/div[2]/div[4]/div/div/div/div[2]/div/form/div[2]/div/smart-text-box/div[1]/div/input'
        description_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_description)))
        description_input.click()
        description_input.send_keys(target)

        xpath_confirm_button = '/html/body/div[2]/div[2]/div[4]/div/div/div/div[3]/div/smart-button[2]/button'
        confirm_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_confirm_button)))
        confirm_button.click()

        self.close_current_tab_and_switch()

    def downloader(self, target):
        """
        Download loss files from PAC2000A.
        
        CRITICAL: This is the main method used for automated loss recording.
        Downloads ROTTURE, SCADUTO, or UTILIZZO INTERNO files.
        """
        logger.info(f"Starting download for: {target}")
        
        try:
            self.wait.until(
                EC.presence_of_element_located((By.ID, "carta107"))
            )

            inventory_new = self.driver.find_element(By.ID, "carta107")
            inventory_new.click()

            self.wait.until(
                lambda driver: len(driver.window_handles) > 1
            )

            self.driver.switch_to.window(self.driver.window_handles[-1])
            
            time.sleep(2)  # Wait for page load

            # Select client
            xpath_client = '/html/body/div[25]/div[2]/div[3]/div/div/div[2]'
            client_button_dropdown = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_client)))
            client_button_dropdown.click()

            time.sleep(1)

            self.actions.send_keys(Keys.ARROW_DOWN).perform()
            self.actions.send_keys(Keys.ENTER).perform()

            # Click search button
            xpath_search = '/html/body/div[25]/div[2]/input[3]'
            search_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_search)))
            search_button.click()
            
            time.sleep(2)  # Wait for results

            # Enter target in description filter
            xpath_description = '/html/body/div[1]/div[3]/div/div/div[5]/div[1]/div[2]/div/div[10]/input' 
            description_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_description)))
            description_input.click()
            description_input.send_keys(target)
            
            time.sleep(2)  # Wait for filtering

            # Find the row with exact target text
            target_xpath = f'//*[normalize-space()="{target}"]'
            
            try:
                self.wait.until(EC.presence_of_element_located((By.XPATH, target_xpath)))
            except TimeoutException:
                logger.error(f"✗ Target '{target}' not found in results after filtering")
                self.close_current_tab_and_switch()
                return

            # Get all rows
            rows_xpath = '/html/body/div[1]/div[3]/div/div/div[5]/div[2]/div/div'
            rows = self.driver.find_elements(By.XPATH, rows_xpath)

            # Find row containing target
            row_index = None
            for i, row in enumerate(rows, start=1):
                try:
                    if target in row.text:
                        row_index = i
                        break
                except Exception:
                    continue

            if row_index is None:
                logger.error(f"✗ Could not find row for target: {target}")
                self.close_current_tab_and_switch()
                return

            logger.info(f"Found target at row index: {row_index}")

            # Click menu for the row
            xpath_menu_for_row = f'/html/body/div[1]/div[3]/div/div/div[5]/div[2]/div/div[{row_index}]/div[1]'
            menu_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_menu_for_row)))
            menu_button.click()

            time.sleep(1)

            # Click export
            xpath_expo = '/html/body/div[2]/div/ul/li[1]'
            expo_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_expo)))
            expo_button.click()

            self.wait.until(
                lambda driver: len(driver.window_handles) > 2
            )

            self.driver.switch_to.window(self.driver.window_handles[-1])

            time.sleep(1)

            # Click export again
            xpath_expo2 = '/html/body/div[1]/div[1]/div/div/div[5]'
            expo_button2 = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_expo2)))
            expo_button2.click()

            time.sleep(1)

            # Final export click
            xpath_expo3 = '/html/body/div[12]/div[2]/input'
            expo_button3 = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_expo3)))
            expo_button3.click()

            # Save file
            self.save_file_with_name(target, save_path)
            
            time.sleep(2)  # Wait for download
            
            self.close_current_tab_and_switch()
            
            logger.info(f"✓ Successfully downloaded: {target}")
            
        except Exception as e:
            logger.exception(f"✗ Error downloading {target}")
            # Try to recover by closing tabs
            try:
                self.close_current_tab_and_switch()
            except:
                pass

    def save_file_with_name(self, target: str, save_dir):
        """
        Wait for Windows 'Save As' dialog and save file with target name.
        Automatically adds '.csv' extension.
        """
        # Ensure .csv extension
        if not target.lower().endswith(".csv"):
            target += ".csv"

        # Build full file path
        full_path = os.path.join(save_dir, target)

        logger.info(f"Saving file to: {full_path}")

        time.sleep(3)
        
        try:
            windows = gw.getWindowsWithTitle("Save as")

            if not windows:
                logger.warning("Save As dialog not found, file may have downloaded automatically")
                return

            # Bring dialog to front
            win = windows[0]
            win.activate()
            time.sleep(0.5)

            # Type the full file path and save
            pyautogui.write(full_path)
            pyautogui.press("enter")

            logger.info(f"✓ File saved as: {full_path}")
            
        except Exception as e:
            logger.error(f"Error in save dialog: {e}")
        
    def close_current_tab_and_switch(self):
        """Close the current browser tab and switch to next available one"""
        handles = self.driver.window_handles
        current = self.driver.current_window_handle

        # Close the current tab
        self.driver.close()

        # Switch to another open tab (if any remain)
        remaining = [h for h in handles if h != current]
        if remaining:
            self.driver.switch_to.window(remaining[0])
