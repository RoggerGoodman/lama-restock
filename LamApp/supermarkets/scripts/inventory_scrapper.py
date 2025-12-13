# LamApp/supermarkets/scripts/inventory_scrapper.py
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
import time
import os
import sys
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

# Save path for loss files (ROTTURE, SCADUTO, UTILIZZO INTERNO)
save_path = str(settings.LOSSES_FOLDER)

# Detect platform
IS_WINDOWS = sys.platform.startswith('win')
IS_LINUX = sys.platform.startswith('linux')


class Inventory_Scrapper:

    def __init__(self, username: str, password: str) -> None:
        """
        Initialize inventory scrapper with credentials.
        
        Args:
            username: PAC2000A username
            password: PAC2000A password
        """
        self.username = username
        self.password = password
        
        # Set up the Selenium WebDriver
        chrome_options = Options()

        # Make direct download the default on all platforms (no prompt).
        if IS_LINUX:
            logger.info("Configuring Chrome for server/headless mode (direct download)")
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
        else:
            logger.info("Configuring Chrome for local mode (direct download preferred)")

        # Direct download configuration - NO dialog (works on Linux + Windows)
        prefs = {
            "download.default_directory": save_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        self.use_save_dialog = False

        # Suppress Chrome DevTools and other noise
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument('--log-level=3')

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
        username_field.send_keys(self.username)
        password_field.send_keys(self.password)
        self.actions.send_keys(Keys.ENTER)
        self.actions.perform()
        
        logger.info(" Login successful")

    def downloader(self, target):
        """
        Download loss files from PAC2000A.
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

            time.sleep(2)

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

            time.sleep(2)

            # Enter target in description filter
            xpath_description = '/html/body/div[1]/div[3]/div/div/div[5]/div[1]/div[2]/div/div[10]/input'
            description_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_description)))
            description_input.click()
            description_input.send_keys(target)

            time.sleep(2)

            # Find the row with exact target text
            target_xpath = f'//*[normalize-space()="{target}"]'

            try:
                self.wait.until(EC.presence_of_element_located((By.XPATH, target_xpath)))
            except TimeoutException:
                logger.error(f"Target '{target}' not found in results after filtering")
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
                logger.error(f"Could not find row for target: {target}")
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
            preexisting = set()
            try:
                preexisting = set(os.listdir(save_path))
            except Exception as e:
                logger.warning(f"Could not list save_path before download snapshot: {e}")

            xpath_expo3 = '/html/body/div[12]/div[2]/input'
            expo_button3 = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_expo3)))

            # Trigger export/download
            expo_button3.click()

            # Wait for the new download to appear and rename it
            renamed = self.wait_for_new_download_and_rename(preexisting, target, save_path, timeout=45)

            if not renamed:
                logger.error(f"Could not retrieve or rename downloaded file for target: {target}")
            else:
                logger.info(f" Download saved as: {renamed}")

            time.sleep(2)

            self.close_current_tab_and_switch()

            logger.info(f" Successfully downloaded: {target}")

        except Exception as e:
            logger.exception(f"Error downloading {target}")
            try:
                self.close_current_tab_and_switch()
            except:
                pass
    
    def wait_for_new_download_and_rename(self, preexisting_files, target: str, save_dir: str, timeout: int = 30):
        """Wait for download and rename to target filename"""
        if not target.lower().endswith(".csv"):
            target = target + ".csv"

        expected_path = os.path.join(save_dir, target)

        start_time = time.time()
        logger.info(f"Waiting up to {timeout}s for new download in {save_dir}... (expect: {target})")

        while time.time() - start_time < timeout:
            try:
                current_files = set(os.listdir(save_dir))
            except Exception as e:
                logger.warning(f"Could not list download directory: {e}")
                current_files = set()

            new_files = current_files - set(preexisting_files)
            
            if new_files:
                csv_candidates = [f for f in new_files if f.lower().endswith(".csv")]
                if csv_candidates:
                    csv_paths = [os.path.join(save_dir, f) for f in csv_candidates]
                    csv_paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                    src = csv_paths[0]
                    
                    final = expected_path
                    if os.path.exists(final):
                        base, ext = os.path.splitext(expected_path)
                        final = f"{base}_{int(time.time())}{ext}"
                        logger.info(f"Target exists, using unique final name: {final}")
                    
                    try:
                        os.replace(src, final)
                        logger.info(f"Renamed downloaded file '{src}' -> '{final}'")
                        return final
                    except Exception as e:
                        logger.exception(f"Failed to rename download file {src} -> {final}: {e}")
                        return False

                crdownloads = [f for f in new_files if f.lower().endswith(".crdownload")]
                if crdownloads:
                    logger.info(f"Download in progress (.crdownload present): {crdownloads}")
            
            time.sleep(1)

        logger.error(f"Download timeout after {timeout}s for expected target: {target}")
        return False
        
    def close_current_tab_and_switch(self):
        """Close the current browser tab and switch to next available one"""
        handles = self.driver.window_handles
        current = self.driver.current_window_handle

        self.driver.close()

        remaining = [h for h in handles if h != current]
        if remaining:
            self.driver.switch_to.window(remaining[0])