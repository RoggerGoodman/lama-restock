# LamApp/supermarkets/scripts/web_lister.py
"""
Web-integrated version of the Lister script.
Downloads product list Excel files from PAC2000A automatically.
"""
import re
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException, TimeoutException
import time
from selenium.webdriver.chrome.options import Options
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class WebLister:
    """
    Downloads product list Excel files from PAC2000A.
    Adapted for web app usage - no hardcoded values.
    """
    
    def __init__(self, username: str, password: str, storage_name: str, 
                 download_dir: str, headless: bool = True):
        """
        Initialize the lister.
        
        Args:
            username: PAC2000A username
            password: PAC2000A password
            storage_name: Storage name (e.g., "01 RIANO GENERI VARI")
            download_dir: Directory to save downloaded files
            headless: Run browser in headless mode (no UI)
        """
        self.username = username
        self.password = password
        self.storage_name = storage_name
        self.download_dir = download_dir
        
        # Extract settore name (remove numeric prefix)
        self.settore = re.sub(r'^\d+\s+', '', storage_name)
        
        # Setup Chrome options
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/chromium-browser"   # or /usr/bin/google-chrome-stable
        chrome_options.add_argument("--kiosk-printing")
        
        if headless:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            #chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            chrome_options.add_argument('--log-level=3')  # Suppress console logs
        
        # Set download directory
        prefs = {
            "download.default_directory": str(Path(download_dir).absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        service = Service("/usr/bin/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.actions = ActionChains(self.driver)
        self.wait = WebDriverWait(self.driver, 300)
        
        logger.info(f"WebLister initialized for storage: {storage_name}")

    def login(self):
        """Login to PAC2000A and navigate to product list"""
        logger.info("Logging in to PAC2000A...")
        
        self.driver.get('https://dropzone.pac2000a.it/')
        
        self.wait.until(EC.presence_of_element_located((By.ID, "username")))
        
        username_field = self.driver.find_element(By.ID, "username")
        password_field = self.driver.find_element(By.ID, "password")
        username_field.send_keys(self.username)
        password_field.send_keys(self.password)
        self.actions.send_keys(Keys.ENTER)
        self.actions.perform()
        
        self.wait.until(EC.presence_of_element_located((By.ID, "carta31")))
        
        list_menu = self.driver.find_element(By.ID, "carta31")
        list_menu.click()
        
        self.wait.until(EC.presence_of_element_located((By.ID, "carta139")))
        
        list_menu1 = self.driver.find_element(By.ID, "carta139")
        list_menu1.click()
        
        self.wait.until(lambda driver: len(driver.window_handles) > 1)
        
        self.driver.switch_to.window(self.driver.window_handles[-1])
        
        self.wait.until(EC.presence_of_element_located((By.ID, "linkListino")))
        
        time.sleep(1)
        
        orders_menu1 = self.driver.find_element(By.ID, "linkListino")
        orders_menu1.click()
        
        time.sleep(1)
        logger.info("Navigation completed")

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

    def apply_filters(self):
        """Apply storage selection and filters"""
        logger.info(f"Applying filters for settore: {self.settore}")
        
        # Select storage
        input_element = self.driver.find_element(
            By.XPATH, "//*[@id='idMagConsegnaActionButton']/input"
        )
        input_element.clear()
        input_element.send_keys(self.settore)
        time.sleep(1)
        
        element = self.driver.find_element(
            By.XPATH, f'//smart-list-item[@label="{self.settore}"]'
        )
        element.click()
        time.sleep(1)
        
        #pop-up killer
        self.close_ordini_popup()

        # Select state
        #drop_down_arrow_1 = self.driver.find_element(
        #    By.XPATH, "//*[@id='idMagStatoAssort']/div[1]/div/div[1]/span[2]"
        #)
        #drop_down_arrow_1.click()
        time.sleep(1)
        #TODO To be re-implemented
        #state_element = self.driver.find_element(
        #    By.XPATH, "//span[normalize-space()='SOSPESO LISTINO/NO COMUNIC.VAR']"
        #)
        #state_element.click()
        
        # Apply category filters based on storage type
        self._apply_category_filters()
        time.sleep(1)
        # Confirm filters
        try:
            confirm_button = self.driver.find_element(
                By.XPATH, "//*[@id='confermaFooterFiltro']/button"
            )
            confirm_button.click()
        except ElementClickInterceptedException:
            # Handle modal overlap
            close_buttons = self.driver.find_elements(
                By.XPATH, "//*[@id='chiudifooterFiltro']/button"
            )
            
            for button in close_buttons:
                if button.is_displayed() and button.accessible_name == 'CHIUDI':
                    try:
                        button.click()
                        break
                    except ElementClickInterceptedException:
                        continue
            
            time.sleep(0.5)
            confirm_button = self.driver.find_element(
                By.XPATH, "//*[@id='confermaFooterFiltro']/button"
            )
            confirm_button.click()
        
        self.wait.until(
            EC.invisibility_of_element_located((By.ID, "loadingWindow"))
        )
        logger.info("Filters applied successfully")

    def _apply_category_filters(self):
        """Apply category filters based on storage type"""
        # Determine which filters to apply
        if self.settore == "S.PALOMBA SURGELATI":
            # No filters for frozen storage
            logger.info("No category filters for frozen storage")
            return
        
        if self.settore == "RIANO GENERI VARI":
            target_filters = {
                "100",  # Sala Generi Vari
                "600",  # Ortofrutta
                "620",  # Frutta secca
                "800",  # Non-Food
                "820"   # Extra-Alimentare
            }
        elif self.settore == "POMEZIA DEPERIBILI":
            target_filters = {
                "300",  # Murale Salumi/Latticini
                "350",  # Pane
                "600"   # Ortofrutta
            }
        else:
            # No filters for unknown storage types
            logger.info(f"No predefined filters for {self.settore}")
            return
        
        logger.info(f"Applying category filters: {target_filters}")
        
        # Open category dropdown
        drop_down_arrow_2 = self.driver.find_element(
            By.XPATH, "//*[@id='idRepartoFilter']/div[1]/div/div[1]/span[2]"
        )
        drop_down_arrow_2.click()
        time.sleep(1)
        
        # Select matching categories
        list_items = self.driver.find_elements(By.CSS_SELECTOR, "smart-list-item")
        
        for item in list_items:
            label = item.get_attribute("label")
            
            if not label:
                continue
            
            code = label.split(" - ")[0].strip()
            
            if code in target_filters:
                # Scroll into view
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", item
                )
                time.sleep(0.2)
                item.click()

    def download_excel(self) -> str:
        """
        Download the product list Excel file.
        
        Returns:
            str: Path to downloaded file
        """
        logger.info("Initiating download...")
        
        # Open export menu
        menu_element = self.driver.find_element(By.XPATH, '//*[@id="menuStrumenti"]')
        self.actions.move_to_element(menu_element).perform()
        time.sleep(0.3)
        
        export_element = self.driver.find_element(By.XPATH, '/html/body/div[1]/nav[2]/div[1]/ul/li[5]/button') 
        export_element.click()
        time.sleep(0.3)
        
        excel_button = self.driver.find_element(By.XPATH, '//*[@id="xlsxBtn"]')
        excel_button.click()
        
        # Wait for download
        logger.info("Waiting for download to complete...")
        time.sleep(10)
        
        # Find downloaded file
        temp_file = Path(self.download_dir) / 'SmartGrid.xlsx'
        
        # Wait until file exists
        max_wait = 60  # seconds
        waited = 0
        while not temp_file.exists() and waited < max_wait:
            time.sleep(1)
            waited += 1
        
        if not temp_file.exists():
            raise FileNotFoundError(f"Download failed: {temp_file} not found after {max_wait}s")
        
        # Rename to storage-specific name
        final_file = Path(self.download_dir) / f"{self.storage_name}.xlsx"
        
        if final_file.exists():
            final_file.unlink()  # Remove old file
        
        shutil.move(str(temp_file), str(final_file))
        
        logger.info(f"Download completed: {final_file}")
        return str(final_file)

    def run(self) -> str:
        """
        Execute the complete download workflow.
        
        Returns:
            str: Path to downloaded Excel file
        """
        try:
            self.login()
            self.apply_filters()
            file_path = self.download_excel()
            return file_path
        finally:
            self.driver.quit()


def download_product_list(username: str, password: str, storage_name: str, 
                          download_dir: str, headless: bool = True) -> str:
    """
    Convenience function to download product list.
    
    Args:
        username: PAC2000A username
        password: PAC2000A password
        storage_name: Storage name (e.g., "01 RIANO GENERI VARI")
        download_dir: Directory to save downloaded files
        headless: Run browser in headless mode
    
    Returns:
        str: Path to downloaded Excel file
    """
    lister = WebLister(username, password, storage_name, download_dir, headless)
    return lister.run()
