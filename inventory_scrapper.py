from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from datetime import date, timedelta
import time
import os
import pygetwindow as gw
import pyautogui
from credentials import PASSWORD, USERNAME

save_path = r"C:\Users\Ruggero\Documents\GitHub\lama-restock\Inventory"


class Inventory_Scrapper:

    def __init__(self) -> None:
        #Set up the Selenium WebDriver (Ensure to have the correct browser driver installed)
        chrome_options = Options()
        #chrome_options.add_argument("--headless")  # Run Chrome in headless mode
        #chrome_options.add_argument("--no-sandbox")  # Required for some environments
        #chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        #chrome_options.add_argument("--disable-gpu")  # Applicable only if you are running on Windows
        
        prefs = {
            "download.prompt_for_download": True,   # <- ask where to save
            "download.default_directory": "",       # <- empty disables forced folder
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        self.actions = ActionChains(self.driver)
        
    # Load the webpage
    def inventory(self):
        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta105"))
        )

        inventory_menu = self.driver.find_element(By.ID, "carta105")
        inventory_menu.click()

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

    def clean_up(self, target):
        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta1126"))
        )

        inventory_new = self.driver.find_element(By.ID, "carta1126")
        inventory_new.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        self.wait.until(
            EC.presence_of_element_located((By.ID, "sidebarButton"))
        )

        sidebar = self.driver.find_element(By.ID, "sidebarButton")
        sidebar.click()

        time.sleep(1) 

        
        lista_link = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='./rilevazione']"))
        )
        # Click the "Lista" link
        lista_link.click()

        time.sleep(1) 

        xpath_description = '/html/body/div[2]/div[2]/div[3]/div/div/div/div[2]/div/form/div[3]/div/smart-text-box/div[1]/div/input'
        description_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_description)))
        description_input.click()
        description_input.send_keys(target)

        xpath_date = '/html/body/div[2]/div[2]/div[3]/div/div/div/div[2]/div/form/div[4]/div[1]/div/smart-date-time-picker/div/div/input'

        # Wait until the input is clickable
        date_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_date)))

        # Compute yesterday’s date in DD/MM/YYYY format
        yesterday = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")

        # Click, select all, and replace with yesterday’s date
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
        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta1126"))
        )

        inventory_new = self.driver.find_element(By.ID, "carta1126")
        inventory_new.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        self.wait.until(
            EC.presence_of_element_located((By.ID, "sidebarButton"))
        )

        sidebar = self.driver.find_element(By.ID, "sidebarButton")
        sidebar.click()

        time.sleep(1) 

        
        lista_link = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[@href='./rilevazione']"))
        )
        # Click the "Lista" link
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
        self.wait.until(
            EC.presence_of_element_located((By.ID, "carta107"))
        )

        inventory_new = self.driver.find_element(By.ID, "carta107")
        inventory_new.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 1
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab
        
        time.sleep(1)

        xpath_client = '/html/body/div[25]/div[2]/div[3]/div/div/div[2]'
        client_button_dropdown = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_client)))
        client_button_dropdown.click()

        time.sleep(1)

        self.actions.send_keys(Keys.ARROW_DOWN).perform()
        self.actions.send_keys(Keys.ENTER).perform()      

        xpath_search = '/html/body/div[25]/div[2]/input[3]'
        search_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_search)))
        search_button.click()
        
        time.sleep(1)

        xpath_description = '/html/body/div[1]/div[3]/div/div/div[5]/div[1]/div[2]/div/div[10]/input' 
        description_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_description)))
        description_input.click()
        description_input.send_keys(target)

         # --- find which row contains the target text, then click that row's menu ---
        # wait until some cell with the exact target text appears in the table
        target_xpath = f'//*[normalize-space()="{target}"]'
        self.wait.until(EC.presence_of_element_located((By.XPATH, target_xpath)))

        # get all row containers (uses same section you were targeting with the absolute path)
        rows_xpath = '/html/body/div[1]/div[3]/div/div/div[5]/div[2]/div/div'
        rows = self.driver.find_elements(By.XPATH, rows_xpath)

        # find the first row whose visible text contains the target
        row_index = None
        for i, row in enumerate(rows, start=1):
            try:
                if target in row.text:
                    row_index = i
                    break
            except Exception:
                continue

        if row_index is None:
            raise RuntimeError(f"Filtered row for target='{target}' not found among {len(rows)} rows.")

        # build XPath for the menu in the matched row (keeps your original structure, just correct index)
        xpath_menu_for_row = f'/html/body/div[1]/div[3]/div/div/div[5]/div[2]/div/div[{row_index}]/div[1]'
        menu_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_menu_for_row)))
        menu_button.click()

        xpath_expo = '/html/body/div[2]/div/ul/li[1]'
        expo_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_expo)))
        expo_button.click()

        self.wait.until(
            lambda driver: len(driver.window_handles) > 2
        )

        self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the new tab

        xpath_expo2 = '/html/body/div[1]/div[1]/div/div/div[5]'
        expo_button2 = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_expo2)))
        expo_button2.click()

        xpath_expo3 = '/html/body/div[12]/div[2]/input'
        expo_button3 = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath_expo3)))
        expo_button3.click()

        self.save_file_with_name(target, save_path)
        self.close_current_tab_and_switch()

    def save_file_with_name(self, target:str, save_dir):
        """
        Waits for the Windows 'Save As' dialog to appear,
        then types the full save path and confirms with Enter.
        Automatically adds '.csv' to the filename if missing.
        """

        # Ensure .csv extension
        if not target.lower().endswith(".csv"):
            target += ".csv"

        # Build full file path
        full_path = os.path.join(save_dir, target)

        time.sleep(3)
        windows = gw.getWindowsWithTitle("Salva con nome")

        # Bring dialog to the front
        win = windows[0]
        win.activate()
        time.sleep(0.5)

        # Type the full file path and save
        pyautogui.write(full_path)
        pyautogui.press("enter")

        print(f"File saved as: {full_path}")        
        
    def close_current_tab_and_switch(self):
        """Closes the current browser tab and switches to the next available one."""
        handles = self.driver.window_handles
        current = self.driver.current_window_handle

        # Close the current tab
        self.driver.close()

        # Switch to another open tab (if any remain)
        remaining = [h for h in handles if h != current]
        if remaining:
            self.driver.switch_to.window(remaining[0])

        

        

