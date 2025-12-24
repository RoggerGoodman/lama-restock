# LamApp/supermarkets/scripts/inventory_scrapper.py
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import os
import sys
from django.conf import settings
import logging
import requests
import csv
from datetime import date, timedelta

logger = logging.getLogger(__name__)

# Save path for loss files (ROTTURE, SCADUTO, UTILIZZO INTERNO)
save_path = str(settings.LOSSES_FOLDER)
# Detect platform
IS_WINDOWS = sys.platform.startswith('win')
IS_LINUX = sys.platform.startswith('linux')

CSV_COLUMN_MAP = {
    "RilevazioniRigheCodiceArticolo": "Code",
    "RilevazioniRigheVarianteArticolo": "Variant",
    "RilevazioniRigheDescrizione": "Description",
    "RilevazioniRigheQuantitaOriginale": "Quantity"
}

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
        self.id_cliente = "31659" #TODO must be made dynamic depending on the user, could be problematic for user with multiple supermarket
        # Set up the Selenium WebDriver
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/chromium-browser"   # or /usr/bin/google-chrome-stable
        # Make direct download the default on all platforms (no prompt).
        if IS_LINUX:
            logger.info("Configuring Chrome for server/headless mode (direct download)")
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("--window-size=1920,1080")
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

        # Set a writable directory for Chrome to use
        user_data_dir = "/tmp/chrome-data"
        os.makedirs(user_data_dir, exist_ok=True)
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
        
        service = Service("/usr/bin/chromedriver")

        self.driver = webdriver.Chrome(service=service, options=chrome_options)
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
    
    def export_all_testate_from_day(self, days_back: int = 0):
        """
        Exports all available testate (ROTTURE, SCADUTO, UTILIZZO INTERNO)
        for the selected day range.
        """
        # -------------------------
        # DATE RANGE
        # -------------------------
        today = date.today()
        dal = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
        al = today.strftime("%Y-%m-%d")

        # -------------------------
        # SESSION (reuse Selenium login)
        # -------------------------
        session = requests.Session()
        for c in self.driver.get_cookies():
            session.cookies.set(c["name"], c["value"])

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://dropzone.pac2000a.it",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0",
        }

        # -------------------------
        # STEP 1: FETCH TESTATE
        # -------------------------
        url_testate = "https://dropzone.pac2000a.it/rilevazioni/RilevazioniTestate_call.php"

        payload_testate = {
            "funzione": "lista",
            "IDAzienda": "",
            "IDCliente": self.id_cliente,
            "DescRilevazione": "",
            "Dal": dal,
            "Al": al,
            "IsExported": "",
            "numRecord": 100,
        }

        resp = session.post(url_testate, headers=headers, data=payload_testate)
        resp.raise_for_status()
        testate = resp.json()

        if not testate:
            print("No testate found in date range.")
            return

        # -------------------------
        # GROUP TESTATE
        # -------------------------
        grouped = {}
        for t in testate:
            desc = t["RilevazioniTestateDescRilevazione"].strip()
            grouped.setdefault(desc, []).append(t)

        os.makedirs(save_path, exist_ok=True)

        # -------------------------
        # STEP 2: EXPORT EACH TESTATA
        # -------------------------
        url_righe = "https://dropzone.pac2000a.it/rilevazioni/RilevazioniRighe_call.php"
        ALLOWED_TYPES = {"ROTTURE", "SCADUTO", "UTILIZZO INTERNO"}

        csv_headers = list(CSV_COLUMN_MAP.values())

        for desc, items in grouped.items():
            if desc not in ALLOWED_TYPES:
                continue

            csv_path = os.path.join(save_path, f"{desc}.csv")

            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=csv_headers)
                writer.writeheader()

                for t in items:
                    id_testata = t["RilevazioniTestateIDRilevazioniTestata"]
                    num_righe = t.get("numRighe", "0")

                    print(f"Exporting {desc} | ID {id_testata} | Rows {num_righe}")

                    payload_righe = {
                        "funzione": "lista",
                        "IDRilevazioniTestata": id_testata,
                    }

                    resp = session.post(url_righe, headers=headers, data=payload_righe)
                    resp.raise_for_status()
                    righe = resp.json()

                    if not righe:
                        print(f"  → No rows for {desc} ({id_testata})")
                        continue

                    for r in righe:
                        row = {}
                        for src, dst in CSV_COLUMN_MAP.items():
                            row[dst] = r.get(src)

                        writer.writerow(row)

            print(f"  → Saved {csv_path}")

        print("All available testate exported.")