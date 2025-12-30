# LamApp/supermarkets/scripts/web_lister.py
"""
Web-integrated version of the Lister script.
Downloads product list Excel files from PAC2000A automatically.
"""
import re
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from pathlib import Path
import logging
import requests
from datetime import date
import time
import os, uuid, shutil

logger = logging.getLogger(__name__)

CSV_COLUMN_MAP = {
    "arCodiceArticolo": "Code",
    "arVarianteArticolo": "Variant",
    "arDescrizione": "Description",
    "Imballo": "Package",
    "arRapportoCessioneVendita": "Multiplier",
    "disponibilita2": "Availability",
    "cessione": "Cost",
    "vendita": "Price",
    "reDescrizione": "Category",
}


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
        self.StatoAssIn=[16, 13] #TODO must be made user selectable (there are more than just these 2 options... sadly)
        self.IDCliente=31659 #TODO must be made dynamic in future
        self.dataIntercettaPrezzi = date.today().strftime("%Y-%m-%d")
        
        # Extract settore name (remove numeric prefix)
        self.settore = re.sub(r'^\d+\s+', '', storage_name)

        self.user_data_dir = f"/tmp/chrome-{uuid.uuid4()}"
        os.makedirs(self.user_data_dir, exist_ok=True)
        os.chmod(self.user_data_dir, 0o700)

        os.environ["HOME"] = self.user_data_dir
        os.environ["XDG_RUNTIME_DIR"] = self.user_data_dir

        # Setup Chrome options
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/google-chrome"
        
        if headless:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-setuid-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
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

        # Set a writable directory for Chrome to use
        chrome_options.add_argument(f"--user-data-dir={self.user_data_dir}")

        service = Service("/usr/local/bin/chromedriver")
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

    def apply_category_filters(self): #TODO storages are fixed, must be made dynamic to accomodate different areas/clients
        """Apply category filters based on storage type"""
        self.output_path = Path(self.download_dir) / f"{self.storage_name}.csv"
        if self.settore == "RIANO GENERI VARI":
            self.IDCodMag=46
            self.RepartoIn=[28, 44, 76, 50, 52] # Sala Generi Vari, Ortofrutta, Frutta secca, Non-Food, Extra-Alimentare
        elif self.settore == "POMEZIA DEPERIBILI":
            self.IDCodMag=47
            self.RepartoIn=[28, 30, 34, 44] # Sala Generi Vari, Murale Salumi/Latticini, Pane, Ortofrutta
            28,30,34,44
        elif self.settore == "S.PALOMBA SURGELATI":
            self.IDCodMag=49
            self.RepartoIn=[38] #Surgelati
        else:
            # No filters for unknown storage types
            logger.info(f"No predefined filters for {self.settore}")
            return

    def fetch_listino(
        self,
        IDAzienda: int = 2,
        IDMarchio: int = 10,
        numRecord: int = 5000):
        """
        Fetch listino products from PAC2000A (Listino_callV2.php)
        Requires an authenticated Selenium driver.
        """

        url = "https://dropzone.pac2000a.it/anagrafiche/Listino_callV2.php"

        # --- payload ---
        payload = {
            "funzione": "lista",
            "IDAzienda": IDAzienda,
            "IDCliente": self.IDCliente,
            "IDMarchio": IDMarchio,
            "IDCodMag": self.IDCodMag,
            "dexArt": "",
            "codiceBarre": "",
            "Livello1": "",
            "Livello2": "",
            "Livello3": "",
            "Livello4": "",
            "StatoAssIn": ",".join(map(str, self.StatoAssIn)),
            "numRecord": numRecord,
            "IDOrdine": "",
            "Riclassificatore2In": "",
            "articoliMarchio": "",
            "itemStagionalita": "",
            "posizioneDomandaIn": "",
            "dataIntercettaPrezzi": self.dataIntercettaPrezzi,
            "RepartoIn": ",".join(map(str, self.RepartoIn)),
            "dayInterval": 3,
            "IDClientiCanale": 74,
            "IDClientiArea": 40,
            "IDFornitore": "",
            "separaLivelloMerceologia": "S",
            "AreePreparazioneIN": "",
            "IDArticolo": "",
            "isAcqUltimaSettimana": 0,
            "codiceEtichettaVisualizza": "",
        }

        # --- headers ---
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://dropzone.pac2000a.it/ordini/gestione/listino",
            "User-Agent": self.driver.execute_script("return navigator.userAgent;"),
        }

        # --- copy cookies from selenium ---
        session = requests.Session()
        for c in self.driver.get_cookies():
            session.cookies.set(c["name"], c["value"])

        # --- request ---
        response = session.post(url, data=payload, headers=headers, timeout=600)
        response.raise_for_status()

        return response.json()

    def save_listino_to_csv(self, data: list[dict], column_map: dict = CSV_COLUMN_MAP):
        products = [row for row in data if is_real_product(row)]

        if not products:
            raise ValueError("No valid products found to export")

        source_fields = list(column_map.keys())
        csv_headers = list(column_map.values())

        with self.output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)

            # Write header row
            writer.writerow(csv_headers)

            # Write data rows
            for row in products:
                writer.writerow([row.get(field, "") for field in source_fields])
        

        return self.output_path        
    
    def run(self) -> str:
            """
            Execute the complete download workflow.
            
            Returns:
                str: Path to downloaded Excel file
            """
            try:
                self.login()
                self.apply_category_filters()
                self.data = self.fetch_listino()
                file_path = self.save_listino_to_csv(self.data)
                return file_path
            finally:
                self.driver.quit()
                shutil.rmtree(self.user_data_dir, ignore_errors=True)

    def gather_missing_product_data(self, cod, v):
        """
        Fetch missing product data from ArticoliDecodifica_call.php
        using CodiceArticolo and VarianteArticolo.

        Returns a dict with selected, normalized fields or None on failure.
        """

        url = "https://dropzone.pac2000a.it/anagrafiche/ArticoliDecodifica_call.php"

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://dropzone.pac2000a.it",
            "Referer": "https://dropzone.pac2000a.it/anagrafiche/articoloDecodificaV2/",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/143.0.0.0 Safari/537.36"
            ),
        }

        payload = {
            "funzione": "decodifica",
            "CodiceBarre": "",
            "CodiceArticolo": cod,
            "VarianteArticolo": v,
            "IDCliente": 31659,
            "IDAzienda": 2,
            "decodificaAnagrafica": "S",
            "ricercaRepCommle_tipoCons": "S",
            "ricercaCodRappCommle": "S",
            "ricercaRapportiCommle": "S",
            "ricercaListinoCessione": "S",
            "intercettaCessione": "S",
            "ricercaListinoVendita": "S",
            "intercettaVendita": "S",
            "ricercaDisponibilita": "S",
            "Acquistato": "S",
            "Venduto": "S",
            "Offerta": "S",
            "ControlloMarchio": "S",
            "ControlloQtaOrdinata": "S",
            "FornitorePrevalente": "S",
            "isInfoArticolo": "S",
            "intercettaUltimaCessione": "S",
            "intercettaUltimaVendita": "S",
            "estrazioneMerceologiaECR": "S",
            "dataIntercettazione": self.dataIntercettaPrezzi,
            "dataDecorrenzaCosto": self.dataIntercettaPrezzi,
            "dataScadenzaCosto": self.dataIntercettaPrezzi,
        }
        # --- copy cookies from selenium ---
        session = requests.Session()
        for c in self.driver.get_cookies():
            session.cookies.set(c["name"], c["value"])

        try:
            response = session.post(url, headers=headers, data=payload, timeout=15)
            response.raise_for_status()
            data = response.json()

        except Exception as e:
            logger.error(
                f"Decodifica failed for {cod}.{v}: {e}"
            )
            return None

        if not isinstance(data, dict):
            logger.warning(
                f"Unexpected response format for {cod}.{v}"
            )
            return None
        
        description = data.get("Descrizione")
        package = data.get("Imballo")
        multiplier = data.get("RapportoCessioneVendita")
        availability = data.get("disponibilita2")
        cost = data.get("cessione")
        price = data.get("vendita")
        category = data.get("DexReparto")
        
        return {
            description,
            package,
            multiplier,
            availability,
            cost,
            price,
            category,
        }

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

def is_real_product(row: dict) -> bool:
    """
    Filters out category/separator rows like:
    arIDArticolo = "0"
    """
    try:
        return int(row.get("arIDArticolo", 0)) > 0
    except (TypeError, ValueError):
        return False