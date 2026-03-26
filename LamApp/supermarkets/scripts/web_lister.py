# LamApp/supermarkets/scripts/web_lister.py
"""
Web-integrated version of the Lister script.
Downloads product list Excel files from PAC2000A automatically.
"""
from .DatabaseManager import DatabaseManager
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
                 download_dir: str, id_cod_mag: int = None,
                 id_cliente: int = None, id_azienda: int = None,
                 id_marchio: int = None, id_clienti_canale: int = None,
                 id_clienti_area: int = None, headless: bool = True):
        """
        Initialize the lister.

        Args:
            username: PAC2000A username
            password: PAC2000A password
            storage_name: Storage name (e.g., "01 RIANO GENERI VARI")
            download_dir: Directory to save downloaded files
            id_cod_mag: Warehouse code from PAC2000A (stored on Storage model)
            id_cliente: Client ID from PAC2000A (stored on Supermarket model)
            id_azienda: Company ID from PAC2000A (stored on Supermarket model)
            id_marchio: Brand ID from PAC2000A (stored on Supermarket model)
            id_clienti_canale: Channel ID from PAC2000A (stored on Supermarket model)
            id_clienti_area: Area ID from PAC2000A (stored on Supermarket model)
            headless: Run browser in headless mode (no UI)
        """
        self.username = username
        self.password = password
        self.storage_name = storage_name
        self.download_dir = download_dir
        self.IDCodMag = id_cod_mag
        self.StatoAssIn=[16, 13] #TODO must be made user selectable (there are more than just these 2 options... sadly)
        self.IDCliente = id_cliente
        self.IDAzienda = id_azienda
        self.IDMarchio = id_marchio
        self.IDClientiCanale = id_clienti_canale
        self.IDClientiArea = id_clienti_area

        self.dataIntercettaPrezzi = date.today().strftime("%Y-%m-%d")
        
        # Extract settore name (remove numeric prefix)
        self.settore = re.sub(r'^\d+\s+', '', storage_name)

        # ✅ FIX: Use unique temp directory for THIS instance
        self.user_data_dir = f"/tmp/chrome-{uuid.uuid4()}"
        os.makedirs(self.user_data_dir, exist_ok=True)
        os.chmod(self.user_data_dir, 0o700)

        # ✅ FIX: Set environment variables for THIS process
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
            chrome_options.add_argument("--disable-extensions")
        
        # Set download directory
        prefs = {
            "download.default_directory": str(Path(download_dir).absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # Use unique user data directory
        chrome_options.add_argument(f"--user-data-dir={self.user_data_dir}")

        # ✅ FIX: Use unique log file per instance (or disable logging)
        log_path = f"/tmp/chromedriver-{uuid.uuid4().hex[:8]}.log"
        
        service = Service(
            "/usr/bin/chromedriver",
            log_path=log_path  # ✅ Now unique per instance!
        )
        
        try:
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.actions = ActionChains(self.driver)
            self.wait = WebDriverWait(self.driver, 300)
            
            logger.info(f"WebLister initialized for storage: {storage_name}")
        
        except Exception as e:
            logger.exception(f"Failed to initialize WebDriver: {e}")
            # Clean up on failure
            shutil.rmtree(self.user_data_dir, ignore_errors=True)
            raise

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
        time.sleep(1)
        logger.info("Login completed")

    def navigate_to_invoices(self):
        self.wait.until(EC.presence_of_element_located((By.ID, "carta2")))
        
        documents_menu = self.driver.find_element(By.ID, "carta2")
        documents_menu.click()

        self.wait.until(EC.presence_of_element_located((By.ID, "carta60")))
        
        invoices_menu = self.driver.find_element(By.ID, "cart60")
        invoices_menu.click()

        self.wait.until(lambda driver: len(driver.window_handles) > 1)
        
        self.driver.switch_to.window(self.driver.window_handles[-1])
    
    def navigate_to_lists(self):
        
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
        logger.info("Navigation to List completed")

    def gather_client_data(self):
        """
        Gather all client-specific parameters from PAC2000A APIs.
        Must be called after login(). Navigates to the lists section to
        intercept IDUser, then fetches client params and x5cper via API calls.
        Returns a dict with all fields needed to populate the Supermarket model.
        """
        from urllib.parse import parse_qs

        # Step 1: Navigate to lists section in a new tab, intercept IDUser via CDP
        self.driver.find_element(By.ID, "carta31").click()
        self.wait.until(EC.presence_of_element_located((By.ID, "carta139")))
        self.driver.find_element(By.ID, "carta139").click()
        self.wait.until(lambda driver: len(driver.window_handles) > 1)
        self.driver.switch_to.window(self.driver.window_handles[-1])

        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": """
            window.__clientePayload = null;
            (function() {
                const origSend = XMLHttpRequest.prototype.send;
                XMLHttpRequest.prototype.send = function(body) {
                    if (this._url && this._url.includes('Cliente_call.php')) {
                        window.__clientePayload = body;
                    }
                    return origSend.apply(this, arguments);
                };
                const origOpen = XMLHttpRequest.prototype.open;
                XMLHttpRequest.prototype.open = function(method, url) {
                    this._url = url;
                    return origOpen.apply(this, arguments);
                };
            })();
        """})
        self.driver.refresh()
        self.wait.until(EC.presence_of_element_located((By.ID, "linkListino")))
        time.sleep(2)

        raw = self.driver.execute_script("return window.__clientePayload;")
        if not raw:
            raise ValueError("Failed to intercept Cliente_call.php — IDUser not captured")
        parsed = parse_qs(raw)
        id_user = int(parsed["IDUser"][0])
        logger.info(f"Captured IDUser: {id_user}")

        # Step 2: Fetch client params from Cliente_call.php
        url_cliente = "https://dropzone.pac2000a.it/anagrafiche/Cliente_call.php"
        payload_cliente = {"funzione": "loadComboV2", "IDUser": id_user, "Chiamante": "gestioneOrdini"}
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://dropzone.pac2000a.it/",
            "User-Agent": self.driver.execute_script("return navigator.userAgent;"),
        }
        session = requests.Session()
        for c in self.driver.get_cookies():
            session.cookies.set(c["name"], c["value"])

        row = session.post(url_cliente, data=payload_cliente, headers=headers, timeout=30).json()[0]

        # Step 3: Fetch x5cper from PersoneProxy.php
        persona_row = session.post(
            "https://dropzone.pac2000a.it/include/PersoneProxy.php",
            data={"ragsoc": "", "app": "RIEPFATT"},
            headers=headers, timeout=30
        ).json()
        x5cper = int((persona_row[0] if isinstance(persona_row, list) else persona_row)["N1CPER"])

        client_data = {
            'id_cliente':        int(row["value"]),
            'id_azienda':        int(row["IDAzienda"]),
            'id_marchio':        int(row["IDMarchio"]),
            'id_clienti_canale': int(row["IDClientiCanale"]),
            'id_clienti_area':   int(row["IDClientiArea"]),
            'id_user':           id_user,
            'x5cper':            x5cper,
        }

        logger.info(f"Gathered client data: {client_data}")
        return client_data

    def close_ordini_popup(self):
        try:
            popup_title = self.driver.find_element(By.XPATH, "//h4[normalize-space()='Elenco Ordini In Corso']")
            
            chiudi_btn = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[2]/div[8]/div/div/div/div[3]/div/smart-button"))
            )
            chiudi_btn.click()
            print("Popup 'Elenco Ordini In Corso' found and closed.")

        except NoSuchElementException:
            pass
        except TimeoutException:
            print("Popup detected but Chiudi button not clickable.")

    def apply_category_filters(self):
        """Apply category filters based on storage type.
        IDCodMag is now set from the constructor (stored on Storage model).
        RepartoIn remains hardcoded per settore for now.
        """
        self.output_path = Path(self.download_dir) / f"{self.storage_name}.csv"

        if self.IDCodMag is None:
            logger.error(f"IDCodMag not set for {self.settore}. "
                         "Storage may need re-sync from PAC2000A.")
            raise ValueError(f"IDCodMag not configured for storage '{self.settore}'. "
                             "Please re-sync storages.")

        # RepartoIn still hardcoded per settore (to be made dynamic later)
        if "GENERI VARI" in self.settore:
            self.RepartoIn = [28, 44, 76, 50, 52]
        elif "DEPERIBILI" in self.settore:
            self.RepartoIn = [28, 30, 34, 44]
        elif "SURGELATI" in self.settore:
            self.RepartoIn = [38]
        else:
            logger.info(f"No predefined RepartoIn filters for {self.settore}, using empty list")
            self.RepartoIn = []

    def fetch_listino(self):
        """
        Fetch listino products from PAC2000A (Listino_callV2.php)
        Requires an authenticated Selenium driver.
        """

        url = "https://dropzone.pac2000a.it/anagrafiche/Listino_callV2.php"

        payload = {
            "funzione": "lista",
            "IDAzienda": self.IDAzienda,
            "IDCliente": self.IDCliente,
            "IDMarchio": self.IDMarchio,
            "IDCodMag": self.IDCodMag,
            "dexArt": "",
            "codiceBarre": "",
            "Livello1": "",
            "Livello2": "",
            "Livello3": "",
            "Livello4": "",
            "StatoAssIn": ",".join(map(str, self.StatoAssIn)),
            "numRecord": 5000,
            "IDOrdine": "",
            "Riclassificatore2In": "",
            "articoliMarchio": "",
            "itemStagionalita": "",
            "posizioneDomandaIn": "",
            "dataIntercettaPrezzi": self.dataIntercettaPrezzi,
            "RepartoIn": ",".join(map(str, self.RepartoIn)),
            "dayInterval": 3,
            "IDClientiCanale": self.IDClientiCanale,
            "IDClientiArea": self.IDClientiArea,
            "IDFornitore": "",
            "separaLivelloMerceologia": "S",
            "AreePreparazioneIN": "",
            "IDArticolo": "",
            "isAcqUltimaSettimana": 0,
            "codiceEtichettaVisualizza": "",
        }

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://dropzone.pac2000a.it/ordini/gestione/listino",
            "User-Agent": self.driver.execute_script("return navigator.userAgent;"),
        }

        session = requests.Session()
        for c in self.driver.get_cookies():
            session.cookies.set(c["name"], c["value"])

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
            writer.writerow(csv_headers)
            for row in products:
                writer.writerow([row.get(field, "") for field in source_fields])

        return self.output_path        
    
    def run(self) -> str:
        """
        Execute the complete download workflow.
        
        Returns:
            str: Path to downloaded CSV file
        """
        try:
            self.login()
            self.navigate_to_lists()
            self.apply_category_filters()
            self.data = self.fetch_listino()
            file_path = self.save_listino_to_csv(self.data)
            return file_path
        finally:
            # ✅ Always clean up
            self.driver.quit()
            shutil.rmtree(self.user_data_dir, ignore_errors=True)

    def gather_missing_product_data(self, cod, var):
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
            "VarianteArticolo": var,
            "IDCliente": self.IDCliente,
            "IDAzienda": self.IDAzienda,
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

        session = requests.Session()
        for c in self.driver.get_cookies():
            session.cookies.set(c["name"], c["value"])

        try:
            response = session.post(url, headers=headers, data=payload, timeout=15)
            response.raise_for_status()
            data = response.json()

        except Exception as e:
            logger.error(f"Decodifica failed for {cod}.{var}: {e}")
            return None

        if not isinstance(data, dict):
            logger.warning(f"Unexpected response format for {cod}.{var}")
            return None

        description = data.get("Descrizione")
        package = data.get("Imballo")
        multiplier = data.get("RapportoCessioneVendita")
        availability = data.get("disponibilita2")
        cost = data.get("cessione")
        price = data.get("vendita")
        category = data.get("DexReparto")

        # Fetch EAN barcode from CodiciBarreProxyAbs_call.php
        ean = None
        id_articolo = data.get("IDArticolo")
        if id_articolo:
            try:
                barcode_url = "https://dropzone.pac2000a.it/articoli/codiciBarre/CodiciBarreProxyAbs_call.php"
                barcode_payload = {
                    "funzione": "lista",
                    "IDArticolo": id_articolo,
                    "IDAzienda": self.IDAzienda,
                    "Limit": 999,
                    "AbilitatoVendita": 1,
                }
                barcode_response = session.post(barcode_url, headers=headers, data=barcode_payload, timeout=15)
                barcode_response.raise_for_status()
                barcode_data = barcode_response.json()
                if isinstance(barcode_data, list) and barcode_data:
                    raw_ean = barcode_data[0].get("CodiceBarre")
                    if raw_ean:
                        ean = int(raw_ean)
            except Exception as e:
                logger.warning(f"EAN fetch failed for {cod}.{var}: {e}")

        return (
            description,
            package,
            multiplier,
            availability,
            cost,
            price,
            category,
            ean,
            id_articolo,
        )

    # ── Stats API (replaces Selenium scrapper) ──────────────────────────────

    def fetch_venduto_mensile(self, id_articolo: int) -> list | None:
        """
        Fetch monthly sales stats from VendutoPvMensile_call.php.
        Returns list of 12 dicts (GEN..DIC, no TOT row) with integer values
        (future months have "" converted to 0). Returns None on failure.
        """
        url = "https://dropzone.pac2000a.it/statistiche/mensili/VendutoPvMensile_call.php"
        payload = {
            "funzione": "statMensile",
            "IDCliente": self.IDCliente,
            "IDArticolo": id_articolo,
        }
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://dropzone.pac2000a.it/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/143.0.0.0 Safari/537.36"
            ),
        }
        session = requests.Session()
        for c in self.driver.get_cookies():
            session.cookies.set(c["name"], c["value"])

        try:
            response = session.post(url, data=payload, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"fetch_venduto_mensile failed for IDArticolo={id_articolo}: {e}")
            return None

        if not isinstance(data, dict) or "TOTALI" not in data:
            logger.warning(f"Unexpected response for IDArticolo={id_articolo}: {data}")
            return None

        def _to_int(val):
            if val == "" or val is None:
                return 0
            return int(val)

        result = []
        for row in data["TOTALI"]:
            if row.get("Mese") == "TOT":
                continue
            result.append({
                "Mese": row["Mese"],
                "QtaAnnoCur":    _to_int(row.get("QtaAnnoCur")),
                "QtaAnnoCurAcq": _to_int(row.get("QtaAnnoCurAcq")),
                "QtaAnnoPrec":   _to_int(row.get("QtaAnnoPrec")),
                "QtaAnnoPrecAcq": _to_int(row.get("QtaAnnoPrecAcq")),
            })
        return result

    def _build_stats_arrays(self, monthly_data: list) -> tuple:
        """
        Convert fetch_venduto_mensile() result into (sold_array, bought_array).
        Output: most-recent-month first, current year then last year, trailing zeros stripped.
        Mirrors the old Selenium scrapper + helper.prepare_array() logic exactly.
        """
        MONTH_ORDER = ["GEN", "FEB", "MAR", "APR", "MAG", "GIU",
                       "LUG", "AGO", "SET", "OTT", "NOV", "DIC"]

        by_month = {row["Mese"]: row for row in monthly_data}

        cur_sold    = [by_month.get(m, {}).get("QtaAnnoCur",     0) for m in MONTH_ORDER]
        cur_bought  = [by_month.get(m, {}).get("QtaAnnoCurAcq",  0) for m in MONTH_ORDER]
        prec_sold   = [by_month.get(m, {}).get("QtaAnnoPrec",    0) for m in MONTH_ORDER]
        prec_bought = [by_month.get(m, {}).get("QtaAnnoPrecAcq", 0) for m in MONTH_ORDER]

        # Most-recent month first, then concatenate current + last year
        cur_sold.reverse();   cur_bought.reverse()
        prec_sold.reverse();  prec_bought.reverse()

        final_sold   = cur_sold   + prec_sold
        final_bought = cur_bought + prec_bought

        # Discard future months from the front (12 - current_month leading zeros)
        months_to_discard = 12 - date.today().month
        i = 0
        while len(final_bought) > 1 and i < months_to_discard:
            final_sold.pop(0)
            final_bought.pop(0)
            i += 1

        # Strip trailing zeros from both simultaneously
        while final_bought and final_bought[-1] == 0 and final_sold[-1] == 0:
            final_bought.pop()
            final_sold.pop()

        return final_sold or [0], final_bought or [0]

    def process_products_stats(self, product_tuples, db:DatabaseManager) -> dict:
        """
        Fetch and store monthly stats for a list of products via API.
        Replaces Scrapper.process_products().

        product_tuples: iterable of (cod, v, is_verified, package_size, id_articolo)
        db: DatabaseManager instance
        Returns: same report dict structure as Scrapper.process_products()
        """
        cur = db.cursor()
        report = {
            "total": len(product_tuples),
            "processed": 0,
            "initialized": 0,
            "updated": 0,
            "skipped_no_id": 0,
            "empty": 0,
            "errors": 0,
            "newly_added": [],
        }

        for cod, v, is_verified, package_size, id_articolo in product_tuples:
            report["processed"] += 1

            if not id_articolo:
                logger.warning(f"[STATS] No id_articolo for {cod}.{v}, skipping")
                report["skipped_no_id"] += 1
                continue

            try:
                monthly_data = self.fetch_venduto_mensile(id_articolo)
                time.sleep(0.2)
                if monthly_data is None:
                    report["errors"] += 1
                    continue

                # Detect newly added: bought this year, never sold, nothing bought last year
                cur_bought  = [row["QtaAnnoCurAcq"]  for row in monthly_data]
                cur_sold    = [row["QtaAnnoCur"]      for row in monthly_data]
                prec_bought = [row["QtaAnnoPrecAcq"]  for row in monthly_data]

                if (any(q > 0 for q in cur_bought) and
                        all(q == 0 for q in cur_sold) and
                        all(q == 0 for q in prec_bought)):
                    logger.info(f"Newly added product detected: {cod}.{v}")
                    if not is_verified:
                        report["newly_added"].append({
                            "cod": cod,
                            "var": v,
                            "package_size": package_size,
                            "reason": "Product ordered but not yet sold (needs verification)",
                        })

                final_sold, final_bought = self._build_stats_arrays(monthly_data)

                if final_sold == [0] and final_bought == [0]:
                    report["empty"] += 1

                cur.execute("SELECT 1 FROM product_stats WHERE cod=%s AND v=%s", (cod, v))
                exists = cur.fetchone() is not None

                if not exists:
                    db.init_product_stats(cod, v, sold=final_sold, bought=final_bought, stock=0, verified=False)
                    report["initialized"] += 1
                else:
                    db.update_product_stats(cod, v, sold_update=final_sold[:2], bought_update=final_bought[:2])
                    report["updated"] += 1

            except Exception:
                logger.exception(f"[STATS] Error processing {cod}.{v}")
                report["errors"] += 1

        logger.info(report)
        logger.info(f"Found {len(report['newly_added'])} newly added products needing verification")
        return report

    def init_stats_for_settore(self, settore: str, db:DatabaseManager, full: bool = True) -> dict:
        """
        Fetch and store stats for all products in a settore via API.
        Replaces Scrapper.init_product_stats_for_settore().
        """
        cur = db.cursor()
        if full:
            cur.execute("""
                SELECT p.cod, p.v, ps.verified, p.pz_x_collo, p.id_articolo
                FROM products p
                LEFT JOIN product_stats ps ON p.cod = ps.cod AND p.v = ps.v
                WHERE p.settore = %s
            """, (settore,))
        else:
            cur.execute("""
                SELECT ps.cod, ps.v, ps.verified, p.pz_x_collo, p.id_articolo
                FROM product_stats ps
                JOIN products p ON ps.cod = p.cod AND ps.v = p.v
                WHERE p.settore = %s AND ps.verified = TRUE
            """, (settore,))

        rows = cur.fetchall()
        product_tuples = [
            (row["cod"], row["v"], row.get("verified", False),
             row.get("pz_x_collo", 12), row.get("id_articolo"))
            for row in rows
        ]
        return self.process_products_stats(product_tuples, db)


def download_product_list(username: str, password: str, storage_name: str,
                          download_dir: str, id_cod_mag: int = None,
                          id_cliente: int = None, id_azienda: int = None,
                          id_marchio: int = None, id_clienti_canale: int = None,
                          id_clienti_area: int = None,
                          headless: bool = True) -> str:
    """
    Convenience function to download product list.

    Args:
        username: PAC2000A username
        password: PAC2000A password
        storage_name: Storage name (e.g., "01 RIANO GENERI VARI")
        download_dir: Directory to save downloaded files
        id_cod_mag: Warehouse code from PAC2000A
        id_cliente: Client ID from PAC2000A
        id_azienda: Company ID from PAC2000A
        id_marchio: Brand ID from PAC2000A
        id_clienti_canale: Channel ID from PAC2000A
        id_clienti_area: Area ID from PAC2000A
        headless: Run browser in headless mode

    Returns:
        str: Path to downloaded CSV file
    """
    lister = WebLister(username, password, storage_name, download_dir,
                       id_cod_mag=id_cod_mag, id_cliente=id_cliente,
                       id_azienda=id_azienda, id_marchio=id_marchio,
                       id_clienti_canale=id_clienti_canale,
                       id_clienti_area=id_clienti_area, headless=headless)
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