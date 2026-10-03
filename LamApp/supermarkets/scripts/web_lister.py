# LamApp/supermarkets/scripts/web_lister.py
"""
Product list and product lookups from Dropzone, over plain HTTP.
"""
import re
import csv
from pathlib import Path
import logging
from datetime import date

from .dropzone_client import DropzoneClient, USER_AGENT

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
    "ivAliquota": "Iva",
}


class WebLister:
    """
    Downloads product list Excel files from Dropzone.
    Adapted for web app usage - no hardcoded values.
    """
    
    def __init__(self, username: str, password: str, storage_name: str,
                 download_dir: str = None, id_cod_mag: int = None,
                 id_cliente: int = None, id_azienda: int = None,
                 id_marchio: int = None, id_clienti_canale: int = None,
                 id_clienti_area: int = None, id_user: int = None,
                 x5cper: int = None):
        """
        Initialize the lister.

        Args:
            username: Dropzone username
            password: Dropzone password
            storage_name: Storage name (e.g., "01 RIANO GENERI VARI")
            download_dir: Directory for the downloaded CSV (only run() writes one)
            id_cod_mag: Warehouse code from Dropzone (stored on Storage model)
            id_cliente: Client ID from Dropzone (stored on Supermarket model)
            id_azienda: Company ID from Dropzone (stored on Supermarket model)
            id_marchio: Brand ID from Dropzone (stored on Supermarket model)
            id_clienti_canale: Channel ID from Dropzone (stored on Supermarket model)
            id_clienti_area: Area ID from Dropzone (stored on Supermarket model)
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
        self.id_user = id_user
        self.x5cper = x5cper

        self.dataIntercettaPrezzi = date.today().strftime("%Y-%m-%d")
        
        # Extract settore name (remove numeric prefix)
        self.settore = re.sub(r'^\d+\s+', '', storage_name)

        self.client = DropzoneClient(username, password)
        self.session = self.client.session

    def login(self):
        self.client.login()

    def close(self):
        self.session.close()

    def apply_category_filters(self):
        """Apply category filters based on storage type.
        IDCodMag is now set from the constructor (stored on Storage model).
        RepartoIn remains hardcoded per settore for now.

        Dropzone silently drops products when multiple Reparto codes are
        queried together in a single RepartoIn request. Querying one
        Reparto at a time returns the full set, so each settore is
        expressed as a list of single-Reparto groups to be fetched
        separately and merged (see fetch_all_listino).
        """
        self.output_path = Path(self.download_dir) / f"{self.storage_name}.csv"

        if self.IDCodMag is None:
            logger.error(f"IDCodMag not set for {self.settore}. "
                         "Storage may need re-sync from Dropzone.")
            raise ValueError(f"IDCodMag not configured for storage '{self.settore}'. "
                             "Please re-sync storages.")

        # RepartoIn still hardcoded per settore (to be made dynamic later)
        if "GENERI VARI" in self.settore:
            self.reparto_groups = [[28], [70], [44], [50], [52], [76]]
        elif "DEPERIBILI" in self.settore:
            self.reparto_groups = [[30], [34], [44]]
        elif "SURGELATI" in self.settore:
            self.reparto_groups = [[38]]
        else:
            logger.info(f"No predefined RepartoIn filters for {self.settore}, using empty list")
            self.reparto_groups = [[]]

    def fetch_all_listino(self) -> list:
        """
        Fetch listino products once per Reparto group in self.reparto_groups
        and merge the results, deduping by (CodiceArticolo, VarianteArticolo).
        """
        merged = []
        seen = set()

        for reparto_in in self.reparto_groups:
            rows = self.fetch_listino(reparto_in)
            logger.info(f"fetch_listino(RepartoIn={reparto_in}): {len(rows)} rows")
            for row in rows:
                key = (row.get("arCodiceArticolo"), row.get("arVarianteArticolo"))
                if key in seen:
                    continue
                seen.add(key)
                merged.append(row)

        logger.info(f"fetch_all_listino: {len(merged)} distinct products across {len(self.reparto_groups)} Reparto groups")
        return merged

    def fetch_listino(self, reparto_in: list = None):
        """
        Fetch listino products from Dropzone (Listino_callV2.php).
        Requires login().
        """
        if reparto_in is None:
            reparto_in = getattr(self, "RepartoIn", [])

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
            "RepartoIn": ",".join(map(str, reparto_in)),
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
            "User-Agent": USER_AGENT,
        }

        response = self.session.post(url, data=payload, headers=headers, timeout=600)
        response.raise_for_status()

        return response.json() or []

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
            self.apply_category_filters()
            self.data = self.fetch_all_listino()
            return self.save_listino_to_csv(self.data)
        finally:
            self.close()

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

        session = self.session

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
                    raw_ean = barcode_data[-1].get("CodiceBarre")
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
        )

    def gather_product_data_by_ean(self, ean):
        """
        Reverse lookup: given an EAN barcode, return (cod, var) from Dropzone.
        Calls ArticoliDecodifica_call.php with CodiceBarre instead of CodiceArticolo/VarianteArticolo.
        Returns (cod, var) tuple or None if not found.
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
            "CodiceBarre": str(ean),
            "CodiceArticolo": "",
            "VarianteArticolo": "",
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
        session = self.session
        try:
            response = session.post(url, headers=headers, data=payload, timeout=15)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"EAN reverse lookup failed for {ean}: {e}")
            return None

        if not isinstance(data, dict):
            return None

        cod = data.get("CodiceArticolo")
        var = data.get("VarianteArticolo")
        if cod and var:
            try:
                return (int(cod), int(var))
            except (ValueError, TypeError):
                return None
        return None


def download_product_list(username: str, password: str, storage_name: str,
                          download_dir: str, id_cod_mag: int = None,
                          id_cliente: int = None, id_azienda: int = None,
                          id_marchio: int = None, id_clienti_canale: int = None,
                          id_clienti_area: int = None) -> str:
    """
    Convenience function to download product list.

    Args:
        username: Dropzone username
        password: Dropzone password
        storage_name: Storage name (e.g., "01 RIANO GENERI VARI")
        download_dir: Directory to save downloaded files
        id_cod_mag: Warehouse code from Dropzone
        id_cliente: Client ID from Dropzone
        id_azienda: Company ID from Dropzone
        id_marchio: Brand ID from Dropzone
        id_clienti_canale: Channel ID from Dropzone
        id_clienti_area: Area ID from Dropzone

    Returns:
        str: Path to downloaded CSV file
    """
    lister = WebLister(username, password, storage_name, download_dir,
                       id_cod_mag=id_cod_mag, id_cliente=id_cliente,
                       id_azienda=id_azienda, id_marchio=id_marchio,
                       id_clienti_canale=id_clienti_canale,
                       id_clienti_area=id_clienti_area)
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