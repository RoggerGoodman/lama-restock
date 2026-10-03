# LamApp/supermarkets/scripts/dropzone_client.py
"""
Browser-free Dropzone client. Login is a plain form POST, and every page the
importers need is a JSON endpoint behind the session cookies it sets.
"""
import logging
import re

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://dropzone.pac2000a.it"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/143.0.0.0 Safari/537.36"
)
AJAX_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": BASE_URL + "/",
}


class DropzoneLoginError(Exception):
    pass


class DropzoneClient:

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self._id_user = None

    def post(self, path: str, data: dict, timeout: int = 60):
        response = self.session.post(BASE_URL + path, data=data, headers=AJAX_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def login(self):
        self.session.get(BASE_URL + "/", timeout=30)
        response = self.session.post(
            BASE_URL + "/ajax/accesso.php",
            data={"utente": self.username, "password": self.password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        response.raise_for_status()
        # The page answers "OK|..." on success, "PWDEXP" on an expired password
        reply = response.text.strip()
        if not reply.startswith("OK"):
            if "PWDEXP" in reply:
                raise DropzoneLoginError("Dropzone password expired")
            raise DropzoneLoginError("Dropzone rejected the credentials")
        logger.info("Dropzone login OK (HTTP)")

    @property
    def id_user(self) -> int:
        """The menu page embeds it as `const IDUtente = "3774"`."""
        if self._id_user is None:
            html = self.session.get(BASE_URL + "/menu/index.php", timeout=30).text
            match = re.search(r'IDUtente\s*=\s*"(\d+)"', html)
            if not match:
                raise ValueError("IDUtente not found in the Dropzone menu page")
            self._id_user = int(match.group(1))
        return self._id_user

    def fetch_x5cper(self) -> int:
        rows = self.post("/include/PersoneProxy.php", {"ragsoc": "", "app": "RIEPFATT"}, timeout=30)
        return int((rows[0] if isinstance(rows, list) else rows)["N1CPER"])

    def fetch_client(self) -> dict:
        rows = self.post("/anagrafiche/Cliente_call.php", {
            "funzione": "loadComboV2", "IDUser": self.id_user, "Chiamante": "gestioneOrdini",
        }, timeout=30)
        return rows[0]

    def gather_client_data(self) -> dict:
        """Every client-level ID the Supermarket model stores."""
        row = self.fetch_client()
        return {
            'id_cliente':        int(row["value"]),
            'id_azienda':        int(row["IDAzienda"]),
            'id_marchio':        int(row["IDMarchio"]),
            'id_clienti_canale': int(row["IDClientiCanale"]),
            'id_clienti_area':   int(row["IDClientiArea"]),
            'id_user':           self.id_user,
            'x5cper':            self.fetch_x5cper(),
        }

    def fetch_warehouses(self, id_cliente: int) -> list:
        """
        The client's delivery warehouses, e.g.
        {"id_cod_mag": 46, "code": "01", "name": "RIANO GENERI VARI", "rebilling": False}.
        `name` is the text the order page's warehouse dropdown shows (storage names come
        from it); `code` is what DDT and credit-note lines carry in UACMAG.
        """
        rows = self.post("/clienti/tabelle/ClientiMagazzinoConsegna_call.php", {
            "funzione": "lista", "IDCliente": id_cliente,
        }, timeout=30) or []
        return [{
            "id_cod_mag": int(r["cliMagIDCodMag"]),
            "code": r["magCodMag"].strip(),
            "name": r["magDescMag"],
            # "63|S": the rebilling pseudo-warehouse (third-party deliveries), not a real storage
            "rebilling": r.get("IDCodMagFornitoreFlag", "").endswith("|S"),
        } for r in rows]

    def fetch_document_headers(self, x5cper: int, date_from: str, date_to: str) -> list:
        """
        Accounting headers for documents dated date_from..date_to ("YYYYMMDD").
        One row per (document, reparto), so a document spans several rows.
        """
        rows = self.post("/fteweb/ScorporoAmministrativo_call.php", {
            "funzione":    "lista",
            "X5TREC":      "02",
            "IDAziendaIn": 0,
            "X5CPERIn":    x5cper,
            "X5CPEC":      "",
            "X5CNATIn":    "",
            "X5DDOCda":    date_from,
            "X5DDOCa":     date_to,
            "tipoDate":    1,
        }) or []
        logger.info(f"fetch_document_headers: {len(rows)} rows for {date_from}→{date_to}")
        return rows

    def fetch_document_lines(self, header: dict) -> list:
        """
        Article lines of one document. DDTs (Tipo "A") and credit notes (Tipo "D")
        live behind different endpoints, and the latter wants the date as dd/mm/yyyy.
        Drops the {"IsEspoPresent": ...} placeholder row both endpoints append.
        """
        if header["Tipo"] == "D":
            path = "/fteweb/fatturediff_righe_data.php"
            d = header["X5DRIF"]
            doc_date = f"{d[6:8]}/{d[4:6]}/{d[0:4]}"
        else:
            path = "/fteweb/fatture_righe_data.php"
            doc_date = header["X5DDOC"]

        rows = self.post(path, {
            "iduser":                       self.id_user,
            "uacazn":                       header["X5CAZN"],
            "uactda":                       header["X5CNFT"],
            "uanbaa":                       header["X5NBFA"],
            "uactag":                       header["X5CTAG"],
            "uanrct":                       header["X5NRCT"],
            "uanrcc":                       header["X5NRFC"],
            "uanrcd":                       header["X5NRCD"],
            "uanfaa":                       "",
            "uactdf":                       "",
            "uanrrt":                       "",
            "uanrrc":                       "",
            "uanrrd":                       "",
            "ubdgen":                       doc_date,
            "rifatturazioneValorizzazione": "",
            "type":                         "view",
        }) or []
        return [r for r in rows if "UACART" in r]
