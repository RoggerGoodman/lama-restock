# LamApp/supermarkets/scripts/orderer.py
"""
Places an order on Dropzone over plain HTTP: the same three requests the order page
makes (create the header, look each product up, insert its row), reproduced field for
field. The order is left as a draft: "Invia" stays a manual step on Dropzone.
"""
import logging
from datetime import date, datetime

from .dropzone_client import DropzoneClient

logger = logging.getLogger(__name__)

# The order page's lookup, minus nothing: these flags decide which prices come back
LOOKUP_FLAGS = {
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
}
OPEN_ENDED = "9999-12-31"


def _page_date(value) -> str:
    """The order page's formattaData: YYYY-M-D without zero padding."""
    if not value:
        return ""
    if value == OPEN_ENDED:
        return OPEN_ENDED
    d = datetime.strptime(value, "%Y-%m-%d")
    return f"{d.year}-{d.month}-{d.day}"


class Orderer:

    def __init__(self, username: str, password: str) -> None:
        self.client = DropzoneClient(username, password)
        self.order_skipped_products = []
        self.order_id = None

    def login(self):
        self.client.login()

    def close(self):
        self.client.session.close()

    def make_orders(self, storage, order_list):
        """
        Create one draft order for `storage` holding every orderable item of
        order_list [(cod, var, qty, discount)].
        Returns (successful_orders, order_skipped_products).
        """
        supermarket = storage.supermarket
        id_cliente = supermarket.id_cliente
        id_azienda = supermarket.id_azienda
        if not id_cliente or not id_azienda:
            client = self.client.fetch_client()
            id_cliente, id_azienda = int(client["value"]), int(client["IDAzienda"])
        id_user = self.client.id_user

        today = date.today().isoformat()
        header = self.client.post("/ordini/elabora_ordine.php", {
            "action": "save",
            "IDOrdine": "",
            "NumeroOrdine": "",
            "DataOrdine": today,
            "DataSpedizione": today,
            "DataInvio": "",
            "IDCliente": id_cliente,
            "FaseOrdine": "0",
            "IDCodMag": storage.id_cod_mag,
            "idUtente": id_user,
        })
        self.order_id = header["IDOrdine"]
        logger.info(f"Draft order {self.order_id} created for {storage.name}")

        successful_orders = []
        for order_item in order_list:
            cod, var, qty, _discount = order_item
            article = self.client.post("/anagrafiche/ArticoliDecodifica_call.php", {
                "IDAzienda": id_azienda,
                "CodiceArticolo": cod,
                "VarianteArticolo": var,
                "IDCliente": id_cliente,
                **LOOKUP_FLAGS,
            }, timeout=30)

            if not isinstance(article, dict) or not article.get("IDArticolo"):
                self._skip(cod, var, qty, "Product not found in Dropzone")
                continue
            if (int(article["CodiceArticolo"]), int(article["VarianteArticolo"])) != (int(cod), int(var)):
                self._skip(cod, var, qty,
                           f"Dropzone resolved it to {article['CodiceArticolo']}.{article['VarianteArticolo']}")
                continue
            if article.get("saAccettaOrdini") != "true":
                logger.info(f"Article {cod}.{var} doesn't accept orders (disabled in ordering system)")
                self._skip(cod, var, qty, 'Product disabled in ordering system (cannot place order)')
                continue

            reply = self.client.post("/ordini/RigaVendita_call.php", {
                "funzione": "insert",
                "IDArticolo": article["IDArticolo"],
                "IDOrdine": self.order_id,
                "IDUnitaMisura": article["IDUnitaMisura"],
                "Quantita": qty,
                "Imballo": article["Imballo"],
                "IDListCessione": article["cessioneIDList"],
                "Prezzo": article["cessione"],
                "CessioneDataDa": _page_date(article["cessioneDataDa"]),
                "CessioneDataA": _page_date(article["cessioneDataA"]),
                "IDListVendita": article["venditaIDList"],
                "Vendita": article["vendita"],
                "VenditaDataDa": _page_date(article["venditaDataDa"]),
                "VenditaDataA": _page_date(article["venditaDataA"]),
                "Margine": article["returnCalcoloMargine"],
                "IDIva": article["IDIva"],
                "IDLeggeIva": article["IDLeggeIva"],
                "Corsia": "",
                "IDFornitore": article["IDFornitore"],
                "Valido": "S",
                "idUtente": id_user,
            }, timeout=30)
            if not str(reply).startswith("Ok"):
                self._skip(cod, var, qty, f"Dropzone refused the row: {reply}")
                continue
            successful_orders.append(order_item)

        logger.info(f"Order execution complete: {len(successful_orders)} successful, "
                    f"{len(self.order_skipped_products)} skipped during ordering")
        return successful_orders, self.order_skipped_products

    def _skip(self, cod, var, qty, reason):
        self.order_skipped_products.append({'cod': cod, 'var': var, 'qty': qty, 'reason': reason})
