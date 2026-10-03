# LamApp/supermarkets/scripts/inventory_scrapper.py
import os
from django.conf import settings
import logging
import csv
from datetime import date, timedelta

from .dropzone_client import DropzoneClient

logger = logging.getLogger(__name__)

# Save path for loss files (ROTTURE, SCADUTO, UTILIZZO INTERNO)
save_path = str(settings.LOSSES_FOLDER)

CSV_COLUMN_MAP = {
    "RilevazioniRigheCodiceBarre": "EAN",
    "RilevazioniRigheDescrizione": "Description",
    "RilevazioniRigheQuantitaOriginale": "Quantity"
}

class Inventory_Scrapper:

    def __init__(self, supermarket, username: str, password: str) -> None:
        self.supermarket = supermarket
        self.id_cliente = self.supermarket.id_cliente
        self.client = DropzoneClient(username, password)

    def login(self):
        self.client.login()

    def close(self):
        self.client.session.close()

    def export_all_testate_from_day(self, max_days_back: int = 30):
        """
        Exports new testate (ROTTURE, SCADUTO, UTILIZZO INTERNO) not yet downloaded.
        Walks backwards day by day; stops for each type once the rilevazione date
        reaches or passes the last date already synced (stored in LossSyncState).
        """
        from ..models import LossSyncState

        session = self.client.session

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://dropzone.pac2000a.it",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0",
        }

        url_testate = "https://dropzone.pac2000a.it/rilevazioni/RilevazioniTestate_call.php"
        url_righe = "https://dropzone.pac2000a.it/rilevazioni/RilevazioniRighe_call.php"
        ALLOWED_TYPES = {"ROTTURE", "SCADUTO", "UTILIZZO INTERNO"}

        sync_state, _ = LossSyncState.objects.get_or_create(supermarket=self.supermarket)
        last_dates = {
            "ROTTURE": sync_state.last_date_rotture,
            "SCADUTO": sync_state.last_date_scaduto,
            "UTILIZZO INTERNO": sync_state.last_date_utilizzo_interno,
        }
        logger.info(f"Starting export — last_dates: {last_dates}")

        today = date.today()
        grouped = {}          # desc -> [testate dicts]
        first_date_found = {} # desc -> most recent rilevazione date with new entries

        for days_back in range(max_days_back + 1):
            target_date_obj = today - timedelta(days=days_back)

            remaining = {
                desc for desc in ALLOWED_TYPES
                if last_dates.get(desc) is None or target_date_obj > last_dates[desc]
            }
            if not remaining:
                break

            target_date = target_date_obj.strftime("%Y-%m-%d")

            payload_testate = {
                "funzione": "lista",
                "IDAzienda": "",
                "IDCliente": self.id_cliente,
                "DescRilevazione": "",
                "Dal": target_date,
                "Al": target_date,
                "IsExported": "",
                "numRecord": 100,
            }

            resp = session.post(url_testate, headers=headers, data=payload_testate)
            resp.raise_for_status()
            testate = resp.json()

            for t in testate:
                desc = t["RilevazioniTestateDescRilevazione"].strip()
                if desc not in remaining:
                    continue
                grouped.setdefault(desc, []).append(t)
                if desc not in first_date_found:
                    first_date_found[desc] = target_date_obj

        if not grouped:
            logger.info("No new testate found.")
            return

        os.makedirs(save_path, exist_ok=True)
        csv_headers = list(CSV_COLUMN_MAP.values())

        for desc, items in grouped.items():
            csv_path = os.path.join(save_path, f"{desc}.csv")

            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=csv_headers)
                writer.writeheader()

                for t in items:
                    id_testata = t["RilevazioniTestateIDRilevazioniTestata"]
                    num_righe = t.get("numRighe", "0")
                    logger.info(f"Exporting {desc} | ID {id_testata} | Rows {num_righe}")

                    payload_righe = {
                        "funzione": "lista",
                        "IDRilevazioniTestata": id_testata,
                    }

                    resp = session.post(url_righe, headers=headers, data=payload_righe)
                    resp.raise_for_status()
                    righe = resp.json()

                    if not righe:
                        logger.info(f"No rows for {desc} ({id_testata})")
                        continue

                    for r in righe:
                        writer.writerow({dst: r.get(src) for src, dst in CSV_COLUMN_MAP.items()})

            logger.info(f"Saved {csv_path}")

            if desc == "ROTTURE":
                sync_state.last_date_rotture = first_date_found[desc]
            elif desc == "SCADUTO":
                sync_state.last_date_scaduto = first_date_found[desc]
            elif desc == "UTILIZZO INTERNO":
                sync_state.last_date_utilizzo_interno = first_date_found[desc]

        sync_state.save()
        logger.info("All available testate exported.")