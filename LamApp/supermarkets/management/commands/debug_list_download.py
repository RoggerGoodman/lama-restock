"""
Diagnostic: download a storage's product list WITHOUT importing, and trace
what happens to a specific cod/var through every stage of the pipeline.

Read-only for the DB. Reuses the real WebLister (same creds/filters as the
nightly run), so it reproduces exactly what the automated update would fetch.

    python manage.py debug_list_download --storage-id 46 --cod 26566 --var 1
"""
import shutil
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from supermarkets.models import Storage
from supermarkets.scripts.web_lister import WebLister, is_real_product


class Command(BaseCommand):
    help = "Download a storage's list without importing; trace a cod/var."

    def add_arguments(self, parser):
        parser.add_argument("--storage-id", type=int)
        parser.add_argument("--storage-name", type=str)
        parser.add_argument("--cod", type=int, required=True)
        parser.add_argument("--var", type=int, default=0)

    def handle(self, *args, **opts):
        if opts["storage_id"]:
            storage = Storage.objects.get(id=opts["storage_id"])
        elif opts["storage_name"]:
            storage = Storage.objects.get(name=opts["storage_name"])
        else:
            raise CommandError("Provide --storage-id or --storage-name")

        sm = storage.supermarket
        target = (opts["cod"], opts["var"])
        self.stdout.write(
            f"Storage: id={storage.id} name={storage.name!r} settore={storage.settore!r}"
        )
        self.stdout.write(f"Target cod/var: {target}\n")

        download_dir = Path(settings.BASE_DIR) / "temp_lists"
        download_dir.mkdir(exist_ok=True)

        lister = WebLister(
            username=sm.username,
            password=sm.password,
            storage_name=storage.name,
            download_dir=str(download_dir),
            id_cod_mag=storage.id_cod_mag,
            id_cliente=sm.id_cliente,
            id_azienda=sm.id_azienda,
            id_marchio=sm.id_marchio,
            id_clienti_canale=sm.id_clienti_canale,
            id_clienti_area=sm.id_clienti_area,
            headless=True,
        )

        try:
            lister.login()
            lister.navigate_to_lists()
            lister.apply_category_filters()
            self.stdout.write(f"reparto_groups: {lister.reparto_groups}\n")

            found_in = []
            merged = []
            seen = set()
            for group in lister.reparto_groups:
                rows = lister.fetch_listino(group)
                self.stdout.write(f"RepartoIn={group}: {len(rows)} rows")
                for row in rows:
                    try:
                        key = (
                            int(row.get("arCodiceArticolo")),
                            int(row.get("arVarianteArticolo")),
                        )
                    except (TypeError, ValueError):
                        key = (row.get("arCodiceArticolo"), row.get("arVarianteArticolo"))
                    if key == target:
                        found_in.append((group, row))
                    if key in seen:
                        continue
                    seen.add(key)
                    merged.append(row)

            self.stdout.write(f"\n=== target {target} ===")
            if not found_in:
                self.stdout.write(
                    "ABSENT from ALL reparto fetches -> the absent-list sweep "
                    "forces disponibilita='No' (and it can never recover while absent)."
                )
            else:
                for group, row in found_in:
                    keeps = is_real_product(row)
                    self.stdout.write(
                        f"FOUND in RepartoIn={group}: "
                        f"disponibilita2={row.get('disponibilita2')!r} "
                        f"arIDArticolo={row.get('arIDArticolo')!r} "
                        f"is_real_product={keeps} "
                        f"desc={row.get('arDescrizione')!r}"
                    )
                    if not keeps:
                        self.stdout.write(
                            "  -> DROPPED by is_real_product (arIDArticolo<=0): "
                            "absent from CSV -> swept to 'No'."
                        )

            lister.data = merged
            path = lister.save_listino_to_csv(lister.data)
            self.stdout.write(f"\nCSV saved (NOT imported): {path}")

        finally:
            lister.driver.quit()
            shutil.rmtree(lister.user_data_dir, ignore_errors=True)
