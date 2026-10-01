"""
Apply a promo PDF to EVERY real supermarket, like the per-store "upload promo".

Standard stores get every row; Rione stores get only the rows tagged RIONE.

    python manage.py upload_promos_all "/path/PROMO GENERICA N°14-2026.pdf"
    python manage.py upload_promos_all promo.pdf --dry-run
"""
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from supermarkets.demo import real_supermarkets
from supermarkets.scripts.DatabaseManager import DatabaseManager
from supermarkets.scripts.helpers import Helper


class Command(BaseCommand):
    help = "Apply a promo PDF to every real supermarket (Rione stores: RIONE rows only)."

    def add_arguments(self, parser):
        parser.add_argument("pdf", help="Path to the promo PDF")
        parser.add_argument("--dry-run", action="store_true", help="Parse and report only, write nothing")

    def handle(self, *args, **opts):
        pdf = Path(opts["pdf"])
        if not pdf.is_file():
            raise CommandError(f"File not found: {pdf}")

        promo_list = Helper.parse_promo_pdf(str(pdf))
        if not promo_list:
            raise CommandError("No promo rows found in the PDF.")

        rione_count = sum(1 for row in promo_list if row[6])
        sale_start, sale_end = promo_list[0][4], promo_list[0][5]
        self.stdout.write(
            f"Parsed {len(promo_list)} rows ({rione_count} RIONE), "
            f"public sale {sale_start} -> {sale_end}"
            f"{' [DRY RUN]' if opts['dry_run'] else ''}\n"
        )
        if rione_count == 0:
            self.stdout.write(self.style.WARNING(
                "No RIONE rows found: Rione stores will receive nothing from this file."
            ))

        ok = failed = 0
        for sm in real_supermarkets().order_by("name"):
            store_list = Helper.promos_for_store(promo_list, sm.is_rione)
            label = f"{sm.name} ({sm.get_store_type_display()})"
            if opts["dry_run"]:
                self.stdout.write(f"  {label}: {len(store_list)} rows")
                continue
            try:
                db = DatabaseManager(supermarket_name=sm.name)
                try:
                    matched = db.update_promos(store_list)
                finally:
                    db.close()
                ok += 1
                self.stdout.write(self.style.SUCCESS(
                    f"  {label}: {matched}/{len(store_list)} rows matched a product"
                ))
            except Exception as e:
                failed += 1
                self.stdout.write(self.style.ERROR(f"  {label}: FAILED - {e}"))

        if not opts["dry_run"]:
            self.stdout.write(f"\nDone: {ok} supermarket(s) updated, {failed} failed.")
