"""
Run the nightly product link pass by hand (see chain_links.py):
cleanup of quiet links, chain links applied, subentranti verified,
unused chain links retired.

    python manage.py sync_product_links --dry-run
    python manage.py sync_product_links
"""
from django.core.management.base import BaseCommand

from supermarkets.chain_links import fmt_pair, sync_all


class Command(BaseCommand):
    help = "Sync every store's product links with the chain list."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Report only, write nothing")

    def handle(self, *args, **opts):
        dry_run = opts["dry_run"]
        reports, retired = sync_all(dry_run=dry_run)

        for report in reports:
            lines = report.lines()
            style = self.style.ERROR if report.error else (lambda s: s)
            self.stdout.write(style(f"{report.supermarket.name}: {len(lines) or 'no'} change(s)"))
            for line in lines:
                self.stdout.write(f"  {line}")

        for cl in retired:
            self.stdout.write(f"retire chain link {fmt_pair(((cl.primary_cod, cl.primary_v), (cl.secondary_cod, cl.secondary_v)))}")

        verb = "Would remove" if dry_run else "Removed"
        self.stdout.write(
            f"\n{verb} {sum(len(r.removed) for r in reports)}, "
            f"add {sum(len(r.added) for r in reports)}, "
            f"verify {sum(len(r.verified) for r in reports)}, "
            f"purge {sum(len(r.purged) for r in reports)}, "
            f"retire {len(retired)} chain link(s); "
            f"{sum(1 for r in reports if r.error)} store(s) failed."
            f"{' [DRY RUN]' if dry_run else ''}"
        )
