"""
Add chain-wide substitutions and apply them to every real store now.

Each pair is SUBENTRANTE SOSTITUITO (cod.v): the subentrante is ordered, the
sostituito is phased out. Pairs are saved as ChainProductLinks, so the nightly
sync_chain_product_links task keeps applying them to stores that start
carrying the sostituito later (new clients included).

    python manage.py link_products 12345.1:67890.1 --notes "Email PAC 02/10" --author ruggero
    python manage.py link_products --file pairs.txt --dry-run
    python manage.py link_products --from-store "Todis Gubbio" --dry-run
    python manage.py link_products 12345.1:67890.1 --purge-old

File format: one pair per line, separated by space, comma, semicolon, colon
or '>'. Blank lines and '#' comments are ignored.

--from-store copies that store's current links into the chain list.

--purge-old: when the nightly cleanup removes the link from a store, the
sostituito is purged there too. Default off; stores can switch it per link.

A store gets a pair only if it carries the sostituito (in stock or sold in the
last 60 days) and neither product is already linked there.
"""
import re

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q

from supermarkets.chain_links import fmt, fmt_pair, sync_store
from supermarkets.demo import real_supermarkets
from supermarkets.models import ChainProductLink, ProductLink, Supermarket

PAIR_SEPARATOR = re.compile(r"[\s,;:>]+")


def parse_product(token):
    cod, dot, v = token.partition(".")
    if not dot or not v:
        raise ValueError(f"{token!r} has no variant, use cod.v")
    return int(cod), int(v)


def parse_pair(text):
    parts = [p for p in PAIR_SEPARATOR.split(text.strip()) if p]
    if len(parts) != 2:
        raise ValueError(f"expected 2 products, got {len(parts)}")
    primary, secondary = parse_product(parts[0]), parse_product(parts[1])
    if primary == secondary:
        raise ValueError("subentrante and sostituito are the same product")
    return primary, secondary


class Command(BaseCommand):
    help = "Add chain-wide product substitutions and apply them to every real store."

    def add_arguments(self, parser):
        parser.add_argument("pairs", nargs="*", help="SUBENTRANTE:SOSTITUITO as cod.v, e.g. 12345.1:67890.1")
        parser.add_argument("--file", help="File with one pair per line")
        parser.add_argument("--from-store", help="Copy this store's links into the chain list")
        parser.add_argument("--notes", default=None, help="Notes stored on every link")
        parser.add_argument("--author", help="Username shown as the links' author")
        parser.add_argument("--purge-old", action="store_true",
                            help="Purge the sostituito when the link is removed from a store")
        parser.add_argument("--dry-run", action="store_true", help="Report only, write nothing")

    def handle(self, *args, **opts):
        author = None
        if opts["author"]:
            author = User.objects.filter(username=opts["author"]).first()
            if author is None:
                raise CommandError(f"User {opts['author']!r} not found.")

        entries = self.collect_entries(opts, author)
        if not entries:
            raise CommandError("No pairs given. Pass them as arguments, with --file or --from-store.")

        seen = set()
        for primary, secondary, *_ in entries:
            for product in (primary, secondary):
                if product in seen:
                    raise CommandError(f"{fmt(product)} appears in more than one pair.")
                seen.add(product)

        dry_run = opts["dry_run"]
        suffix = " [DRY RUN]" if dry_run else ""

        new_links = []
        self.stdout.write(f"Chain list{suffix}:")
        with transaction.atomic():
            for primary, secondary, notes, created_by, purge in entries:
                existing = ChainProductLink.objects.filter(
                    Q(primary_cod=primary[0], primary_v=primary[1])
                    | Q(secondary_cod=primary[0], secondary_v=primary[1])
                    | Q(primary_cod=secondary[0], primary_v=secondary[1])
                    | Q(secondary_cod=secondary[0], secondary_v=secondary[1])
                ).first()
                label = fmt_pair((primary, secondary))
                if existing:
                    same = (
                        (existing.primary_cod, existing.primary_v) == primary
                        and (existing.secondary_cod, existing.secondary_v) == secondary
                    )
                    reason = "already in the chain list" if same else (
                        "conflicts with " + fmt_pair((
                            (existing.primary_cod, existing.primary_v),
                            (existing.secondary_cod, existing.secondary_v),
                        ))
                    )
                    self.stdout.write(self.style.WARNING(f"  skip    {label}: {reason}"))
                    continue
                link = ChainProductLink(
                    primary_cod=primary[0], primary_v=primary[1],
                    secondary_cod=secondary[0], secondary_v=secondary[1],
                    notes=notes, created_by=created_by, purge_on_removal=purge,
                )
                if not dry_run:
                    link.save()
                new_links.append(link)
                self.stdout.write(self.style.SUCCESS(f"  add     {label}{'  [purge old]' if purge else ''}"))

        if not new_links:
            self.stdout.write("\nNothing new to apply.")
            return

        self.stdout.write(f"\nStores{suffix}:")
        added = verified = failed = 0
        for sm in real_supermarkets().order_by("name"):
            try:
                report = sync_store(sm, new_links, cleanup=False, dry_run=dry_run)
            except Exception as e:
                failed += 1
                self.stdout.write(self.style.ERROR(f"  {sm.name}: FAILED - {e}"))
                continue
            added += len(report.added)
            verified += len(report.verified)
            for line in report.lines():
                self.stdout.write(f"  {sm.name}: {line}")

        verb = "Would add" if dry_run else "Added"
        self.stdout.write(
            f"\n{verb} {len(new_links)} chain link(s); {added} store link(s), "
            f"{verified} subentrante(s) verified, {failed} store(s) failed."
        )

    def collect_entries(self, opts, author):
        """[(primary, secondary, notes, created_by, purge_on_removal)]"""
        entries = []
        raw = list(opts["pairs"])
        if opts["file"]:
            with open(opts["file"], encoding="utf-8") as f:
                for line in f:
                    line = line.split("#", 1)[0].strip()
                    if line:
                        raw.append(line)
        for text in raw:
            try:
                primary, secondary = parse_pair(text)
            except ValueError as e:
                raise CommandError(f"Invalid pair {text!r}: {e}")
            entries.append((primary, secondary, opts["notes"] or "", author, opts["purge_old"]))

        if opts["from_store"]:
            sm = Supermarket.objects.filter(name=opts["from_store"]).first()
            if sm is None:
                raise CommandError(f"Store {opts['from_store']!r} not found.")
            for link in ProductLink.objects.filter(supermarket=sm).select_related("created_by"):
                entries.append((
                    (link.primary_cod, link.primary_v),
                    (link.secondary_cod, link.secondary_v),
                    opts["notes"] if opts["notes"] is not None else link.notes,
                    author or link.created_by,
                    opts["purge_old"] or link.purge_on_removal,
                ))
        return entries
