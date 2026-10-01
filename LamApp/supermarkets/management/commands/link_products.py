"""
Create product links (substitutions) in EVERY real supermarket.

Each pair is PRIMARY SECONDARY: the primary is the product to order, the
secondary the one being phased out. Both must be written as cod.v.

    python manage.py link_products 12345.0:67890.1 23456.0:78901.2
    python manage.py link_products --file pairs.txt --notes "Email PAC 01/10"
    python manage.py link_products --file pairs.txt --dry-run

File format: one pair per line, separated by space, comma, semicolon, colon
or '>'. Blank lines and '#' comments are ignored.

A store is skipped for a pair if either product already belongs to a link
there: the order logic supports only one partner per product.
"""
import re

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q

from supermarkets.demo import real_supermarkets
from supermarkets.models import ProductLink, ProductLinkNotification

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
        raise ValueError("primary and secondary are the same product")
    return primary, secondary


def fmt(product):
    return f"{product[0]}.{product[1]}"


class Command(BaseCommand):
    help = "Link product pairs (primary -> secondary) in every real supermarket."

    def add_arguments(self, parser):
        parser.add_argument("pairs", nargs="*", help="PRIMARY:SECONDARY as cod.v, e.g. 12345.0:67890.1")
        parser.add_argument("--file", help="File with one pair per line")
        parser.add_argument("--notes", default="", help="Notes stored on every link")
        parser.add_argument("--dry-run", action="store_true", help="Report only, write nothing")

    def handle(self, *args, **opts):
        raw = list(opts["pairs"])
        if opts["file"]:
            with open(opts["file"], encoding="utf-8") as f:
                for line in f:
                    line = line.split("#", 1)[0].strip()
                    if line:
                        raw.append(line)
        if not raw:
            raise CommandError("No pairs given. Pass them as arguments or with --file.")

        pairs = []
        for text in raw:
            try:
                pairs.append(parse_pair(text))
            except ValueError as e:
                raise CommandError(f"Invalid pair {text!r}: {e}")

        seen = set()
        for primary, secondary in pairs:
            for product in (primary, secondary):
                if product in seen:
                    raise CommandError(f"{fmt(product)} appears in more than one pair.")
                seen.add(product)

        supermarkets = list(real_supermarkets().order_by("name"))
        dry_run = opts["dry_run"]
        self.stdout.write(
            f"{len(pairs)} pair(s) x {len(supermarkets)} supermarket(s)"
            f"{' [DRY RUN]' if dry_run else ''}\n"
        )

        created = skipped = 0
        with transaction.atomic():
            for primary, secondary in pairs:
                self.stdout.write(f"{fmt(primary)} -> {fmt(secondary)}")
                for sm in supermarkets:
                    existing = ProductLink.objects.filter(supermarket=sm).filter(
                        Q(primary_cod=primary[0], primary_v=primary[1])
                        | Q(secondary_cod=primary[0], secondary_v=primary[1])
                        | Q(primary_cod=secondary[0], primary_v=secondary[1])
                        | Q(secondary_cod=secondary[0], secondary_v=secondary[1])
                    ).first()
                    if existing:
                        skipped += 1
                        same = (
                            (existing.primary_cod, existing.primary_v) == primary
                            and (existing.secondary_cod, existing.secondary_v) == secondary
                        )
                        reason = "already linked" if same else (
                            f"conflicts with {existing.primary_cod}.{existing.primary_v}"
                            f" -> {existing.secondary_cod}.{existing.secondary_v}"
                        )
                        self.stdout.write(self.style.WARNING(f"  skip    {sm.name}: {reason}"))
                        continue

                    created += 1
                    self.stdout.write(self.style.SUCCESS(f"  create  {sm.name}"))
                    if dry_run:
                        continue
                    fields = dict(
                        supermarket=sm,
                        primary_cod=primary[0], primary_v=primary[1],
                        secondary_cod=secondary[0], secondary_v=secondary[1],
                    )
                    ProductLink.objects.create(notes=opts["notes"], **fields)
                    ProductLinkNotification.objects.create(**fields)

        verb = "Would create" if dry_run else "Created"
        self.stdout.write(f"\n{verb} {created} link(s), skipped {skipped}.")
