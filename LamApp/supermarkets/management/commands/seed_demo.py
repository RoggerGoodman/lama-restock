"""
Create (or refresh) the read-only demo account and its seeded showcase data.

Idempotent, and touches ONLY the demo supermarket's schema. The demo user is put
in the "Demo" group; DemoReadOnlyMiddleware blocks every mutating request for that
group, and the Celery beat tasks exclude it (see supermarkets/demo.py), so nothing
here can ever fire a real order.

    python manage.py seed_demo
    python manage.py seed_demo --password mypass --products-per-cluster 15

Day-to-day the data is also refreshed lazily on demo login (see demo_seed.py), so
you normally never need to run this by hand after the first time.
"""
from django.core.management.base import BaseCommand

from supermarkets.demo_seed import ensure_demo_account


class Command(BaseCommand):
    help = "Create or refresh the read-only demo account and its seeded data."

    def add_arguments(self, parser):
        parser.add_argument("--password", default="demo1234",
                            help="Password for the demo login (default: demo1234)")
        parser.add_argument("--products-per-cluster", type=int, default=10,
                            help="Products to generate per cluster (default: 10)")

    def handle(self, *args, **opts):
        ensure_demo_account(
            password=opts["password"],
            per_cluster=opts["products_per_cluster"],
            log=self.stdout.write,
        )
        self.stdout.write(self.style.SUCCESS(
            f"Demo ready. Login: demo / {opts['password']}"))
