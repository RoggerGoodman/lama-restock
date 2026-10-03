from django.core.management.base import BaseCommand

from supermarkets.models import Supermarket


class Command(BaseCommand):
    help = "Encrypt stored Dropzone passwords at rest. Safe to re-run."

    def handle(self, *args, **opts):
        count = 0
        for sm in Supermarket.objects.all():
            # Reads come back as plaintext (legacy or decrypted); update() re-encrypts.
            Supermarket.objects.filter(pk=sm.pk).update(password=sm.password)
            count += 1
        self.stdout.write(self.style.SUCCESS(f"Encrypted {count} Dropzone passwords."))
