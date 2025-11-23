from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import Supermarket, Storage
from .scripts.finder import Finder  # Import the Finder class

@receiver(post_save, sender=Supermarket)
def fetch_storags(sender, instance, created, **kwargs):
    if created:  # Only run when a new supermarket is created
        finder = Finder()
        finder.login(instance.username, instance.password)  # Use the supermarket's credentials
        storages = finder.find_storages()

        for storage_name in storages:
            Storage.objects.create(name=storage_name, supermarket=instance)
