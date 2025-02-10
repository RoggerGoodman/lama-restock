from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import Supermarket, Category
from .scripts.finder import Finder  # Import the Finder class

@receiver(post_save, sender=Supermarket)
def fetch_categories(sender, instance, created, **kwargs):
    if created:  # Only run when a new supermarket is created
        finder = Finder()
        finder.login(instance.username, instance.password)  # Use the supermarket's credentials
        categories = finder.find_storages()

        for category_name in categories:
            Category.objects.create(name=category_name, supermarket=instance)
