from django.apps import AppConfig


class SupermarketsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'supermarkets'

    def ready(self):
        from . import scheduler
        scheduler.start()
        from . import signals  # Import signals when the app starts