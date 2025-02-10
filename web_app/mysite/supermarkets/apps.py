from django.apps import AppConfig

class SupermarketsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'supermarkets'

    def ready(self):
        import supermarkets.signals  # Import signals when the app starts