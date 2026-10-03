# LamApp/supermarkets/fields.py
import os
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken
from django.core.exceptions import ImproperlyConfigured
from django.db import models


@lru_cache(maxsize=1)
def _fernet():
    key = os.environ.get('FIELD_ENCRYPTION_KEY')
    if not key:
        raise ImproperlyConfigured("FIELD_ENCRYPTION_KEY environment variable must be set!")
    return Fernet(key.encode())


class EncryptedCharField(models.CharField):
    """CharField stored Fernet-encrypted at rest; reads return the plaintext."""

    def get_prep_value(self, value):
        value = super().get_prep_value(value)
        if not value:
            return value
        return _fernet().encrypt(value.encode()).decode()

    def from_db_value(self, value, expression, connection):
        if not value:
            return value
        try:
            return _fernet().decrypt(value.encode()).decode()
        except InvalidToken:
            # Legacy plaintext row, not yet run through encrypt_dropzone_passwords
            return value
