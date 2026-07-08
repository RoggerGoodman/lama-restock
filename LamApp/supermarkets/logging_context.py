"""Dynamic per-supermarket log routing.

Django's LOGGING dict is fixed at process start, but supermarkets are DB rows
created at runtime, so there's no way to statically declare one handler per
supermarket. Instead, a contextvar tracks which supermarket the currently
executing code is working on, and the custom handlers below read it at
emit() time to decide which file to write to.

Celery's prefork pool runs one task at a time per process, and Gunicorn's
sync workers run one request at a time per process, so a plain contextvar
set/reset around each per-supermarket entry point is sufficient - no cross-task
locking is needed for correctness.
"""
import contextvars
import logging
import threading
from pathlib import Path

from concurrent_log_handler import ConcurrentRotatingFileHandler
from django.conf import settings
from django.utils.text import slugify

current_supermarket = contextvars.ContextVar('current_supermarket', default=None)
current_order_handler = contextvars.ContextVar('current_order_handler', default=None)

_SYSTEM_SLUG = '_system'

_SIMPLE_FORMAT = logging.Formatter(
    fmt='[{levelname}] {asctime} {message}',
    style='{',
    datefmt='%Y-%m-%d %H:%M:%S',
)


def supermarket_slug(name):
    return slugify(name) or 'unknown'


def _logs_dir():
    return Path(settings.BASE_DIR) / 'logs'


class SupermarketLogContext:
    """with SupermarketLogContext(supermarket.name): ... routes app/celery/selenium
    logs emitted inside the block to logs/<slug>/<type>.log."""

    def __init__(self, supermarket_name):
        self.slug = supermarket_slug(supermarket_name)
        self._token = None

    def __enter__(self):
        self._token = current_supermarket.set(self.slug)
        return self

    def __exit__(self, exc_type, exc, tb):
        current_supermarket.reset(self._token)
        return False


class OrderRunLogContext:
    """with OrderRunLogContext(supermarket.name, storage.name): ... opens a fresh
    logs/<slug>/decision_maker/<timestamp>_<storage-slug>.log for the duration of
    one order run, and closes it on exit."""

    def __init__(self, supermarket_name, storage_name):
        from django.utils import timezone

        slug = supermarket_slug(supermarket_name)
        storage_slug = supermarket_slug(storage_name)
        directory = _logs_dir() / slug / 'decision_maker'
        directory.mkdir(parents=True, exist_ok=True)

        timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
        self.path = directory / f'{timestamp}_{storage_slug}.log'
        self._handler = logging.FileHandler(self.path, encoding='utf-8')
        self._handler.setFormatter(_SIMPLE_FORMAT)
        self._token = None

    def __enter__(self):
        self._token = current_order_handler.set(self._handler)
        return self

    def __exit__(self, exc_type, exc, tb):
        current_order_handler.reset(self._token)
        self._handler.close()
        return False


def enter_supermarket_log(supermarket_name):
    """Non-context-manager form of SupermarketLogContext, for wrapping existing
    function bodies without reindenting. Pair with exit_supermarket_log in a
    finally block."""
    ctx = SupermarketLogContext(supermarket_name)
    ctx.__enter__()
    return ctx


def exit_supermarket_log(ctx):
    if ctx is not None:
        ctx.__exit__(None, None, None)


def enter_order_log(supermarket_name, storage_name):
    """Non-context-manager form of OrderRunLogContext, for wrapping existing
    function bodies without reindenting. Pair with exit_order_log in a finally
    block."""
    ctx = OrderRunLogContext(supermarket_name, storage_name)
    ctx.__enter__()
    return ctx


def exit_order_log(ctx):
    if ctx is not None:
        ctx.__exit__(None, None, None)


class PerSupermarketFileHandler(logging.Handler):
    """One instance per log type (app/celery/selenium). Reads current_supermarket
    at emit() time and delegates to a lazily-created, cached ConcurrentRotatingFileHandler
    at logs/<slug>/<type>.log. Falls back to logs/_system/<type>.log with no active context."""

    _lock = threading.Lock()

    def __init__(self, log_type, maxBytes=10485760, backupCount=1, level=logging.NOTSET):
        super().__init__(level=level)
        self.log_type = log_type
        self.maxBytes = maxBytes
        self.backupCount = backupCount
        self._handlers = {}

    def _handler_for(self, slug):
        cached = self._handlers.get(slug)
        if cached is not None:
            return cached
        with self._lock:
            cached = self._handlers.get(slug)
            if cached is not None:
                return cached
            directory = _logs_dir() / slug
            directory.mkdir(parents=True, exist_ok=True)
            handler = ConcurrentRotatingFileHandler(
                str(directory / f'{self.log_type}.log'),
                maxBytes=self.maxBytes,
                backupCount=self.backupCount,
                encoding='utf-8',
            )
            handler.setFormatter(_SIMPLE_FORMAT)
            self._handlers[slug] = handler
            return handler

    def emit(self, record):
        slug = current_supermarket.get() or _SYSTEM_SLUG
        try:
            handler = self._handler_for(slug)
            handler.emit(record)
        except Exception:
            self.handleError(record)


class PerOrderFileHandler(logging.Handler):
    """For decision_maker-family loggers. Delegates to the current order run's open
    FileHandler (see OrderRunLogContext). With no active order context, falls back
    to a shared ConcurrentRotatingFileHandler at logs/_system/decision_maker.log."""

    _lock = threading.Lock()

    def __init__(self, maxBytes=20971520, backupCount=1, level=logging.NOTSET):
        super().__init__(level=level)
        self.maxBytes = maxBytes
        self.backupCount = backupCount
        self._fallback_handler = None

    def _fallback(self):
        if self._fallback_handler is not None:
            return self._fallback_handler
        with self._lock:
            if self._fallback_handler is not None:
                return self._fallback_handler
            directory = _logs_dir() / _SYSTEM_SLUG
            directory.mkdir(parents=True, exist_ok=True)
            handler = ConcurrentRotatingFileHandler(
                str(directory / 'decision_maker.log'),
                maxBytes=self.maxBytes,
                backupCount=self.backupCount,
                encoding='utf-8',
            )
            handler.setFormatter(_SIMPLE_FORMAT)
            self._fallback_handler = handler
            return handler

    def emit(self, record):
        try:
            handler = current_order_handler.get() or self._fallback()
            handler.emit(record)
        except Exception:
            self.handleError(record)
