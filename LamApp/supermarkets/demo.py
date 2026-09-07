# Demo account: a read-only showcase login for prospects.
# One User in the DEMO_GROUP owns one seeded Supermarket; DemoReadOnlyMiddleware
# blocks every mutating request for that user, so no order/scrape/sync can fire.
DEMO_GROUP = "Demo"


def is_demo_user(user):
    """True if `user` is the demo account. Cached on the request-bound user."""
    if not user or not user.is_authenticated:
        return False
    cached = getattr(user, "_is_demo", None)
    if cached is None:
        cached = user.groups.filter(name=DEMO_GROUP).exists()
        user._is_demo = cached
    return cached


def demo_context(request):
    """Expose `is_demo` to every template (see base.html demo banner)."""
    return {"is_demo": is_demo_user(getattr(request, "user", None))}


def real_supermarkets():
    """All supermarkets except the demo. The Celery beat tasks run as the system
    and bypass DemoReadOnlyMiddleware, so they must exclude the demo explicitly."""
    from .models import Supermarket
    return Supermarket.objects.exclude(owner__groups__name=DEMO_GROUP)


def real_storages():
    """Storages whose supermarket is not the demo (see real_supermarkets)."""
    from .models import Storage
    return Storage.objects.exclude(supermarket__owner__groups__name=DEMO_GROUP)
