from django import template

register = template.Library()


@register.filter
def get_item(dictionary, key):
    """Get an item from a dictionary by key."""
    if dictionary is None:
        return None
    return dictionary.get(key)


@register.simple_tag
def pending_credit_notes_count(user):
    """Sidebar badge: credit notes waiting for this user's approval."""
    if not user.is_authenticated:
        return 0
    from supermarkets.models import CreditNote
    return CreditNote.objects.filter(
        storage__supermarket__owner=user, status=CreditNote.STATUS_PENDING
    ).count()
