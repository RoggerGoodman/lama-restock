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


@register.filter
def it_num(value, decimals=2):
    """1234.5 -> '1.234,50' (Italian grouping and decimal comma); '' for None."""
    if value is None or value == '':
        return ''
    formatted = f"{float(value):,.{int(decimals)}f}"
    return formatted.replace(',', '\x00').replace('.', ',').replace('\x00', '.')
