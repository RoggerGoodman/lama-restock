"""Helpers shared by several view modules."""


def net_price_of(price, iva):
    """
    Strip IVA from an IVA-included sale price (price_std/price_s are stored
    IVA-included; cost is net). iva is the whole-percent aliquot (4, 10, 22...);
    None/0 means no adjustment. Profit margins are computed on this net price so
    they match the supplier's Margine.
    """
    price = float(price or 0.0)
    if iva:
        return price / (1 + float(iva) / 100.0)
    return price


def parse_shelf_barcode(code):
    """
    Decode an internal shelf-label barcode (a "crypted" cod.v) into (cod, v).

    Shelf codes are 13 digits: discard the first 5 and the last 1; the two
    remaining last digits are the variante, and what is left in the middle is the
    cod (both stripped of leading zeros). E.g.
    7982024129017 -> (24129, 1), 7982008181109 -> (8181, 10).

    Product EANs printed on the item use other prefixes and return None, so the
    caller falls back to a normal EAN lookup.
    """
    code = (code or '').strip()
    if len(code) == 13 and code.isdigit() and code.startswith('7982'):
        return int(code[5:10]), int(code[10:12])
    return None
