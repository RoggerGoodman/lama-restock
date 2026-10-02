# LamApp/supermarkets/equipment.py
"""Store equipment catalog (Todis "Listino Attrezzatura PdV"), ordered on the GENERI VARI storage."""
import json
from functools import lru_cache
from pathlib import Path

CATALOG_PATH = Path(__file__).resolve().parent / "data" / "equipment_catalog.json"
EQUIPMENT_SETTORE = "GENERI VARI"


@lru_cache(maxsize=1)
def load_catalog():
    with open(CATALOG_PATH, encoding="utf-8") as f:
        return json.load(f)


def catalog_by_category():
    """[(category, [items])] in listino order."""
    groups = {}
    for item in load_catalog():
        groups.setdefault(item["category"], []).append(item)
    return list(groups.items())


def catalog_index():
    return {(item["cod"], item["v"]): item for item in load_catalog()}


def equipment_storage(supermarket):
    return supermarket.storages.filter(settore=EQUIPMENT_SETTORE).first()
