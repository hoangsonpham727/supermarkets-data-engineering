"""Pure-Python parsing & cleaning helpers for the supermarket price pipeline.

These functions are deliberately framework-free (no PySpark) so they can be:
  * unit-tested locally with pytest, and
  * wrapped as Spark UDFs in ``src/pipelines/silver.py``.

The four source feeds (Aldi, Coles, IGA, Woolworths) each express the same
concepts in different, messy ways. Centralising the parsing rules here keeps the
Silver transformation declarative and the edge-cases tested.
"""

from __future__ import annotations

import ast
import hashlib
import re
from dataclasses import dataclass
from typing import Optional

# --------------------------------------------------------------------------- #
# Unit-price parsing
# --------------------------------------------------------------------------- #
# Real examples seen across the four feeds:
#   "$11.16 per 100g"      (Aldi)
#   "3c per tablet"        (Aldi  -> cents, count-based)
#   "$2.58 per 100mL"      (Coles)
#   "$24.75 per 1Kg"       (Coles)
#   "2.46 per litre"       (IGA   -> no '$' sign)
#   "5.50 per kg"          (IGA / WOW)

# Canonical units we collapse the long tail of spellings into.
_WEIGHT_UNITS = {"mg": 0.001, "g": 1.0, "kg": 1000.0}          # -> grams
_VOLUME_UNITS = {"ml": 1.0, "millilitre": 1.0, "l": 1000.0,    # -> millilitres
                 "litre": 1000.0, "liter": 1000.0}
# Anything count-like normalises to "each".
_COUNT_WORDS = {"each", "ea", "tablet", "tablets", "capsule", "capsules",
                "pack", "pk", "sheet", "sheets", "wash", "washes", "serve",
                "serves", "unit", "units", "item", "items", "piece", "pieces"}


@dataclass(frozen=True)
class UnitPrice:
    """Structured result of parsing a free-text 'price per unit' string."""
    value_aud: Optional[float]      # price in dollars (cents already converted)
    quantity: Optional[float]       # the package quantity the price applies to
    unit: Optional[str]             # canonical unit: g/kg/mg/ml/l/each
    measure: Optional[str]          # display token, e.g. 'per_100g', 'per_l'

    @property
    def ok(self) -> bool:
        return self.value_aud is not None and self.unit is not None


def _canonical_unit(raw_unit: str) -> Optional[str]:
    u = raw_unit.strip().lower()
    if u in _WEIGHT_UNITS:
        return u
    if u in _VOLUME_UNITS:
        # collapse all volume spellings to 'l' or 'ml' buckets
        return "ml" if _VOLUME_UNITS[u] == 1.0 else "l"
    if u in _COUNT_WORDS:
        return "each"
    return None


def _parse_unit_price(raw: Optional[str]) -> UnitPrice:
    if raw is None:
        return UnitPrice(None, None, None, None)
    text = str(raw).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return UnitPrice(None, None, None, None)

    # Split on the first 'per' or '/' separator.
    parts = re.split(r"\s*(?:per|/)\s*", text, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return UnitPrice(None, None, None, None)
    left, right = parts[0].strip(), parts[1].strip()

    # ---- price side --------------------------------------------------------
    has_dollar = "$" in left
    is_cents = bool(re.search(r"\dc\b", left)) or (left.lower().endswith("c") and not has_dollar)
    num_match = re.search(r"[\d.]+", left)
    if not num_match:
        return UnitPrice(None, None, None, None)
    value = float(num_match.group())
    if is_cents and not has_dollar:
        value = round(value / 100.0, 4)

    # ---- quantity + unit side ---------------------------------------------
    qty_match = re.match(r"([\d.]+)?\s*([a-zA-Z]+)", right)
    if not qty_match:
        return UnitPrice(value, None, None, None)
    qty = float(qty_match.group(1)) if qty_match.group(1) else 1.0
    unit = _canonical_unit(qty_match.group(2))
    if unit is None:
        return UnitPrice(value, qty, None, None)

    qty_token = "" if qty == 1.0 else (str(int(qty)) if qty.is_integer() else str(qty))
    measure = f"per_{qty_token}{unit}"
    return UnitPrice(value, qty, unit, measure)


def parse_unit_price(raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    """Return ``(value_aud, measure)`` — the headline parsing contract.

    >>> parse_unit_price("$11.16 per 100g")
    (11.16, 'per_100g')
    >>> parse_unit_price("3c per tablet")
    (0.03, 'per_each')
    """
    up = _parse_unit_price(raw)
    return up.value_aud, up.measure


def normalize_unit_price(raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    """Collapse any unit price to a comparable base: per **kg**, per **L**, or per **each**.

    This is what makes cross-retailer comparison valid: "$2.58 per 100mL" and
    "2.46 per litre" both resolve to a price-per-litre.

    >>> normalize_unit_price("$2.58 per 100mL")   # -> $25.80 / L
    (25.8, 'per_l')
    >>> normalize_unit_price("$11.16 per 100g")   # -> $111.60 / kg
    (111.6, 'per_kg')
    """
    up = _parse_unit_price(raw)
    if not up.ok or up.quantity in (None, 0):
        return None, None
    if up.unit in _WEIGHT_UNITS:
        grams = _WEIGHT_UNITS[up.unit] * up.quantity
        return round(up.value_aud / grams * 1000.0, 4), "per_kg"
    if up.unit in {"ml", "l"}:
        ml = (1.0 if up.unit == "ml" else 1000.0) * up.quantity
        return round(up.value_aud / ml * 1000.0, 4), "per_l"
    # count-based
    return round(up.value_aud / up.quantity, 4), "per_each"


# --------------------------------------------------------------------------- #
# Stringified Python list parsing (Coles 'Suppliers', WOW 'Department', etc.)
# --------------------------------------------------------------------------- #
def parse_list_string(raw: Optional[str]) -> list[str]:
    """Parse a stringified Python list into a clean ``list[str]``.

    Handles the gnarly Coles supplier cells that mix single and double quotes:
        "['97446 CARTER & SPENCER', \"635235 COSTA'S P/L\"]"
    and the simpler WOW/IGA category cells like "['Bakery']" / "[]".
    """
    if raw is None:
        return []
    text = str(raw).strip()
    if not text or text in {"[]", "nan", "None", "null"}:
        return []
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple)):
            return [str(x).strip() for x in parsed if str(x).strip()]
        return [str(parsed).strip()]
    except (ValueError, SyntaxError):
        # Fallback: strip brackets/quotes and split on commas.
        inner = text.strip("[]")
        items = re.split(r"',\s*'|\",\s*\"|',\s*\"|\",\s*'", inner)
        return [i.strip(" '\"") for i in items if i.strip(" '\"")]


def first_from_list(raw: Optional[str], default: str = "unknown") -> str:
    """First element of a stringified list, or ``default`` when empty.

    Used to derive a single 'primary supplier' / clean category from list cells.
    """
    items = parse_list_string(raw)
    return items[0] if items else default


# --------------------------------------------------------------------------- #
# Misc field harmonisation
# --------------------------------------------------------------------------- #
def make_surrogate_sku(retailer: str, product_name: str,
                       product_url: Optional[str] = None) -> str:
    """Deterministic surrogate key for feeds without a SKU (Aldi).

    Stable across runs/days for the same product so SCD2 history is coherent.
    """
    basis = "|".join([
        (retailer or "").strip().lower(),
        (product_name or "").strip().lower(),
        (product_url or "").strip().lower(),
    ])
    return hashlib.md5(basis.encode("utf-8")).hexdigest()[:16]


def extract_brand(product_name: Optional[str]) -> Optional[str]:
    """Best-effort brand from a product name for feeds lacking a Brand column.

    Heuristic only (first 1-2 capitalised tokens) — documented as approximate;
    a future enhancement is a curated brand dictionary / fuzzy match.
    """
    if not product_name:
        return None
    tokens = str(product_name).strip().split()
    if not tokens:
        return None
    brand_tokens = []
    for tok in tokens[:2]:
        if tok[:1].isupper() and tok.isalpha():
            brand_tokens.append(tok)
        else:
            break
    return " ".join(brand_tokens) if brand_tokens else tokens[0]


_AVAILABILITY_MAP = {
    "available": "available",
    "in stock": "available",
    "instock": "available",
    "unavailable": "unavailable",
    "out of stock": "unavailable",
    "sold out": "unavailable",
}


def normalize_availability(raw: Optional[str]) -> str:
    """Map free-text availability to a standard enum: available/unavailable/unknown."""
    if raw is None:
        return "unknown"
    return _AVAILABILITY_MAP.get(str(raw).strip().lower(), "unknown")


def to_special_bool(*fields: Optional[str]) -> bool:
    """True when any 'on special' signal is truthy.

    Coles uses an explicit ``On Special`` flag; WOW carries a non-empty
    ``Specials`` description; Aldi/IGA have no signal (-> False).
    """
    falsy = {"nan", "none", "null", "false", "0", "no", "f", "n"}
    for f in fields:
        if f is None:
            continue
        s = str(f).strip().lower()
        if not s or s in falsy:
            continue
        # any other non-empty value (explicit flag OR a special description) -> True
        return True
    return False


def clean_price(raw) -> Optional[float]:
    """Parse a price cell to float dollars, tolerating '$', commas and blanks."""
    if raw is None:
        return None
    text = str(raw).strip().replace("$", "").replace(",", "")
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Canonical category mapping (used to build silver.dim_category)
# --------------------------------------------------------------------------- #
# The four retailers use inconsistent taxonomy. We map raw category tokens to a
# shared two-level hierarchy so BI can compare like-for-like.
CATEGORY_MAP: dict[str, tuple[str, str]] = {
    # raw token (lowercased)            -> (canonical_l1,        canonical_l2)
    "fruit-vegetables":                  ("Fresh Food",          "Fruit & Vegetables"),
    "fruit-&-veg":                       ("Fresh Food",          "Fruit & Vegetables"),
    "fruit":                             ("Fresh Food",          "Fruit & Vegetables"),
    "bakery":                            ("Fresh Food",          "Bakery"),
    "dairy-eggs-fridge":                 ("Fresh Food",          "Dairy, Eggs & Fridge"),
    "meat-seafood":                      ("Fresh Food",          "Meat & Seafood"),
    "drinks":                            ("Drinks",              "Drinks"),
    "soft-drinks":                       ("Drinks",              "Soft Drinks"),
    "health-beauty":                     ("Health & Beauty",     "Health & Beauty"),
    "health":                            ("Health & Beauty",     "Health"),
    "groceries":                         ("Pantry",              "Groceries"),
    "pantry":                            ("Pantry",              "Pantry"),
    "frozen":                            ("Frozen",              "Frozen"),
    "household":                         ("Household",           "Household"),
    "pet":                               ("Pet",                 "Pet"),
    "baby":                              ("Baby",                "Baby"),
}


def canonical_category(raw_main: Optional[str],
                       raw_sub: Optional[str] = None) -> tuple[str, str]:
    """Resolve a (canonical_l1, canonical_l2) pair from raw retailer categories.

    Tries the sub-category first (more specific), then the main category, then
    falls back to a title-cased main / 'Other'.
    """
    for token in (raw_sub, raw_main):
        if not token:
            continue
        key = str(token).strip().lower().replace(" ", "-").replace("&", "&")
        if key in CATEGORY_MAP:
            return CATEGORY_MAP[key]
    l1 = (str(raw_main).strip().title() if raw_main else "Other")
    return l1, l1
