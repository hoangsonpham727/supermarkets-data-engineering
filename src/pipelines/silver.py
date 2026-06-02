"""Silver layer — conform the four heterogeneous feeds into one model + dimensions.

Lakeflow Declarative Pipeline (DLT). Responsibilities:
  1. Map each retailer's native columns to a single conformed schema.
  2. Apply parsing rules (unit price, list strings, brand, surrogate SKU).
  3. Enforce data-quality expectations (drop / quarantine / pass).
  4. Build conformed dimensions: dim_product (SCD2), dim_category, dim_retailer,
     dim_date.

SINGLE SOURCE OF TRUTH
  All cleaning / standardisation / data-quality logic lives in the *pure functions*
  in this file (the section below the imports). They are the only copy — the same
  functions are unit-tested directly (see tests/) and wrapped as Spark UDFs further
  down. There is intentionally no parallel module in src/common: Databricks workers
  cannot import src.common, so duplicating the logic there only invited divergence.

UDF SERIALISATION
  Spark serialises UDFs via cloudpickle and ships them to *worker* nodes. The pure
  functions are top-level in THIS shipped library file, so they serialise cleanly.
  Each function imports its stdlib dependencies (re, ast, hashlib) inside its own
  body so they resolve at execution time on the worker rather than via the closure.

LOCAL IMPORTABILITY
  `import dlt` / pyspark are guarded so this module can be imported in a plain
  Python (pytest) environment with neither installed: the pure functions load, and
  the DLT table/UDF wiring at the bottom is skipped (`if dlt is not None`).
"""

from __future__ import annotations

import re as _re

try:
    import dlt
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        ArrayType, BooleanType, DoubleType, StringType, StructField, StructType,
    )
except ImportError:               # local / unit-test environment without Spark+DLT
    dlt = None


# =========================================================================== #
#  PURE CLEANING / STANDARDISATION FUNCTIONS  (the single source of truth)
#  No Spark/DLT references; stdlib imported inside each body for worker safety.
# =========================================================================== #

# Canonical unit buckets shared by the unit-price parser.
_WEIGHT_UNITS = {"mg": 0.001, "g": 1.0, "kg": 1000.0}          # -> grams
_VOLUME_UNITS = {"ml": 1.0, "millilitre": 1.0, "l": 1000.0,    # -> millilitres
                 "litre": 1000.0, "liter": 1000.0}
_COUNT_WORDS = {"each", "ea", "tablet", "tablets", "capsule", "capsules",
                "pack", "pk", "sheet", "sheets", "wash", "washes", "serve",
                "serves", "unit", "units", "item", "items", "piece", "pieces"}


def _canonical_unit(raw_unit):
    """Collapse a raw unit token to one of: mg/g/kg, ml/l, each, or None."""
    u = raw_unit.strip().lower()
    if u in _WEIGHT_UNITS:
        return u
    if u in _VOLUME_UNITS:
        return "ml" if _VOLUME_UNITS[u] == 1.0 else "l"
    if u in _COUNT_WORDS:
        return "each"
    return None


def _parse_unit_price_core(raw):
    """Parse a free-text unit-price ONCE → (value_aud, qty, unit, measure).

    Handles the full variety across the four feeds:
      "$11.16 per 100g" | "3c per tablet" | "$2.58 per 100mL"
      "2.46 per litre"  | "5.50 per kg"   | "$24.75 per 1Kg" | "$0.58 / 100G"
    ``unit`` is canonical (mg/g/kg/ml/l/each) or None; ``measure`` is the display
    token e.g. 'per_100g'. Both parse_unit_price() and normalize_unit_price()
    derive from this single pass.
    """
    if raw is None:
        return (None, None, None, None)
    import re
    text = str(raw).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return (None, None, None, None)
    parts = re.split(r"\s*(?:per|/)\s*", text, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return (None, None, None, None)
    left, right = parts[0].strip(), parts[1].strip()

    has_dollar = "$" in left
    is_cents = bool(re.search(r"\dc\b", left)) or (
        left.lower().endswith("c") and not has_dollar
    )
    num = re.search(r"[\d.]+", left)
    if not num:
        return (None, None, None, None)
    value = float(num.group())
    if is_cents and not has_dollar:
        value = round(value / 100.0, 4)

    qty_m = re.match(r"([\d.]+)?\s*([a-zA-Z]+)", right)
    if not qty_m:
        return (value, None, None, None)
    qty = float(qty_m.group(1)) if qty_m.group(1) else 1.0
    unit = _canonical_unit(qty_m.group(2))
    if unit is None:
        return (value, qty, None, None)
    qty_tok = "" if qty == 1.0 else (str(int(qty)) if qty == int(qty) else str(qty))
    return (value, qty, unit, f"per_{qty_tok}{unit}")


def parse_unit_price(raw):
    """Return ``(value_aud, measure)`` — the headline parsing contract.

    >>> parse_unit_price("$11.16 per 100g")
    (11.16, 'per_100g')
    >>> parse_unit_price("3c per tablet")
    (0.03, 'per_each')
    """
    value, _qty, _unit, measure = _parse_unit_price_core(raw)
    return value, measure


def unit_price_all(raw):
    """Single-pass parse → (value, measure, norm_value, norm_measure).

    ``norm_*`` collapse the price to a comparable base (per kg / per L / per each)
    so cross-retailer comparison is valid; None when the unit is unrecognised.
    """
    value, qty, unit, measure = _parse_unit_price_core(raw)
    if unit is None or qty in (None, 0):
        return (value, measure, None, None)
    if unit in _WEIGHT_UNITS:
        grams = _WEIGHT_UNITS[unit] * qty
        return (value, measure, round(value / grams * 1000.0, 4), "per_kg")
    if unit in {"ml", "l"}:
        ml = (1.0 if unit == "ml" else 1000.0) * qty
        return (value, measure, round(value / ml * 1000.0, 4), "per_l")
    return (value, measure, round(value / qty, 4), "per_each")


def normalize_unit_price(raw):
    """Collapse any unit price to a comparable base: per **kg**, **L**, or **each**.

    >>> normalize_unit_price("$2.58 per 100mL")   # -> $25.80 / L
    (25.8, 'per_l')
    >>> normalize_unit_price("$11.16 per 100g")   # -> $111.60 / kg
    (111.6, 'per_kg')
    """
    _value, _measure, norm_value, norm_measure = unit_price_all(raw)
    return norm_value, norm_measure


def parse_list_string(raw):
    """Parse a stringified Python list into a clean ``list[str]``.

    Handles the gnarly Coles supplier cells that mix single and double quotes,
    e.g. ``"['97446 CARTER & SPENCER', \\"635235 COSTA'S P/L\\"]"`` and the
    simpler WOW/IGA category cells like ``"['Bakery']"`` / ``"[]"``.
    """
    if raw is None:
        return []
    import ast
    import re
    text = str(raw).strip()
    if not text or text in {"[]", "nan", "None", "null"}:
        return []
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple)):
            return [str(x).strip() for x in parsed if str(x).strip()]
        return [str(parsed).strip()]
    except (ValueError, SyntaxError):
        inner = text.strip("[]")
        items = re.split(r"',\s*'|\",\s*\"|',\s*\"|\",\s*'", inner)
        return [i.strip(" '\"") for i in items if i.strip(" '\"")]


def first_from_list(raw, default="unknown"):
    """First element of a stringified list, or ``default`` when empty."""
    items = parse_list_string(raw)
    return items[0] if items else default


def clean_price(raw):
    """Parse a price cell to float AUD, tolerating '$', commas and blanks."""
    if raw is None:
        return None
    import re
    text = re.sub(r"[$,]", "", str(raw).strip())
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def make_surrogate_sku(retailer, product_name, product_url=None):
    """Deterministic 16-char surrogate key for feeds without a SKU (Aldi).

    Stable across runs/days for the same product so SCD2 history stays coherent.
    """
    import hashlib
    basis = "|".join([
        (retailer or "").strip().lower(),
        (product_name or "").strip().lower(),
        (product_url or "").strip().lower(),
    ])
    return hashlib.md5(basis.encode("utf-8")).hexdigest()[:16]


def extract_brand(product_name):
    """Best-effort brand from a product name for feeds lacking a Brand column.

    NOTE: heuristic and approximate — it takes the leading 1-2 capitalised, all-
    alphabetic tokens (so lowercase feeds like IGA degrade to the first token,
    e.g. "granny smith apple" -> "granny"). Only Aldi/IGA rely on this, and only
    the SCD2-tracked ``brand`` attribute is affected, never a join key. A curated
    brand dictionary / fuzzy match is the future enhancement.
    """
    if not product_name:
        return None
    tokens = str(product_name).strip().split()
    if not tokens:
        return None
    brand = []
    for tok in tokens[:2]:
        if tok[:1].isupper() and tok.isalpha():
            brand.append(tok)
        else:
            break
    return " ".join(brand) if brand else tokens[0]


_AVAILABILITY_MAP = {
    "available": "available",
    "in stock": "available",
    "instock": "available",
    "unavailable": "unavailable",
    "out of stock": "unavailable",
    "sold out": "unavailable",
}


def normalize_availability(raw):
    """Map free-text availability to available / unavailable / unknown."""
    if raw is None:
        return "unknown"
    return _AVAILABILITY_MAP.get(str(raw).strip().lower(), "unknown")


def to_special_bool(*fields):
    """True when any on-special signal is truthy (flag or non-empty description).

    Coles uses an explicit ``On Special`` flag; WOW carries a non-empty
    ``Specials`` description; Aldi/IGA have no signal (-> False).
    """
    falsy = {"nan", "none", "null", "false", "0", "no", "f", "n"}
    for f in fields:
        if f is None:
            continue
        s = str(f).strip().lower()
        if s and s not in falsy:
            return True
    return False


# Category map is a plain dict of primitives — cloudpickle serialises it fine.
# Keys are lowercased tokens from any retailer's category field, in BOTH the raw
# scraped form ('health-beauty') and the notebook-cleaned form ('health beauty'),
# so _canonical_category resolves either without a separate normalisation step.
_CATEGORY_MAP = {
    # ---- Fruit & Veg ----
    "fruit-vegetables":          ("Fresh Food",      "Fruit & Vegetables"),
    "fruit-&-veg":               ("Fresh Food",      "Fruit & Vegetables"),
    "fruit & veg":               ("Fresh Food",      "Fruit & Vegetables"),
    "fruit":                     ("Fresh Food",      "Fruit & Vegetables"),
    "fruit vegetables":          ("Fresh Food",      "Fruit & Vegetables"),
    # ---- Bakery ----
    "bakery":                    ("Fresh Food",      "Bakery"),
    # ---- Dairy ----
    "dairy-eggs-fridge":         ("Fresh Food",      "Dairy, Eggs & Fridge"),
    "dairy eggs & fridge":       ("Fresh Food",      "Dairy, Eggs & Fridge"),   # WOW cleaned
    "dairy eggs fridge":         ("Fresh Food",      "Dairy, Eggs & Fridge"),
    "fresh product":             ("Fresh Food",      "Dairy, Eggs & Fridge"),   # Aldi raw
    # ---- Meat ----
    "meat-seafood":              ("Fresh Food",      "Meat & Seafood"),
    "meat seafood & deli":       ("Fresh Food",      "Meat & Seafood"),         # WOW cleaned
    "meat seafood deli":         ("Fresh Food",      "Meat & Seafood"),
    "deli":                      ("Fresh Food",      "Meat & Seafood"),
    # ---- Drinks ----
    "drinks":                    ("Drinks",          "Drinks"),
    "soft-drinks":               ("Drinks",          "Soft Drinks"),
    "soft drinks":               ("Drinks",          "Soft Drinks"),
    "beer wine spirits":         ("Drinks",          "Beer, Wine & Spirits"),
    # ---- Health & Beauty ----
    "health-beauty":             ("Health & Beauty", "Health & Beauty"),
    "health beauty":             ("Health & Beauty", "Health & Beauty"),        # Coles cleaned
    "health & beauty":           ("Health & Beauty", "Health & Beauty"),
    "health":                    ("Health & Beauty", "Health"),
    "beauty":                    ("Health & Beauty", "Health & Beauty"),        # Aldi raw
    "personal care":             ("Health & Beauty", "Personal Care"),
    # ---- Pantry ----
    "groceries":                 ("Pantry",          "Groceries"),
    "pantry":                    ("Pantry",          "Pantry"),
    "international foods":        ("Pantry",          "International Foods"),
    "biscuits snacks":           ("Pantry",          "Biscuits & Snacks"),
    "snacks":                    ("Pantry",          "Biscuits & Snacks"),
    # ---- Frozen ----
    "frozen":                    ("Frozen",          "Frozen"),
    "freezer":                   ("Frozen",          "Frozen"),                 # Aldi raw
    # ---- Household ----
    "household":                 ("Household",       "Household"),
    "laundry household":         ("Household",       "Household"),              # Aldi raw
    "laundry":                   ("Household",       "Laundry"),
    "cleaning":                  ("Household",       "Cleaning"),
    # ---- Baby ----
    "baby":                      ("Baby",            "Baby"),
    # ---- Pet ----
    "pet":                       ("Pet",             "Pet"),
}


def canonical_category(raw_main, raw_sub=None):
    """Return ``(canonical_l1, canonical_l2)`` from raw retailer category strings.

    Tries the sub-category first (more specific) then the main category, matching
    each against both the space- and dash-normalised key forms so raw scraped
    values ('health-beauty') and notebook-cleaned values ('health beauty') both
    resolve. Falls back to a title-cased main / 'Other'.
    """
    import re
    for token in (raw_sub, raw_main):
        if not token:
            continue
        base = str(token).strip().lower()
        if base in _CATEGORY_MAP:
            return _CATEGORY_MAP[base]
        dashed = re.sub(r"\s+", "-", base)
        if dashed in _CATEGORY_MAP:
            return _CATEGORY_MAP[dashed]
    # Unknown token: title-case it only if it looks like a real category. Anything
    # carrying digits-at-start or quote/bracket/slash characters is almost certainly
    # corrupt (e.g. a supplier fragment from a mis-parsed CSV) — bucket it as 'Other'
    # rather than letting junk surface in canonical_l1/l2.
    main = str(raw_main).strip() if raw_main else ""
    if main and not main[:1].isdigit() and not re.search(r"[\"'\[\]/]", main):
        return (main.title(), main.title())
    return ("Other", "Other")


# --------------------------------------------------------------------------- #
#  Per-retailer notebook-derived cleaners (each does distinct, real work:
#  name fixes, list parsing, category filtering — not duplicated logic).
# --------------------------------------------------------------------------- #

# Aldi raw sub-category slug -> cleaned name (canonical_category maps onward).
_ALDI_CAT_MAP = {
    "fresh product":      "Dairy Eggs & Fridge",
    "beauty":             "Health & Beauty",
    "health":             "Health & Beauty",
    "laundry household":  "Household",
    "freezer":            "Frozen",
}


def aldi_category(raw):
    """Map Aldi raw sub-category slugs to cleaned names, then title-case."""
    if not raw:
        return "Other"
    s = str(raw).strip().lower()
    return _ALDI_CAT_MAP.get(s, str(raw).strip()).title()


def parse_wow_departments(raw):
    """Parse WOW Department stringified list → clean list of category strings.

    Fixes comma-containing category names BEFORE splitting so they aren't torn
    apart, e.g. "['Meat, Seafood & Deli', 'Dairy, Eggs & Fridge']"
    -> ['Meat Seafood & Deli', 'Dairy Eggs & Fridge']. Empty -> ['NOT LISTED'].
    """
    import ast
    if not raw:
        return ["NOT LISTED"]
    text = str(raw).strip()
    text = text.replace("Meat, Seafood & Deli", "Meat Seafood & Deli")
    text = text.replace("Dairy, Eggs & Fridge", "Dairy Eggs & Fridge")
    try:
        parsed = ast.literal_eval(text)
        items = [str(x).strip().replace("'", "") for x in parsed if str(x).strip()]
    except (ValueError, SyntaxError):
        items = [text.strip("[]'\" ")]
    items = [i for i in items if i]
    return items if items else ["NOT LISTED"]


def primary_wow_department(raw):
    """Collapse a WOW Department list to ONE category for the conformed grain.

    Returns the first real department, 'NOT LISTED' when the list is empty, or
    None when the product is *only* Tobacco/Liquor (behind a login wall / BWS
    stock) so the caller drops it — preserving the notebook's exclusion intent
    without exploding one product into several fact rows.
    """
    depts = parse_wow_departments(raw)
    excluded = {"Tobacco Product", "Liquor"}
    real = [d for d in depts if d and d not in excluded]
    if real:
        return real[0]
    if any(d in excluded for d in depts):
        return None                      # purely excluded categories -> drop
    return "NOT LISTED"


def clean_package_size(raw):
    """6pk -> '6 pack', lowercase 'l' suffix -> 'L' (e.g. 500l -> 500L)."""
    if not raw:
        return ""
    import re
    s = str(raw)
    s = re.sub(r"(\d)pk\b", r"\1 pack", s)
    s = re.sub(r"(?<=\d)l\b", "L", s)
    return s.strip()


def clean_unit_size(raw):
    """'1 each' -> 'each', lowercase, lowercase l -> L."""
    if not raw:
        return ""
    import re
    s = str(raw).strip().replace("1 each", "each").lower()
    return re.sub(r"(?<=\d)l\b", "L", s)


def clean_category_text(raw):
    """Replace '--' and '-' with spaces and title-case (Coles/IGA category text)."""
    if not raw:
        return "Other"
    return str(raw).replace("--", " ").replace("-", " ").title().strip()


def clean_iga_product_name(raw):
    """IGA name fixes: trailing 'gm' -> 'g', '8pk' -> '8 pack', digit-l -> L.

    The digit-adjacent 'l' -> 'L' rule is intentionally broad (it also upper-cases
    the 'l' inside '600ml' -> '600mL'); acceptable for display normalisation.
    """
    if not raw:
        return ""
    import re
    s = str(raw)
    s = re.sub(r"gm$", "g", s)
    s = re.sub(r"(?<=\d)pk", " pack", s)
    s = re.sub(r"(?<=\d)l", "L", s)
    return s


def clean_iga_sku(raw):
    """IGA SKUs are barcodes stored as floats ('9300675009775.0'); strip '.0'."""
    if not raw:
        return ""
    s = str(raw).strip()
    return s[:-2] if s.endswith(".0") else s


# =========================================================================== #
#  DATA-QUALITY RULES  (pure predicates -> drop / quarantine / pass)
# =========================================================================== #

VALID_RETAILERS = {"Aldi", "Coles", "IGA", "Woolworths"}
PRICE_MIN = 0.0
PRICE_MAX = 1000.0


def check_retailer(retailer):
    if retailer in VALID_RETAILERS:
        return True, None
    return False, f"invalid_retailer:{retailer!r}"


def check_product_name(name):
    if name and str(name).strip():
        return True, None
    return False, "missing_product_name"


def check_price_present(price, availability):
    """An available product must have a positive price; an unavailable one may
    legitimately carry a null price (WOW 'Unavailable' rows) and is not dropped.
    """
    if price is None:
        if availability == "unavailable":
            return True, None          # acceptable; quarantined downstream
        return False, "null_price_for_available_item"
    if price <= PRICE_MIN:
        return False, f"non_positive_price:{price}"
    return True, None


def check_price_range(price):
    if price is None:
        return True, None
    if price > PRICE_MAX:
        return False, f"price_out_of_range:{price}"
    return True, None


def check_price_unit_consistency(price, unit_price_value, tolerance_ratio=100.0):
    """Flag rows where headline price and unit price disagree by orders of magnitude.

    NOTE: this compares the headline price to the *raw* unit-price value (e.g. a
    per-100mL figure), not the normalised per-kg/L value — so it is an order-of-
    magnitude sanity check, not an exact reconciliation. It correctly catches the
    Aldi anomaly (Price=69.0 vs '3c per tablet' => ratio 2300). Quarantine, don't
    drop: the unit price may still be useful.
    """
    if price is None or unit_price_value is None or unit_price_value <= 0:
        return True, None
    ratio = price / unit_price_value
    if ratio > tolerance_ratio or ratio < (1.0 / tolerance_ratio):
        return False, f"price_unit_mismatch:ratio={ratio:.1f}"
    return True, None


def classify_row(retailer, product_name, price, availability, unit_price_value):
    """Return ``(action, reason)`` where action is pass / quarantine / drop."""
    for ok, reason in (check_retailer(retailer), check_product_name(product_name)):
        if not ok:
            return "drop", reason
    present_ok, present_reason = check_price_present(price, availability)
    if not present_ok:
        return "drop", present_reason
    for ok, reason in (
        check_price_range(price),
        check_price_unit_consistency(price, unit_price_value),
    ):
        if not ok:
            return "quarantine", reason
    if price is None and availability == "unavailable":
        return "quarantine", "unavailable_no_price"
    return "pass", None


# =========================================================================== #
#  COLUMN-NAME RESOLVER  (pure; no Spark needed to compute the sanitised name)
# =========================================================================== #

_BAD_CHARS = _re.compile(r"[ ,;{}\(\)\n\t=]+")


def _sanitised_name(name):
    """Bronze renames e.g. 'Unit Price' -> 'Unit_Price'; reproduce that mapping."""
    sanitised = _BAD_CHARS.sub("_", name)
    if not name.startswith("_"):          # preserve metadata cols like _snapshot_date
        sanitised = sanitised.strip("_")
    return sanitised


# =========================================================================== #
#  DLT PIPELINE WIRING  (only runs on Databricks; skipped on local import)
# =========================================================================== #

if dlt is not None:

    _unit_schema = StructType([
        StructField("value",        DoubleType()),
        StructField("measure",      StringType()),
        StructField("norm_value",   DoubleType()),
        StructField("norm_measure", StringType()),
    ])

    udf_price        = F.udf(clean_price,               DoubleType())
    udf_unit_all     = F.udf(unit_price_all,            _unit_schema)
    udf_first        = F.udf(first_from_list,           StringType())
    udf_sku          = F.udf(make_surrogate_sku,        StringType())
    udf_brand        = F.udf(extract_brand,             StringType())
    udf_avail        = F.udf(normalize_availability,    StringType())
    udf_special      = F.udf(lambda a, b: to_special_bool(a, b), BooleanType())
    udf_cat          = F.udf(lambda m, s: list(canonical_category(m, s)),
                             ArrayType(StringType()))
    udf_classify     = F.udf(lambda *a: list(classify_row(*a)),
                             ArrayType(StringType()))
    udf_aldi_cat     = F.udf(aldi_category,             StringType())
    udf_wow_primary  = F.udf(primary_wow_department,    StringType())
    udf_clean_pkg    = F.udf(clean_package_size,        StringType())
    udf_clean_usize  = F.udf(clean_unit_size,           StringType())
    udf_clean_cat    = F.udf(clean_category_text,       StringType())
    udf_clean_iga_pn = F.udf(clean_iga_product_name,    StringType())
    udf_clean_iga_sk = F.udf(clean_iga_sku,             StringType())

    def _col(df, name):
        """Column expression for *name*, trying the Bronze-sanitised form (spaces
        -> underscores) if the original isn't present. Falls back to typed NULL so
        the downstream select always has the expected column.
        """
        if name in df.columns:
            return F.col(f"`{name}`")
        sanitised = _sanitised_name(name)
        if sanitised in df.columns:
            return F.col(f"`{sanitised}`")
        return F.lit(None)

    def _conform(df, *, retailer, name, sku=None, brand=None, price=None,
                 prev_price=None, unit_price=None, pkg=None, on_special_cols=(),
                 avail=None, ratings=None, supplier_list=None,
                 main_cat=None, sub_cat=None, url=None):
        """Project a retailer's Bronze df onto the conformed Silver schema."""

        name_c  = _col(df, name)
        url_c   = _col(df, url).cast("string") if url else F.lit(None).cast("string")
        src_sku = (_col(df, sku) if sku
                   else udf_sku(F.lit(retailer), name_c, url_c))
        brand_c = (_col(df, brand) if brand else udf_brand(name_c))

        # Single parse pass yields both the display and normalised unit price.
        _no_unit = F.struct(
            F.lit(None).cast("double").alias("value"),
            F.lit(None).cast("string").alias("measure"),
            F.lit(None).cast("double").alias("norm_value"),
            F.lit(None).cast("string").alias("norm_measure"),
        )
        unit = udf_unit_all(_col(df, unit_price)) if unit_price else _no_unit

        sp_args = [_col(df, c) for c in on_special_cols] or [F.lit(None), F.lit(None)]
        while len(sp_args) < 2:
            sp_args.append(F.lit(None))

        cat = udf_cat(
            _col(df, main_cat).cast("string") if main_cat else F.lit(None).cast("string"),
            _col(df, sub_cat).cast("string")  if sub_cat  else F.lit(None).cast("string"),
        )

        return df.select(
            F.lit(retailer).alias("retailer"),
            src_sku.cast("string").alias("source_sku"),
            name_c.cast("string").alias("product_name"),
            brand_c.cast("string").alias("brand"),
            (udf_price(_col(df, price))      if price      else F.lit(None).cast("double")).alias("price_aud"),
            (udf_price(_col(df, prev_price)) if prev_price else F.lit(None).cast("double")).alias("previous_price_aud"),
            unit["value"].alias("unit_price_value"),
            unit["measure"].alias("unit_price_measure"),
            unit["norm_value"].alias("norm_unit_price"),
            unit["norm_measure"].alias("norm_unit_measure"),
            (_col(df, pkg).cast("string") if pkg else F.lit(None).cast("string")).alias("package_size_raw"),
            udf_special(sp_args[0], sp_args[1]).alias("on_special"),
            udf_avail(_col(df, avail) if avail else F.lit(None)).alias("availability"),
            (_col(df, ratings).cast("double") if ratings else F.lit(None).cast("double")).alias("ratings"),
            # --- lineage / ad-hoc columns: retained intentionally for analysis; ---
            # --- no Gold mart or dashboard reads these today.                   ---
            (udf_first(_col(df, supplier_list)) if supplier_list else F.lit(None).cast("string")).alias("primary_supplier"),
            (_col(df, main_cat).cast("string") if main_cat else F.lit(None).cast("string")).alias("raw_main_category"),
            (_col(df, sub_cat).cast("string")  if sub_cat  else F.lit(None).cast("string")).alias("raw_sub_category"),
            cat[0].alias("canonical_l1"),
            cat[1].alias("canonical_l2"),
            url_c.alias("product_url"),
            # Composite key for cross-retailer fuzzy matching (Brand_Product_Size).
            F.trim(F.concat_ws(" ",
                brand_c.cast("string"),
                name_c.cast("string"),
                (_col(df, pkg).cast("string") if pkg else F.lit("")),
            )).alias("brand_product_size"),
            F.col("_snapshot_date").alias("snapshot_date"),
            F.col("_source_file"),
            F.col("_ingest_ts"),
        )

    # ----------------------------------------------------------------------- #
    #  PER-RETAILER CONFORMED STREAMING VIEWS
    # ----------------------------------------------------------------------- #

    @dlt.view(name="norm_aldi")
    def norm_aldi():
        df = dlt.read_stream("bronze.raw_aldi")
        # Aldi has no Brand/SKU and no Availability; size is embedded in the name.
        # Sub Category carries the usable taxonomy; Main Category is just 'groceries'.
        df = df.withColumn("_aldi_cat", udf_aldi_cat(_col(df, "Sub Category")))
        return _conform(
            df, retailer="Aldi",
            name="Product", price="Price", unit_price="Unit Price",
            main_cat="_aldi_cat", url="Product Page",
        )

    @dlt.view(name="norm_coles")
    def norm_coles():
        df = dlt.read_stream("bronze.raw_coles")
        df = df.withColumn("_unit_size", udf_clean_usize(_col(df, "Unit Size")))
        df = df.withColumn("_category",  udf_clean_cat(_col(df, "Category")))
        # Liquor is duplicated under other categories / out of scope (notebook).
        df = df.filter(F.col("_category") != "Liquor")
        return _conform(
            df, retailer="Coles",
            name="Product Name", sku="SKU", brand="Brand",
            price="Current Price", prev_price="Previous Price",
            unit_price="Price per unit", pkg="_unit_size",
            on_special_cols=("On Special",), avail="Availability",
            supplier_list="Suppliers", main_cat="_category", url="URL",
        )

    @dlt.view(name="norm_iga")
    def norm_iga():
        df = dlt.read_stream("bronze.raw_iga")
        df = df.withColumn("_product_name", udf_clean_iga_pn(_col(df, "Product Name")))
        df = df.withColumn("_sku",          udf_clean_iga_sk(_col(df, "SKU")))
        df = df.withColumn("_category",     udf_clean_cat(_col(df, "Main Category")))
        return _conform(
            df, retailer="IGA",
            name="_product_name", sku="_sku", price="Price",
            unit_price="Price per unit",
            main_cat="_category", sub_cat="Sub Category", url="Product URL",
        )

    @dlt.view(name="norm_woolworths")
    def norm_woolworths():
        df = dlt.read_stream("bronze.raw_woolworths")
        # Collapse the multi-value Department to ONE primary category so the grain
        # stays retailer x product x date (no explode). Products that are purely
        # Tobacco/Liquor yield NULL and are dropped (login-wall / BWS scope).
        df = (df
              .withColumn("_dept", udf_wow_primary(_col(df, "Department")))
              .filter(F.col("_dept").isNotNull())
              .withColumn("_pkg",  udf_clean_pkg(_col(df, "Package Size"))))
        return _conform(
            df, retailer="Woolworths",
            name="Product Name", sku="SKU", brand="Brand", price="Price",
            unit_price="Price per unit", pkg="_pkg",
            on_special_cols=("Specials",), avail="Availability", ratings="Ratings",
            main_cat="_dept", url="Product URL",
        )

    # ----------------------------------------------------------------------- #
    #  CONFORMED FACT WITH DQ EXPECTATIONS + QUARANTINE FLAG
    # ----------------------------------------------------------------------- #

    @dlt.table(
        name="silver.fact_price",
        comment="Conformed daily price fact across all retailers (grain: retailer × product × date).",
        table_properties={"quality": "silver"},
    )
    @dlt.expect_or_drop("valid_retailer",       "retailer IN ('Aldi','Coles','IGA','Woolworths')")
    @dlt.expect_or_drop("has_product_name",     "product_name IS NOT NULL AND product_name <> ''")
    @dlt.expect("price_present_or_unavailable", "price_aud IS NOT NULL OR availability = 'unavailable'")
    def fact_price():
        unioned = (
            dlt.read_stream("norm_aldi")
            .unionByName(dlt.read_stream("norm_coles"))
            .unionByName(dlt.read_stream("norm_iga"))
            .unionByName(dlt.read_stream("norm_woolworths"))
        )
        dq = udf_classify(
            F.col("retailer"), F.col("product_name"), F.col("price_aud"),
            F.col("availability"), F.col("unit_price_value"),
        )
        return (
            unioned
            .withColumn("_dq",            dq)
            .withColumn("dq_action",      F.col("_dq")[0])
            .withColumn("dq_reason",      F.col("_dq")[1])
            .filter(F.col("dq_action") != "drop")
            .withColumn("is_quarantined", F.col("dq_action") == "quarantine")
            .withColumn("product_sk",     F.md5(F.concat_ws("|", "retailer", "source_sku")))
            .withColumn("category_sk",    F.md5(F.concat_ws("|", "canonical_l1", "canonical_l2")))
            .drop("_dq")
        )

    @dlt.table(name="silver.fact_price_quarantine",
               comment="Rows flagged by DQ rules — kept for review, excluded from BI marts.")
    def fact_price_quarantine():
        return dlt.read_stream("fact_price").filter(F.col("is_quarantined"))

    # ----------------------------------------------------------------------- #
    #  DIMENSIONS
    # ----------------------------------------------------------------------- #

    dlt.create_streaming_table(
        name="silver.dim_product",
        comment="SCD2 product dimension — tracks attribute changes over time.",
    )
    dlt.apply_changes(
        target="dim_product",
        source="fact_price",
        keys=["product_sk"],
        sequence_by=F.col("snapshot_date"),
        stored_as_scd_type=2,
        track_history_column_list=["product_name", "brand", "package_size_raw",
                                   "canonical_l1", "canonical_l2"],
        except_column_list=["price_aud", "previous_price_aud", "on_special",
                            "availability", "is_quarantined", "dq_action", "dq_reason",
                            "_ingest_ts", "_source_file"],
    )

    @dlt.table(name="silver.dim_category", comment="Canonical two-level category hierarchy.")
    def dim_category():
        return (
            dlt.read_stream("fact_price")
            .select("category_sk", "canonical_l1", "canonical_l2")
            .dropDuplicates(["category_sk"])
        )

    @dlt.table(name="silver.dim_retailer", comment="Static retailer reference dimension.")
    def dim_retailer():
        rows = [
            ("Aldi",        "ALDI Australia",   "Discounter"),
            ("Coles",       "Coles Group",      "Full-service"),
            ("IGA",         "Metcash (IGA)",    "Independent"),
            ("Woolworths",  "Woolworths Group", "Full-service"),
        ]
        schema = StructType([
            StructField("retailer",       StringType()),
            StructField("parent_company", StringType()),
            StructField("retailer_type",  StringType()),
        ])
        return (
            spark.createDataFrame(rows, schema)  # noqa: F821
            .withColumn("retailer_sk", F.md5(F.col("retailer")))
        )

    @dlt.table(name="silver.dim_date", comment="Generated calendar dimension.")
    def dim_date():
        base = (
            dlt.read("fact_price")
            .select(F.min("snapshot_date").alias("mn"),
                    F.max("snapshot_date").alias("mx"))
            .collect()[0]
        )
        return (
            spark.sql(  # noqa: F821
                f"SELECT explode(sequence(to_date('{base['mn']}'), "
                f"to_date('{base['mx']}'), interval 1 day)) AS date_day"
            )
            .withColumn("date_sk",     F.date_format("date_day", "yyyyMMdd").cast("int"))
            .withColumn("year",        F.year("date_day"))
            .withColumn("month",       F.month("date_day"))
            .withColumn("day",         F.dayofmonth("date_day"))
            .withColumn("day_of_week", F.dayofweek("date_day"))
            .withColumn("is_weekend",  F.dayofweek("date_day").isin(1, 7))
        )
