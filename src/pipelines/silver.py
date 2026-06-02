"""Silver layer — conform the four heterogeneous feeds into one model + dimensions.

Lakeflow Declarative Pipeline (DLT). Responsibilities:
  1. Map each retailer's native columns to a single conformed schema.
  2. Apply parsing rules (unit price, list strings, brand, surrogate SKU).
  3. Enforce data-quality expectations (drop / quarantine / pass).
  4. Build conformed dimensions: dim_product (SCD2), dim_category, dim_retailer,
     dim_date.

IMPORTANT — UDF serialisation:
  Spark serialises UDFs via cloudpickle and ships them to *worker* nodes.  Workers
  only have the Databricks runtime on their PYTHONPATH — they cannot see src.common.
  All UDF functions below are therefore self-contained: no imports from src.common,
  and any stdlib imports (re, ast, hashlib) are done INSIDE the function body so
  they are resolved at execution time on the worker, not captured in the closure.
"""

import re as _re

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, StringType, StructField, StructType,
)

# =========================================================================== #
#  SELF-CONTAINED UDF FUNCTIONS
#  No references to src.common — only stdlib used, imported inside each body.
# =========================================================================== #

def _clean_price(raw):
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


def _parse_unit_price(raw):
    """Return (value_aud, measure) from a free-text unit-price string.

    Handles the full variety seen across the four feeds:
      "$11.16 per 100g"  |  "3c per tablet"  |  "$2.58 per 100mL"
      "2.46 per litre"   |  "5.50 per kg"    |  "$24.75 per 1Kg"
    """
    if not raw:
        return (None, None)
    import re
    text = str(raw).strip()
    parts = re.split(r"\s*(?:per|/)\s*", text, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return (None, None)
    left, right = parts[0].strip(), parts[1].strip()

    has_dollar = "$" in left
    is_cents = bool(re.search(r"\dc\b", left)) or (
        left.lower().endswith("c") and not has_dollar
    )
    num = re.search(r"[\d.]+", left)
    if not num:
        return (None, None)
    value = float(num.group())
    if is_cents and not has_dollar:
        value = round(value / 100.0, 4)

    qty_m = re.match(r"([\d.]+)?\s*([a-zA-Z]+)", right)
    if not qty_m:
        return (value, None)
    qty = float(qty_m.group(1)) if qty_m.group(1) else 1.0
    unit_raw = qty_m.group(2).lower()

    _weight  = {"mg", "g", "kg"}
    _volume  = {"ml", "millilitre", "l", "litre", "liter"}
    _count   = {"each","ea","tablet","tablets","capsule","capsules","pack","pk",
                "sheet","sheets","wash","washes","serve","serves","unit","units",
                "item","items","piece","pieces"}

    if unit_raw in _weight:
        unit = unit_raw
    elif unit_raw in _volume:
        unit = "ml" if unit_raw in {"ml", "millilitre"} else "l"
    elif unit_raw in _count:
        unit = "each"
    else:
        return (value, None)

    qty_tok = "" if qty == 1.0 else (str(int(qty)) if qty == int(qty) else str(qty))
    return (value, f"per_{qty_tok}{unit}")


def _norm_unit_price(raw):
    """Collapse any unit-price string to a comparable base (per kg, per L, per each)."""
    if not raw:
        return (None, None)
    import re
    text = str(raw).strip()
    parts = re.split(r"\s*(?:per|/)\s*", text, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return (None, None)
    left, right = parts[0].strip(), parts[1].strip()

    has_dollar = "$" in left
    is_cents = bool(re.search(r"\dc\b", left)) or (
        left.lower().endswith("c") and not has_dollar
    )
    num = re.search(r"[\d.]+", left)
    if not num:
        return (None, None)
    value = float(num.group())
    if is_cents and not has_dollar:
        value = round(value / 100.0, 4)

    qty_m = re.match(r"([\d.]+)?\s*([a-zA-Z]+)", right)
    if not qty_m:
        return (None, None)
    qty = float(qty_m.group(1)) if qty_m.group(1) else 1.0
    if qty == 0:
        return (None, None)
    unit_raw = qty_m.group(2).lower()

    _wt = {"mg": 0.001, "g": 1.0, "kg": 1000.0}
    _vl = {"ml": 1.0, "millilitre": 1.0, "l": 1000.0, "litre": 1000.0, "liter": 1000.0}
    _ct = {"each","ea","tablet","tablets","capsule","capsules","pack","pk",
           "sheet","sheets","wash","washes","serve","serves","unit","units",
           "item","items","piece","pieces"}

    if unit_raw in _wt:
        grams = _wt[unit_raw] * qty
        return (round(value / grams * 1000.0, 4), "per_kg")
    if unit_raw in _vl:
        ml = _vl[unit_raw] * qty
        return (round(value / ml * 1000.0, 4), "per_l")
    if unit_raw in _ct:
        return (round(value / qty, 4), "per_each")
    return (None, None)


def _first_from_list(raw):
    """Return first element of a stringified Python list, or 'unknown'."""
    if not raw:
        return "unknown"
    import ast, re  # noqa: E401
    text = str(raw).strip()
    if not text or text in {"[]", "nan", "None", "null"}:
        return "unknown"
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple)):
            items = [str(x).strip() for x in parsed if str(x).strip()]
            return items[0] if items else "unknown"
        return str(parsed).strip() or "unknown"
    except (ValueError, SyntaxError):
        inner = text.strip("[]")
        items = re.split(r"',\s*'|\",\s*\"", inner)
        items = [i.strip(" '\"") for i in items if i.strip(" '\"")]
        return items[0] if items else "unknown"


def _make_surrogate_sku(retailer, product_name, product_url):
    """Deterministic 16-char surrogate key for feeds without a SKU (Aldi)."""
    import hashlib
    basis = "|".join([
        (retailer or "").strip().lower(),
        (product_name or "").strip().lower(),
        (product_url or "").strip().lower(),
    ])
    return hashlib.md5(basis.encode("utf-8")).hexdigest()[:16]


def _extract_brand(product_name):
    """Best-effort brand extraction from a product name (heuristic)."""
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


def _normalize_availability(raw):
    """Map free-text availability to available / unavailable / unknown."""
    if raw is None:
        return "unknown"
    _map = {
        "available": "available",
        "in stock": "available",
        "unavailable": "unavailable",
        "out of stock": "unavailable",
        "sold out": "unavailable",
    }
    return _map.get(str(raw).strip().lower(), "unknown")


def _to_special_bool(a, b):
    """True when any on-special signal is truthy (flag or non-empty description)."""
    _falsy = {"nan", "none", "null", "false", "0", "no", "f", "n"}
    for f in (a, b):
        if f is None:
            continue
        s = str(f).strip().lower()
        if s and s not in _falsy:
            return True
    return False


# Category map is a plain dict of primitives — cloudpickle can serialise it fine.
# Keys are lowercased, space-normalised tokens from any retailer's category field.
# Includes both raw scraped forms and the cleaned forms produced by the notebook.
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
    "health & beauty":          ("Health & Beauty", "Health & Beauty"),
    "health":                    ("Health & Beauty", "Health"),
    "beauty":                    ("Health & Beauty", "Health & Beauty"),        # Aldi raw
    "personal care":             ("Health & Beauty", "Personal Care"),
    # ---- Pantry ----
    "groceries":                 ("Pantry",          "Groceries"),
    "pantry":                    ("Pantry",          "Pantry"),
    "international foods":       ("Pantry",          "International Foods"),
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


def _canonical_category(raw_main, raw_sub=None):
    """Return [canonical_l1, canonical_l2] from raw retailer category strings.

    Tries both dash-normalised and space-normalised key forms so raw scraped
    values ('health-beauty') and notebook-cleaned values ('Health Beauty')
    both resolve correctly.
    """
    import re
    for token in (raw_sub, raw_main):
        if not token:
            continue
        base = str(token).strip().lower()
        # Try key with spaces (notebook-cleaned form)
        if base in _CATEGORY_MAP:
            return list(_CATEGORY_MAP[base])
        # Try key with dashes (raw scraped form)
        dashed = re.sub(r"\s+", "-", base)
        if dashed in _CATEGORY_MAP:
            return list(_CATEGORY_MAP[dashed])
    l1 = str(raw_main).strip().title() if raw_main else "Other"
    return [l1, l1]


_VALID_RETAILERS = {"Aldi", "Coles", "IGA", "Woolworths"}


def _classify_row(retailer, product_name, price, availability, unit_price_value):
    """Return [action, reason] — action is pass / quarantine / drop."""
    if retailer not in _VALID_RETAILERS:
        return ["drop", f"invalid_retailer:{retailer!r}"]
    if not (product_name and str(product_name).strip()):
        return ["drop", "missing_product_name"]
    if price is None:
        if availability == "unavailable":
            return ["quarantine", "unavailable_no_price"]
        return ["drop", "null_price_for_available_item"]
    if price <= 0:
        return ["drop", f"non_positive_price:{price}"]
    if price > 1000:
        return ["quarantine", f"price_out_of_range:{price}"]
    if unit_price_value is not None and unit_price_value > 0:
        ratio = price / unit_price_value
        if ratio > 100 or ratio < 0.01:
            return ["quarantine", f"price_unit_mismatch:ratio={ratio:.1f}"]
    return ["pass", None]


# =========================================================================== #
#  NOTEBOOK-DERIVED CLEANING FUNCTIONS  (self-contained, no external imports)
# =========================================================================== #

# ---- Aldi: explicit sub-category remapping from notebook ---- #
_ALDI_CAT_MAP = {
    "fresh product":      "Dairy Eggs & Fridge",
    "beauty":             "Health & Beauty",
    "health":             "Health & Beauty",
    "laundry household":  "Household",
    "freezer":            "Frozen",
}

def _aldi_category(raw):
    """Map Aldi raw sub-category slugs to cleaned names, then title-case."""
    if not raw:
        return "Other"
    s = str(raw).strip().lower()
    return _ALDI_CAT_MAP.get(s, str(raw).strip()).title()


# ---- WOW: explode multi-category Department strings ---- #
def _parse_wow_departments(raw):
    """Parse WOW Department stringified list → clean array of category strings.

    Fixes comma-containing category names BEFORE splitting so they don't get
    torn apart:
        "['Meat, Seafood & Deli', 'Dairy, Eggs & Fridge']"
        → ['Meat Seafood & Deli', 'Dairy Eggs & Fridge']
    """
    import ast
    if not raw:
        return ["NOT LISTED"]
    text = str(raw).strip()
    # Replace commas INSIDE known multi-word category names before parsing.
    text = text.replace("Meat, Seafood & Deli",  "Meat Seafood & Deli")
    text = text.replace("Dairy, Eggs & Fridge",  "Dairy Eggs & Fridge")
    try:
        parsed = ast.literal_eval(text)
        items = [str(x).strip().replace("'", "") for x in parsed if str(x).strip()]
    except (ValueError, SyntaxError):
        items = [text.strip("[]'\" ")]
    items = [i for i in items if i]
    return items if items else ["NOT LISTED"]


# ---- WOW: normalize Package Size ---- #
def _clean_package_size(raw):
    """6pk → '6 pack', lowercase 'l' suffix → 'L'  (e.g. 500l → 500L)."""
    if not raw:
        return ""
    import re
    s = str(raw)
    s = re.sub(r"(\d)pk\b", r"\1 pack", s)   # 6pk → 6 pack
    s = re.sub(r"(?<=\d)l\b", "L", s)         # 500l → 500L
    return s.strip()


# ---- Coles: normalize Unit Size ---- #
def _clean_unit_size(raw):
    """'1 each' → 'each', lowercase, lowercase l → L."""
    if not raw:
        return ""
    import re
    s = str(raw).strip().replace("1 each", "each").lower()
    s = re.sub(r"(?<=\d)l\b", "L", s)
    return s


# ---- Coles/IGA: clean category text ---- #
def _clean_category_text(raw):
    """Replace '--' and '-' with spaces and title-case (matches notebook output)."""
    if not raw:
        return "Other"
    return str(raw).replace("--", " ").replace("-", " ").title().strip()


# ---- IGA: clean product names ---- #
def _clean_iga_product_name(raw):
    """Apply the three IGA regex fixes from the notebook:
       'gm' → 'g' at end, '8pk' → '8 pack', '600l' → '600L'.
    """
    if not raw:
        return ""
    import re
    s = str(raw)
    s = re.sub(r"gm$", "g", s)               # trailing 'gm' → 'g'
    s = re.sub(r"(?<=\d)pk", " pack", s)      # '8pk' → '8 pack'
    s = re.sub(r"(?<=\d)l", "L", s)           # '600l' → '600L'
    return s


# ---- IGA: clean SKU (float → string, strip '.0') ---- #
def _clean_iga_sku(raw):
    """IGA SKUs are barcodes stored as floats (e.g. '9300675009775.0').
    Strip the spurious '.0' decimal suffix."""
    if not raw:
        return ""
    s = str(raw).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


# =========================================================================== #
#  UDF REGISTRATIONS
# =========================================================================== #

_unit_schema = StructType([
    StructField("value",   DoubleType()),
    StructField("measure", StringType()),
])
_cat_schema = StructType([
    StructField("l1", StringType()),
    StructField("l2", StringType()),
])
_dq_schema = StructType([
    StructField("action", StringType()),
    StructField("reason", StringType()),
])

udf_price        = F.udf(_clean_price,             DoubleType())
udf_unit         = F.udf(_parse_unit_price,        _unit_schema)
udf_norm_unit    = F.udf(_norm_unit_price,         _unit_schema)
udf_first        = F.udf(_first_from_list,         StringType())
udf_sku          = F.udf(_make_surrogate_sku,      StringType())
udf_brand        = F.udf(_extract_brand,           StringType())
udf_avail        = F.udf(_normalize_availability,  StringType())
udf_special      = F.udf(_to_special_bool,         BooleanType())
udf_cat          = F.udf(_canonical_category,      ArrayType(StringType()))
udf_classify     = F.udf(_classify_row,            ArrayType(StringType()))
# Notebook-derived cleaning UDFs
udf_aldi_cat     = F.udf(_aldi_category,           StringType())
udf_wow_depts    = F.udf(_parse_wow_departments,   ArrayType(StringType()))
udf_clean_pkg    = F.udf(_clean_package_size,      StringType())
udf_clean_usize  = F.udf(_clean_unit_size,         StringType())
udf_clean_cat    = F.udf(_clean_category_text,     StringType())
udf_clean_iga_pn = F.udf(_clean_iga_product_name,  StringType())
udf_clean_iga_sk = F.udf(_clean_iga_sku,           StringType())

# =========================================================================== #
#  COLUMN RESOLVER  (_col)
# =========================================================================== #

_BAD_CHARS = _re.compile(r"[ ,;{}\(\)\n\t=]+")


def _col(df, name):
    """Return the column expression for *name*, trying the Bronze-sanitised form
    (spaces → underscores) if the original name isn't found.  Falls back to a
    typed NULL so the downstream select always has the expected column present.
    """
    if name in df.columns:
        return F.col(f"`{name}`")
    # Bronze renames e.g. "Unit Price" → "Unit_Price"; try that automatically.
    sanitised = _BAD_CHARS.sub("_", name)
    if not name.startswith("_"):          # preserve metadata cols like _snapshot_date
        sanitised = sanitised.strip("_")
    if sanitised in df.columns:
        return F.col(f"`{sanitised}`")
    return F.lit(None)


# =========================================================================== #
#  CONFORMED SCHEMA BUILDER
# =========================================================================== #

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

    _no_unit = F.struct(
        F.lit(None).cast("double").alias("value"),
        F.lit(None).cast("string").alias("measure"),
    )
    unit = udf_unit(_col(df, unit_price)) if unit_price else _no_unit
    norm = udf_norm_unit(_col(df, unit_price)) if unit_price else _no_unit

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
        (udf_price(_col(df, price))     if price      else F.lit(None).cast("double")).alias("price_aud"),
        (udf_price(_col(df, prev_price)) if prev_price else F.lit(None).cast("double")).alias("previous_price_aud"),
        unit["value"].alias("unit_price_value"),
        unit["measure"].alias("unit_price_measure"),
        norm["value"].alias("norm_unit_price"),
        norm["measure"].alias("norm_unit_measure"),
        (_col(df, pkg).cast("string") if pkg else F.lit(None).cast("string")).alias("package_size_raw"),
        udf_special(sp_args[0], sp_args[1]).alias("on_special"),
        udf_avail(_col(df, avail) if avail else F.lit(None)).alias("availability"),
        (_col(df, ratings).cast("double") if ratings else F.lit(None).cast("double")).alias("ratings"),
        (udf_first(_col(df, supplier_list)) if supplier_list else F.lit(None).cast("string")).alias("primary_supplier"),
        (_col(df, main_cat).cast("string") if main_cat else F.lit(None).cast("string")).alias("raw_main_category"),
        (_col(df, sub_cat).cast("string")  if sub_cat  else F.lit(None).cast("string")).alias("raw_sub_category"),
        cat[0].alias("canonical_l1"),
        cat[1].alias("canonical_l2"),
        url_c.alias("product_url"),
        # Composite key for cross-retailer fuzzy matching (notebook: Brand_Product_Size).
        F.trim(F.concat_ws(" ",
            brand_c.cast("string"),
            name_c.cast("string"),
            (_col(df, pkg).cast("string") if pkg else F.lit("")),
        )).alias("brand_product_size"),
        F.col("_snapshot_date").alias("snapshot_date"),
        F.col("_source_file"),
        F.col("_ingest_ts"),
    )


# =========================================================================== #
#  PER-RETAILER CONFORMED STREAMING VIEWS
#  Column names use the original CSV headers; _col() auto-resolves sanitised
#  forms produced by Bronze (e.g. "Unit Price" → "Unit_Price").
# =========================================================================== #

@dlt.view(name="norm_aldi")
def norm_aldi():
    df = dlt.read_stream("bronze.raw_aldi")
    # Apply notebook category remapping before conforming.
    df = df.withColumn("_aldi_cat", udf_aldi_cat(_col(df, "Sub Category")))
    return _conform(
        df, retailer="Aldi",
        name="Product", price="Price", unit_price="Unit Price",
        main_cat="_aldi_cat", url="Product Page",
    )


@dlt.view(name="norm_coles")
def norm_coles():
    df = dlt.read_stream("bronze.raw_coles")
    # Clean Unit Size: '1 each' → 'each', lowercase, l → L (notebook).
    df = df.withColumn("_unit_size", udf_clean_usize(_col(df, "Unit Size")))
    # Clean Category: replace dashes with spaces, title-case (notebook).
    df = df.withColumn("_category",  udf_clean_cat(_col(df, "Category")))
    # Filter: remove Liquor (duplicated in other categories — notebook logic).
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
    # Clean product names: gm→g, pk→pack, l→L (notebook regex rules).
    df = df.withColumn("_product_name", udf_clean_iga_pn(_col(df, "Product Name")))
    # Clean SKU: strip spurious '.0' from float barcodes (notebook).
    df = df.withColumn("_sku",          udf_clean_iga_sk(_col(df, "SKU")))
    # Clean category: replace dashes with spaces, title-case (notebook).
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
    # Parse Department list and explode: one row per category per product.
    # Fix comma-containing names first ('Meat, Seafood & Deli' → 'Meat Seafood & Deli')
    # so the split is correct (notebook logic).
    df = (df
          .withColumn("_dept_array", udf_wow_depts(_col(df, "Department")))
          .withColumn("_dept",       F.explode(F.col("_dept_array")))
          # Filter: Tobacco is behind a login wall; Liquor is BWS stock (notebook).
          .filter(~F.col("_dept").isin("Tobacco Product", "Liquor"))
          # Replace empty category with 'NOT LISTED' (notebook).
          .withColumn("_dept",       F.when(F.col("_dept") == "", "NOT LISTED")
                                      .otherwise(F.col("_dept")))
          # Normalize Package Size: pk→pack, l→L (notebook).
          .withColumn("_pkg",        udf_clean_pkg(_col(df, "Package Size")))
    )
    return _conform(
        df, retailer="Woolworths",
        name="Product Name", sku="SKU", brand="Brand", price="Price",
        unit_price="Price per unit", pkg="_pkg",
        on_special_cols=("Specials",), avail="Availability", ratings="Ratings",
        main_cat="_dept", url="Product URL",
    )


# =========================================================================== #
#  CONFORMED FACT WITH DQ EXPECTATIONS + QUARANTINE FLAG
# =========================================================================== #

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
        .withColumn("_dq",           dq)
        .withColumn("dq_action",     F.col("_dq")[0])
        .withColumn("dq_reason",     F.col("_dq")[1])
        .filter(F.col("dq_action") != "drop")
        .withColumn("is_quarantined", F.col("dq_action") == "quarantine")
        .withColumn("product_sk",    F.md5(F.concat_ws("|", "retailer", "source_sku")))
        .withColumn("category_sk",   F.md5(F.concat_ws("|", "canonical_l1", "canonical_l2")))
        .drop("_dq")
    )


@dlt.table(name="silver.fact_price_quarantine",
           comment="Rows flagged by DQ rules — kept for review, excluded from BI marts.")
def fact_price_quarantine():
    return dlt.read_stream("fact_price").filter(F.col("is_quarantined"))


# =========================================================================== #
#  DIMENSIONS
# =========================================================================== #

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
