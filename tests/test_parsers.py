"""Unit tests for the source-feed parsing helpers.

Run locally with:  pytest -q
These cover the real edge-cases seen in the four 2021-04-23 snapshots.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipelines.silver import (  # noqa: E402
    canonical_category,
    clean_price,
    extract_brand,
    first_from_list,
    make_surrogate_sku,
    normalize_availability,
    normalize_unit_price,
    parse_list_string,
    parse_unit_price,
    primary_wow_department,
    to_special_bool,
    unit_price_all,
)


# --------------------------- parse_unit_price ------------------------------ #
class TestParseUnitPrice:
    def test_dollars_per_100g(self):
        assert parse_unit_price("$11.16 per 100g") == (11.16, "per_100g")

    def test_cents_per_tablet_normalises_to_each(self):
        assert parse_unit_price("3c per tablet") == (0.03, "per_each")

    def test_dollars_per_100ml(self):
        assert parse_unit_price("$2.58 per 100mL") == (2.58, "per_100ml")

    def test_per_litre_no_dollar_sign(self):
        assert parse_unit_price("2.46 per litre") == (2.46, "per_l")

    def test_per_kg(self):
        assert parse_unit_price("5.50 per kg") == (5.5, "per_kg")

    def test_quantity_one_kg_omits_qty_token(self):
        assert parse_unit_price("$24.75 per 1Kg") == (24.75, "per_kg")

    def test_slash_separator(self):
        assert parse_unit_price("$3.00 / 100g") == (3.0, "per_100g")

    def test_garbage_returns_none(self):
        assert parse_unit_price("") == (None, None)
        assert parse_unit_price(None) == (None, None)
        assert parse_unit_price("n/a") == (None, None)


# --------------------------- normalize_unit_price -------------------------- #
class TestNormalizeUnitPrice:
    def test_100ml_to_per_litre(self):
        assert normalize_unit_price("$2.58 per 100mL") == (25.8, "per_l")

    def test_100g_to_per_kg(self):
        assert normalize_unit_price("$11.16 per 100g") == (111.6, "per_kg")

    def test_already_per_kg_unchanged(self):
        assert normalize_unit_price("5.50 per kg") == (5.5, "per_kg")

    def test_litre_unchanged(self):
        assert normalize_unit_price("2.46 per litre") == (2.46, "per_l")

    def test_comparability_two_volumes(self):
        # The whole point: two differently-expressed volume prices become comparable.
        a, _ = normalize_unit_price("$2.58 per 100mL")   # 25.80 / L
        b, _ = normalize_unit_price("$20.00 per 1L")     # 20.00 / L
        assert b < a


# --------------------------- parse_list_string ----------------------------- #
class TestParseListString:
    def test_simple_single_element(self):
        assert parse_list_string("['Bakery']") == ["Bakery"]

    def test_empty_list(self):
        assert parse_list_string("[]") == []
        assert parse_list_string(None) == []

    def test_coles_supplier_list_mixed_quotes(self):
        raw = ("['97446 CARTER & SPENCER GROUP P/L', '939301 FRESH PRODUCE DC (WA)', "
               "\"635235 COSTA'S P/L (VIC DC)\", '414760 ST GEORGE PROD PTY LTD']")
        result = parse_list_string(raw)
        assert result[0] == "97446 CARTER & SPENCER GROUP P/L"
        assert "635235 COSTA'S P/L (VIC DC)" in result
        assert len(result) == 4

    def test_first_from_list_default(self):
        assert first_from_list("[]", default="unknown") == "unknown"
        assert first_from_list("['Bakery']") == "Bakery"


# --------------------------- surrogate sku --------------------------------- #
class TestSurrogateSku:
    def test_stable_across_calls(self):
        a = make_surrogate_sku("Aldi", "Essential Health Paw Paw Ointment ea",
                               "https://aldi.com.au/x")
        b = make_surrogate_sku("Aldi", "Essential Health Paw Paw Ointment ea",
                               "https://aldi.com.au/x")
        assert a == b and len(a) == 16

    def test_differs_by_product(self):
        a = make_surrogate_sku("Aldi", "Product A", "u")
        b = make_surrogate_sku("Aldi", "Product B", "u")
        assert a != b


# --------------------------- misc harmonisation ---------------------------- #
class TestMisc:
    def test_extract_brand(self):
        assert extract_brand("Diet Coke Soft Drink 600ml") == "Diet Coke"
        assert extract_brand("granny smith apple") == "granny"
        assert extract_brand(None) is None

    def test_normalize_availability(self):
        assert normalize_availability("Available") == "available"
        assert normalize_availability("Unavailable") == "unavailable"
        assert normalize_availability(None) == "unknown"
        assert normalize_availability("weird") == "unknown"

    def test_to_special_bool(self):
        assert to_special_bool("TRUE") is True
        assert to_special_bool("Save $2") is True          # WOW special description
        assert to_special_bool("", None, "false") is False
        assert to_special_bool(None) is False

    def test_clean_price(self):
        assert clean_price("$17.00") == 17.0
        assert clean_price("1,234.50") == 1234.5
        assert clean_price("") is None
        assert clean_price(None) is None


# --------------------------- canonical category ---------------------------- #
class TestCanonicalCategory:
    def test_maps_known_tokens(self):
        assert canonical_category("groceries", "health") == ("Health & Beauty", "Health")
        assert canonical_category("fruit-vegetables") == ("Fresh Food", "Fruit & Vegetables")
        assert canonical_category("fruit-&-veg", "fruit") == ("Fresh Food", "Fruit & Vegetables")

    def test_unknown_falls_back_to_titlecase(self):
        l1, l2 = canonical_category("MysteryDept")
        assert l1 == "Mysterydept"

    def test_corrupt_supplier_fragment_buckets_to_other(self):
        # Junk that used to leak from mis-parsed Coles CSV rows must not surface
        # as a category — it should bucket to 'Other'.
        assert canonical_category("234966 Leo'S Import And Distributors P/") == ("Other", "Other")
        assert canonical_category('720488 L\'Oreal Aust P/L"]') == ("Other", "Other")


# --------------------------- single-pass unit price ------------------------ #
class TestUnitPriceAll:
    def test_one_pass_returns_display_and_normalised(self):
        # Same string yields both the display measure and the normalised base.
        assert unit_price_all("$2.58 per 100mL") == (2.58, "per_100ml", 25.8, "per_l")
        assert unit_price_all("$11.16 per 100g") == (11.16, "per_100g", 111.6, "per_kg")

    def test_count_based_normalises_to_each(self):
        assert unit_price_all("3c per tablet") == (0.03, "per_each", 0.03, "per_each")

    def test_unparseable_unit_keeps_value_drops_norm(self):
        value, measure, norm_value, norm_measure = unit_price_all("5.00 per widget")
        assert value == 5.0 and measure is None
        assert norm_value is None and norm_measure is None

    def test_garbage_all_none(self):
        assert unit_price_all(None) == (None, None, None, None)
        assert unit_price_all("n/a") == (None, None, None, None)


# --------------------------- WOW primary department ------------------------ #
class TestPrimaryWowDepartment:
    def test_single_department(self):
        assert primary_wow_department("['Bakery']") == "Bakery"

    def test_picks_first_real_department(self):
        # Comma-containing names are fixed before the split, not torn apart.
        assert primary_wow_department(
            "['Meat, Seafood & Deli', 'Dairy, Eggs & Fridge']"
        ) == "Meat Seafood & Deli"

    def test_excluded_only_is_dropped(self):
        # Purely Tobacco/Liquor products -> None so the caller drops them.
        assert primary_wow_department("['Liquor']") is None
        assert primary_wow_department("['Tobacco Product']") is None

    def test_real_department_wins_over_excluded(self):
        assert primary_wow_department("['Liquor', 'Pantry']") == "Pantry"

    def test_empty_kept_as_not_listed(self):
        assert primary_wow_department("[]") == "NOT LISTED"
        assert primary_wow_department(None) == "NOT LISTED"
