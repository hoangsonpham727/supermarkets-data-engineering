"""Unit tests for the data-quality rule predicates used by the Silver layer."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipelines.silver import (  # noqa: E402
    check_price_present,
    check_price_unit_consistency,
    check_retailer,
    classify_row,
)


class TestDropRules:
    def test_invalid_retailer_dropped(self):
        action, reason = classify_row("Costco", "Milk 2L", 3.0, "available", 1.5)
        assert action == "drop" and "invalid_retailer" in reason

    def test_missing_name_dropped(self):
        action, reason = classify_row("Coles", "", 3.0, "available", None)
        assert action == "drop" and reason == "missing_product_name"

    def test_null_price_available_dropped(self):
        action, reason = classify_row("IGA", "Bread", None, "available", None)
        assert action == "drop" and reason == "null_price_for_available_item"


class TestQuarantineRules:
    def test_wow_unavailable_null_price_quarantined_not_dropped(self):
        # The signature WOW case: price is null because the item is unavailable.
        action, reason = classify_row(
            "Woolworths", "Mighty Soft Wholemeal", None, "unavailable", None)
        assert action == "quarantine" and reason == "unavailable_no_price"

    def test_aldi_price_unit_anomaly_quarantined(self):
        # paracetamol: Price=69.0 but unit price implies ~0.03/tablet -> mismatch
        action, reason = classify_row(
            "Aldi", "Hedanol Paracetamol 20pk", 69.0, "unknown", 0.03)
        assert action == "quarantine" and "price_unit_mismatch" in reason

    def test_out_of_range_price_quarantined(self):
        action, reason = classify_row("Coles", "Gold Bar", 5000.0, "available", 5000.0)
        assert action == "quarantine" and "price_out_of_range" in reason


class TestPass:
    def test_clean_row_passes(self):
        action, reason = classify_row("Coles", "Conditioner", 17.0, "available", 2.58)
        assert action == "pass" and reason is None


class TestIndividualRules:
    def test_check_retailer(self):
        assert check_retailer("Aldi")[0] is True
        assert check_retailer("Foo")[0] is False

    def test_check_price_present_unavailable_ok(self):
        assert check_price_present(None, "unavailable")[0] is True
        assert check_price_present(None, "available")[0] is False

    def test_price_unit_consistency_ok_when_close(self):
        assert check_price_unit_consistency(17.0, 2.58)[0] is True
        assert check_price_unit_consistency(69.0, 0.03)[0] is False
