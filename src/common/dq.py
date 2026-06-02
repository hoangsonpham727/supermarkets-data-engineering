"""Data-quality rule predicates, kept pure-Python so they are unit-testable and
can be reused both as Spark expressions (Silver DLT expectations) and in tests.

Each rule returns a ``(passed: bool, reason: str|None)`` tuple. The Silver layer
uses these to decide drop / quarantine / pass, mirroring how DLT ``expect_*``
annotations behave — but expressed in a way we can assert on locally.
"""

from __future__ import annotations

from typing import Optional

VALID_RETAILERS = {"Aldi", "Coles", "IGA", "Woolworths"}

# Sanity bounds for a grocery line price (AUD). Outside this band is suspicious.
PRICE_MIN = 0.0
PRICE_MAX = 1000.0


def check_retailer(retailer: Optional[str]) -> tuple[bool, Optional[str]]:
    if retailer in VALID_RETAILERS:
        return True, None
    return False, f"invalid_retailer:{retailer!r}"


def check_product_name(name: Optional[str]) -> tuple[bool, Optional[str]]:
    if name and str(name).strip():
        return True, None
    return False, "missing_product_name"


def check_price_present(price: Optional[float],
                        availability: str) -> tuple[bool, Optional[str]]:
    """An *available* product must have a positive price; an unavailable one may
    legitimately have a null price (WOW 'Unavailable' rows) and is not dropped.
    """
    if price is None:
        if availability == "unavailable":
            return True, None          # acceptable, but will be quarantined downstream
        return False, "null_price_for_available_item"
    if price <= PRICE_MIN:
        return False, f"non_positive_price:{price}"
    return True, None


def check_price_range(price: Optional[float]) -> tuple[bool, Optional[str]]:
    if price is None:
        return True, None
    if price > PRICE_MAX:
        return False, f"price_out_of_range:{price}"
    return True, None


def check_price_unit_consistency(price: Optional[float],
                                 unit_price_value: Optional[float],
                                 tolerance_ratio: float = 100.0
                                 ) -> tuple[bool, Optional[str]]:
    """Flag rows where the headline price and the unit price disagree wildly.

    Catches the Aldi-style anomaly where a 20pk of tablets shows ``Price=69.0``
    while the unit price implies cents — a likely cents/dollars data-entry error.
    We don't drop these (the unit price may still be useful); we quarantine.
    """
    if price is None or unit_price_value is None or unit_price_value <= 0:
        return True, None
    ratio = price / unit_price_value
    if ratio > tolerance_ratio or ratio < (1.0 / tolerance_ratio):
        return False, f"price_unit_mismatch:ratio={ratio:.1f}"
    return True, None


# Severity model used by Silver:
#   "drop"        -> row is structurally unusable, excluded from Silver
#   "quarantine"  -> row is kept but flagged out of the BI marts
#   "pass"        -> clean
DROP_RULES = (check_retailer, check_product_name)


def classify_row(retailer, product_name, price, availability,
                 unit_price_value) -> tuple[str, Optional[str]]:
    """Return ``(action, reason)`` where action is pass/quarantine/drop."""
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

    # Unavailable items carry no usable price -> quarantine from price marts.
    if price is None and availability == "unavailable":
        return "quarantine", "unavailable_no_price"

    return "pass", None
