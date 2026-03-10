"""
General boolean gates — pre-checks that apply before any specific rule item.

These are Pass 1 (Hard Legal Exclusion) from the spec.
"""
from engine.models import Candidate, RuleResult


def confidence_from_margin_m(margin_m: float) -> float:
    """
    Confidence score based on margin above threshold in meters.
    >500m = 0.95, 200-500m = 0.85, 50-200m = 0.75, <50m = 0.65
    """
    if margin_m > 500:
        return 0.95
    elif margin_m > 200:
        return 0.85
    elif margin_m > 50:
        return 0.75
    else:
        return 0.65


def check_general_requirements(candidate: Candidate) -> RuleResult:
    """
    Check general boolean gates that apply to ALL rule items.
    If this fails, no further rule evaluation needed.
    """
    reasons = []
    evidence_needed = []
    failed = False

    if not candidate.zoning_ok:
        reasons.append("FAIL: Premises not capable of pharmacy use under zoning")
        failed = True

    if not candidate.accessible_to_public:
        reasons.append("FAIL: Premises not accessible to public")
        failed = True

    if not candidate.legal_right_to_occupy:
        reasons.append("FAIL: No legal right to occupy")
        failed = True

    if not candidate.can_open_within_6_months:
        reasons.append("FAIL: Cannot open within 6 months")
        failed = True

    # For Stage A screening, we assume these are OK and note evidence needed
    if not failed:
        reasons.append("General requirements assumed OK (Stage A screening)")
        evidence_needed.extend([
            "Verify zoning permits pharmacy use",
            "Verify public accessibility",
            "Confirm lease/right to occupy available",
            "Confirm can open within 6 months of approval",
        ])

    return RuleResult(
        item="General",
        passed=not failed,
        reasons=reasons,
        evidence_needed=evidence_needed,
        confidence=0.5 if not failed else 0.0,  # Low confidence — these are assumptions
    )


def is_new_pharmacy_item(item: str) -> bool:
    """Items 130, 131, 132 are new-pharmacy items where supermarket access rule applies."""
    return item in ("Item 130", "Item 131", "Item 132")


def check_supermarket_access(candidate: Candidate) -> bool:
    """
    For new-pharmacy items (130, 131, 132): must NOT be directly accessible
    from within a supermarket. Returns True if OK (not inside supermarket).
    """
    return not candidate.direct_access_from_supermarket
