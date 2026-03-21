"""
General Requirements checker — Part 2, Section 10 of the Pharmacy Location Rules.

Every application (new or relocation) must satisfy ALL six general requirements.
Two can be automated; four require manual verification.

References:
  - Handbook V1.10, "General requirements for all applications"
  - Rules Part 2 Section 10 subsections 3(a)–(f)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from engine.context import EvaluationContext
    from engine.models import Candidate


# Threshold: if candidate coords are within this distance (km) of an existing
# pharmacy, we treat it as the *same* approved premises.
_APPROVED_PREMISES_MATCH_KM = 0.05  # 50 metres


@dataclass
class GeneralRequirements:
    """
    Six boolean gates from Part 2, Section 10 of the Rules.

    Automated checks set their value immediately.
    Manual checks start as ``None`` (unknown) and must be explicitly verified.
    """

    # --- Automated checks ---
    not_approved_premises: bool = True
    """3(a): proposed premises are not already approved premises."""

    not_accessible_from_supermarket: bool = True
    """3(f): proposed premises not directly accessible from within a supermarket."""

    # --- Manual verification (None = not yet verified) ---
    legal_right_to_occupy: Optional[bool] = None
    """3(b): applicant has legal right to occupy (lease/ownership)."""

    council_zoning_allows_pharmacy: Optional[bool] = None
    """3(c): premises can be used for pharmacy under local government / state laws."""

    accessible_by_public: Optional[bool] = None
    """3(d): premises would be accessible by the public (not restricted)."""

    ready_within_6_months: Optional[bool] = None
    """3(e): applicant can begin operating within 6 months of Authority recommendation."""

    # Audit trail
    evidence_notes: List[str] = field(default_factory=list)
    """Free-text notes for evidence tracking / manual verification log."""

    # ------------------------------------------------------------------ #
    #  Status helpers
    # ------------------------------------------------------------------ #

    def overall_status(self) -> str:
        """
        Aggregate status across all six checks.

        Returns
        -------
        "passed"
            All six are explicitly ``True``.
        "failed"
            At least one is explicitly ``False``.
        "pending_verification"
            No failures, but one or more manual checks are still ``None``.
        """
        all_values = [
            self.not_approved_premises,
            self.not_accessible_from_supermarket,
            self.legal_right_to_occupy,
            self.council_zoning_allows_pharmacy,
            self.accessible_by_public,
            self.ready_within_6_months,
        ]

        if any(v is False for v in all_values):
            return "failed"
        if any(v is None for v in all_values):
            return "pending_verification"
        return "passed"

    @property
    def failed_checks(self) -> List[str]:
        """Return names of checks that are explicitly ``False``."""
        mapping = {
            "not_approved_premises": self.not_approved_premises,
            "not_accessible_from_supermarket": self.not_accessible_from_supermarket,
            "legal_right_to_occupy": self.legal_right_to_occupy,
            "council_zoning_allows_pharmacy": self.council_zoning_allows_pharmacy,
            "accessible_by_public": self.accessible_by_public,
            "ready_within_6_months": self.ready_within_6_months,
        }
        return [name for name, val in mapping.items() if val is False]

    @property
    def pending_checks(self) -> List[str]:
        """Return names of checks still awaiting manual verification."""
        mapping = {
            "legal_right_to_occupy": self.legal_right_to_occupy,
            "council_zoning_allows_pharmacy": self.council_zoning_allows_pharmacy,
            "accessible_by_public": self.accessible_by_public,
            "ready_within_6_months": self.ready_within_6_months,
        }
        return [name for name, val in mapping.items() if val is None]

    def to_dict(self) -> dict:
        return {
            "not_approved_premises": self.not_approved_premises,
            "not_accessible_from_supermarket": self.not_accessible_from_supermarket,
            "legal_right_to_occupy": self.legal_right_to_occupy,
            "council_zoning_allows_pharmacy": self.council_zoning_allows_pharmacy,
            "accessible_by_public": self.accessible_by_public,
            "ready_within_6_months": self.ready_within_6_months,
            "overall_status": self.overall_status(),
            "failed_checks": self.failed_checks,
            "pending_checks": self.pending_checks,
            "evidence_notes": self.evidence_notes,
        }


# ================================================================== #
#  Automated check functions
# ================================================================== #


def check_not_approved_premises(
    candidate: "Candidate",
    context: "EvaluationContext",
) -> bool:
    """
    Rule 3(a): The proposed premises must not already be approved premises.

    We check whether the candidate coordinates fall within 50 m of any
    pharmacy in the database.  A match means the site is (likely) already
    approved → returns ``False``.
    """
    nearby = context.pharmacies_within_radius(
        candidate.latitude,
        candidate.longitude,
        _APPROVED_PREMISES_MATCH_KM,
    )
    return len(nearby) == 0


def check_not_accessible_from_supermarket(
    candidate: "Candidate",
    context: "EvaluationContext",
) -> bool:
    """
    Rule 3(f): The proposed premises must not be directly accessible by the
    public from within a supermarket.

    Heuristic:
    - If the candidate is flagged as ``direct_access_from_supermarket`` on the
      Candidate model, honour that.
    - If the candidate is inside a shopping centre, check whether any
      supermarket in the DB is within 30 m (adjacent / directly accessible).
      This is a rough proxy; real verification requires floor-plan review.

    Returns ``True`` if the site passes (i.e. is NOT accessible from a supermarket).
    """
    # Explicit flag on the Candidate model (set during data import)
    if getattr(candidate, "direct_access_from_supermarket", False):
        return False

    # If the candidate is in a shopping centre, check supermarket adjacency
    if getattr(candidate, "in_shopping_centre", False) or candidate.source_type == "shopping_centre":
        nearby_supers = context.supermarkets_within_radius(
            candidate.latitude,
            candidate.longitude,
            0.03,  # 30 m — very close = likely directly accessible
        )
        if nearby_supers:
            # Could be directly accessible — flag conservatively
            # Real verification needs a floor plan, so we mark it as a fail
            # and add a note that manual review is needed.
            return False

    return True


def run_automated_checks(
    candidate: "Candidate",
    context: "EvaluationContext",
) -> GeneralRequirements:
    """
    Build a ``GeneralRequirements`` with automated checks filled in.

    Manual checks remain ``None`` pending human verification.
    """
    gr = GeneralRequirements()

    gr.not_approved_premises = check_not_approved_premises(candidate, context)
    if not gr.not_approved_premises:
        gr.evidence_notes.append(
            "AUTO: Candidate coords match an existing approved pharmacy within 50 m."
        )

    gr.not_accessible_from_supermarket = check_not_accessible_from_supermarket(
        candidate, context
    )
    if not gr.not_accessible_from_supermarket:
        gr.evidence_notes.append(
            "AUTO: Candidate appears directly accessible from a supermarket "
            "(flagged or within 30 m of a supermarket in a shopping centre). "
            "Manual floor-plan review recommended."
        )

    # Manual checks: leave as None with guidance notes
    gr.evidence_notes.append(
        "MANUAL: Verify legal_right_to_occupy — lease, agreement to lease, or ownership evidence."
    )
    gr.evidence_notes.append(
        "MANUAL: Verify council_zoning_allows_pharmacy — council planning approval or zoning confirmation."
    )
    gr.evidence_notes.append(
        "MANUAL: Verify accessible_by_public — no restrictions limiting access to certain groups."
    )
    gr.evidence_notes.append(
        "MANUAL: Verify ready_within_6_months — fit-out schedule, quotes, council approvals."
    )

    return gr
