"""
Ministerial Discretion scoring — section 90A(2) of the National Health Act 1953.

The Minister has discretionary power to approve a pharmacist even when the
ACPA Rules are not met.  This is only available AFTER the Authority has
rejected the application and the delegate has refused approval.

This module scores borderline sites that *almost* qualify under a rule item,
estimating:
  - How close the site is to each threshold (gap analysis)
  - Community need for a pharmacy
  - Precedent from similar approved pharmacies

Reference: Handbook V1.10 — "Ministerial discretion to approve"
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from engine.context import EvaluationContext
    from engine.models import Candidate, RuleResult


# ------------------------------------------------------------------ #
#  Thresholds per rule item (distance in km, counts as integers)
# ------------------------------------------------------------------ #

RULE_THRESHOLDS: dict[str, dict[str, float]] = {
    "Item 130": {"nearest_pharmacy_km": 1.5},
    "Item 131": {"nearest_pharmacy_road_km": 10.0},
    "Item 132": {
        "nearest_pharmacy_km": 0.2,        # 200 m straight line to nearest in town
        "other_pharmacy_road_km": 10.0,     # 10 km road to all others
        "gp_fte_count": 4.0,
        "supermarket_gla_sqm": 2500.0,
    },
    "Item 133": {"nearest_pharmacy_km": 0.5},
    "Item 136": {
        "nearest_pharmacy_km": 0.3,         # 300 m
        "pbs_prescriber_fte": 8.0,
        "medical_practitioner_fte": 7.0,
    },
}

# Ministerial candidate thresholds
MAX_GAP_PERCENTAGE = 15.0       # Must be within 15% of threshold
MIN_COMMUNITY_NEED = 0.6        # Minimum community need score (0–1)


@dataclass
class GapAnalysis:
    """Gap analysis for a single threshold within a rule item."""

    threshold_name: str
    """Human-readable name of the threshold (e.g. 'nearest_pharmacy_km')."""

    threshold_value: float
    """Required value to pass (e.g. 1.5 km)."""

    actual_value: float
    """Measured/observed value for the candidate."""

    @property
    def gap_amount(self) -> float:
        """
        How far below the threshold.
        Positive = below threshold (failed), negative = above (passed).
        For distance thresholds where you need to be *at least* X:
          gap = threshold - actual  (positive means too close).
        """
        return self.threshold_value - self.actual_value

    @property
    def gap_percentage(self) -> float:
        """Percentage below threshold. 0% = exactly at threshold."""
        if self.threshold_value == 0:
            return 0.0
        return max(0.0, (self.gap_amount / self.threshold_value) * 100.0)

    @property
    def passed(self) -> bool:
        return self.actual_value >= self.threshold_value

    def to_dict(self) -> dict:
        return {
            "threshold_name": self.threshold_name,
            "threshold_value": self.threshold_value,
            "actual_value": round(self.actual_value, 4),
            "gap_amount": round(self.gap_amount, 4),
            "gap_percentage": round(self.gap_percentage, 2),
            "passed": self.passed,
        }


@dataclass
class MinisterialAssessment:
    """Full ministerial discretion assessment for a candidate under one rule item."""

    candidate_id: str
    item: str
    gap_analyses: List[GapAnalysis] = field(default_factory=list)

    # Community need metrics
    community_need_score: float = 0.0
    """0.0–1.0 score based on population underserved within 10 km."""

    population_within_10km: int = 0
    """Estimated population within 10 km of candidate."""

    pharmacies_within_10km: int = 0
    """Number of pharmacies within 10 km."""

    population_per_pharmacy: float = 0.0
    """Ratio: population / pharmacies (higher = more need)."""

    # Precedent
    precedent_score: float = 0.0
    """0.0–1.0 — are there similar approved pharmacies nearby with comparable gaps?"""

    precedent_notes: str = ""

    # Overall assessment
    ministerial_candidate: bool = False
    """True if gap_percentage < 15% AND community_need_score > 0.6."""

    case_summary: str = ""
    """Generated text summarising the ministerial case."""

    def to_dict(self) -> dict:
        return {
            "candidate_id": self.candidate_id,
            "item": self.item,
            "gap_analyses": [g.to_dict() for g in self.gap_analyses],
            "community_need_score": round(self.community_need_score, 3),
            "population_within_10km": self.population_within_10km,
            "pharmacies_within_10km": self.pharmacies_within_10km,
            "population_per_pharmacy": round(self.population_per_pharmacy, 1),
            "precedent_score": round(self.precedent_score, 3),
            "precedent_notes": self.precedent_notes,
            "ministerial_candidate": self.ministerial_candidate,
            "case_summary": self.case_summary,
        }


# ================================================================== #
#  Scoring functions
# ================================================================== #


def _compute_community_need(
    candidate: "Candidate",
    context: "EvaluationContext",
) -> tuple[float, int, int, float]:
    """
    Estimate community need based on population-to-pharmacy ratio within 10 km.

    Returns (score, population, pharmacy_count, pop_per_pharmacy).

    Scoring heuristic:
      - pop_per_pharmacy >= 8000 → 1.0 (severely underserved)
      - pop_per_pharmacy >= 5000 → 0.8
      - pop_per_pharmacy >= 3000 → 0.6
      - pop_per_pharmacy >= 2000 → 0.4
      - pop_per_pharmacy >= 1000 → 0.2
      - below 1000              → 0.1

    If no pharmacies within 10 km, score = 1.0 (maximum need).
    """
    pharmacies_10km = context.pharmacies_within_radius(
        candidate.latitude, candidate.longitude, 10.0
    )
    pharmacy_count = len(pharmacies_10km)

    # Use population data from candidate if available, otherwise estimate
    population = getattr(candidate, "pop_10km", 0) or 0
    if population == 0:
        # Rough estimate: use ABS average density if no data
        # Australia average ~3.3 people/km² but pharmacy areas are urban
        # Assume moderate suburban density: ~500 people/km² within 10km radius
        # π * 10² * 500 ≈ 157,000 — too high for rural
        # Use a conservative default
        population = 5000  # placeholder — real data comes from ABS grids

    if pharmacy_count == 0:
        return 1.0, population, 0, float("inf")

    pop_per_pharmacy = population / pharmacy_count

    if pop_per_pharmacy >= 8000:
        score = 1.0
    elif pop_per_pharmacy >= 5000:
        score = 0.8
    elif pop_per_pharmacy >= 3000:
        score = 0.6
    elif pop_per_pharmacy >= 2000:
        score = 0.4
    elif pop_per_pharmacy >= 1000:
        score = 0.2
    else:
        score = 0.1

    return score, population, pharmacy_count, pop_per_pharmacy


def _compute_precedent(
    candidate: "Candidate",
    context: "EvaluationContext",
    item: str,
) -> tuple[float, str]:
    """
    Check if there are approved pharmacies nearby that were approved under
    similar circumstances (borderline threshold cases).

    This is a rough heuristic — real precedent analysis would require
    historical ACPA decision data.

    For now, we check:
    - Are there pharmacies at similar distances to their nearest neighbour?
    - In similar population density areas?

    Returns (score 0–1, explanation string).
    """
    thresholds = RULE_THRESHOLDS.get(item, {})
    if not thresholds:
        return 0.0, "No thresholds defined for precedent comparison."

    # Look at pharmacies within 20 km and check their spacing
    nearby_pharmacies = context.pharmacies_within_radius(
        candidate.latitude, candidate.longitude, 20.0
    )

    if len(nearby_pharmacies) < 2:
        return 0.5, (
            "Few pharmacies in the area — limited precedent data. "
            "Remote/underserved area may strengthen ministerial case."
        )

    # Check inter-pharmacy distances — if existing pharmacies are closely
    # spaced, it suggests the area supports dense pharmacy coverage
    min_spacing = float("inf")
    for pharm, dist in nearby_pharmacies[:5]:
        # Find the nearest OTHER pharmacy to this one
        other_nearest, other_dist = context.nearest_pharmacy(
            pharm["latitude"], pharm["longitude"]
        )
        if other_nearest and other_dist < min_spacing:
            min_spacing = other_dist

    # If nearby pharmacies are spaced at similar or smaller distances
    # than the candidate's gap, there's precedent for close spacing
    distance_threshold = thresholds.get(
        "nearest_pharmacy_km",
        thresholds.get("nearest_pharmacy_road_km", 1.5),
    )

    if min_spacing < distance_threshold:
        score = 0.7
        notes = (
            f"Existing pharmacies in the area have spacing as low as "
            f"{min_spacing:.2f} km, below the {distance_threshold} km threshold. "
            f"This supports a precedent argument."
        )
    elif min_spacing < distance_threshold * 1.5:
        score = 0.4
        notes = (
            f"Nearest inter-pharmacy spacing is {min_spacing:.2f} km, "
            f"moderately close to the {distance_threshold} km threshold."
        )
    else:
        score = 0.2
        notes = (
            f"Existing pharmacies are well-spaced ({min_spacing:.2f} km minimum). "
            f"Limited precedent for closer spacing."
        )

    return score, notes


def _build_gap_analyses(
    rule_result: "RuleResult",
    item: str,
) -> List[GapAnalysis]:
    """
    Extract gap analyses from a RuleResult's distances dict.

    The distances dict is expected to contain keys matching threshold names
    with measured values.
    """
    thresholds = RULE_THRESHOLDS.get(item, {})
    gaps = []

    for threshold_name, threshold_value in thresholds.items():
        actual = rule_result.distances.get(threshold_name)
        if actual is not None:
            gaps.append(
                GapAnalysis(
                    threshold_name=threshold_name,
                    threshold_value=threshold_value,
                    actual_value=float(actual),
                )
            )

    return gaps


def _generate_case_summary(assessment: MinisterialAssessment) -> str:
    """Generate a human-readable ministerial case summary."""
    lines = [
        f"MINISTERIAL DISCRETION CASE — {assessment.item}",
        f"Candidate: {assessment.candidate_id}",
        "",
        "GAP ANALYSIS:",
    ]

    for gap in assessment.gap_analyses:
        status = "✓ PASSED" if gap.passed else f"✗ FAILED (gap: {gap.gap_percentage:.1f}%)"
        lines.append(
            f"  {gap.threshold_name}: required {gap.threshold_value}, "
            f"actual {gap.actual_value:.4f} — {status}"
        )

    lines.extend([
        "",
        "COMMUNITY NEED:",
        f"  Score: {assessment.community_need_score:.2f} / 1.00",
        f"  Population within 10 km: {assessment.population_within_10km:,}",
        f"  Pharmacies within 10 km: {assessment.pharmacies_within_10km}",
        f"  Population per pharmacy: {assessment.population_per_pharmacy:,.0f}",
        "",
        "PRECEDENT:",
        f"  Score: {assessment.precedent_score:.2f} / 1.00",
        f"  {assessment.precedent_notes}",
        "",
    ])

    if assessment.ministerial_candidate:
        lines.append(
            "★ RECOMMENDATION: This site is a MINISTERIAL CANDIDATE. "
            "The gap is narrow and community need is demonstrated. "
            "Consider pursuing ministerial discretion under s90A(2) "
            "after ACPA rejection."
        )
    else:
        failing_reasons = []
        max_gap = max(
            (g.gap_percentage for g in assessment.gap_analyses if not g.passed),
            default=0.0,
        )
        if max_gap >= MAX_GAP_PERCENTAGE:
            failing_reasons.append(
                f"gap too large ({max_gap:.1f}% > {MAX_GAP_PERCENTAGE}% threshold)"
            )
        if assessment.community_need_score < MIN_COMMUNITY_NEED:
            failing_reasons.append(
                f"community need insufficient ({assessment.community_need_score:.2f} < {MIN_COMMUNITY_NEED})"
            )
        lines.append(
            f"✗ NOT a ministerial candidate: {'; '.join(failing_reasons) or 'does not meet criteria'}."
        )

    return "\n".join(lines)


# ================================================================== #
#  Public API
# ================================================================== #


def assess_ministerial_discretion(
    candidate: "Candidate",
    rule_result: "RuleResult",
    context: "EvaluationContext",
) -> MinisterialAssessment:
    """
    Assess whether a *failed* rule result is close enough to warrant
    a ministerial discretion application.

    Parameters
    ----------
    candidate
        The candidate premises.
    rule_result
        A RuleResult that has ``passed=False``.
    context
        EvaluationContext with spatial data.

    Returns
    -------
    MinisterialAssessment
        Full assessment including gap analysis, community need, precedent,
        and whether to flag as a ministerial candidate.
    """
    item = rule_result.item
    assessment = MinisterialAssessment(
        candidate_id=candidate.id,
        item=item,
    )

    # 1. Gap analysis
    assessment.gap_analyses = _build_gap_analyses(rule_result, item)

    # 2. Community need
    (
        assessment.community_need_score,
        assessment.population_within_10km,
        assessment.pharmacies_within_10km,
        assessment.population_per_pharmacy,
    ) = _compute_community_need(candidate, context)

    # 3. Precedent
    assessment.precedent_score, assessment.precedent_notes = _compute_precedent(
        candidate, context, item
    )

    # 4. Determine ministerial candidate flag
    failed_gaps = [g for g in assessment.gap_analyses if not g.passed]
    if failed_gaps:
        max_gap_pct = max(g.gap_percentage for g in failed_gaps)
        assessment.ministerial_candidate = (
            max_gap_pct < MAX_GAP_PERCENTAGE
            and assessment.community_need_score >= MIN_COMMUNITY_NEED
        )
    else:
        # All gaps passed — shouldn't be here (rule should have passed)
        assessment.ministerial_candidate = False

    # 5. Generate summary
    assessment.case_summary = _generate_case_summary(assessment)

    return assessment


def assess_all_failed_rules(
    candidate: "Candidate",
    rule_results: List["RuleResult"],
    context: "EvaluationContext",
) -> List[MinisterialAssessment]:
    """
    Run ministerial assessment on all failed rule results for a candidate.

    Returns a list of MinisterialAssessment objects, one per failed rule.
    Only assesses rules that have defined thresholds in RULE_THRESHOLDS.
    """
    assessments = []
    for rr in rule_results:
        if rr.passed:
            continue
        if rr.item not in RULE_THRESHOLDS:
            continue
        if not rr.distances:
            # No distance data to analyse gaps
            continue
        assessment = assess_ministerial_discretion(candidate, rr, context)
        assessments.append(assessment)
    return assessments
