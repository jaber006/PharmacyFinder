"""
"All Relevant Times" risk assessment.

The Rules require that conditions are met at TWO points:
  1. The day on which the application is made.
  2. The day on which the Authority considers the application.

There is typically a 2–6 month gap between these dates. This module
assesses the risk that qualifying conditions may change during that window.

Risk levels:
  LOW    — Stable conditions, unlikely to change.
  MEDIUM — Some volatility; conditions could shift.
  HIGH   — Fragile; meaningful chance conditions won't hold at hearing.

References:
  - Handbook V1.10 Glossary: "all relevant times"
  - Item 130: nearest pharmacy distance could change if new pharmacy opens
  - Item 132: GP count in town could drop below 4 FTE
  - Item 136: 8 FTE PBS prescribers must be maintained for 2 months prior
              AND until hearing date
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from engine.context import EvaluationContext
    from engine.models import Candidate


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


@dataclass
class RiskFactor:
    """A single risk factor that could affect qualification at hearing."""

    category: str
    """Risk category (e.g. 'gp_departure', 'new_pharmacy', 'prescriber_drop')."""

    description: str
    """Human-readable explanation of the risk."""

    risk_level: RiskLevel
    """LOW / MEDIUM / HIGH."""

    mitigation: str = ""
    """Suggested mitigation or monitoring action."""

    data_points: dict = field(default_factory=dict)
    """Supporting data for the risk assessment."""

    def to_dict(self) -> dict:
        return {
            "category": self.category,
            "description": self.description,
            "risk_level": self.risk_level.value,
            "mitigation": self.mitigation,
            "data_points": self.data_points,
        }


@dataclass
class RiskAssessment:
    """Complete risk assessment for a candidate under a specific rule item."""

    candidate_id: str
    item: str
    risk_factors: List[RiskFactor] = field(default_factory=list)
    overall_risk: RiskLevel = RiskLevel.LOW
    summary: str = ""
    recommended_monitoring: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "candidate_id": self.candidate_id,
            "item": self.item,
            "risk_factors": [rf.to_dict() for rf in self.risk_factors],
            "overall_risk": self.overall_risk.value,
            "summary": self.summary,
            "recommended_monitoring": self.recommended_monitoring,
        }


# ================================================================== #
#  Risk assessors per rule item
# ================================================================== #


def _assess_item_130_risks(
    candidate: "Candidate",
    context: "EvaluationContext",
) -> List[RiskFactor]:
    """
    Item 130 risks: ≥ 1.5 km from nearest pharmacy.

    Primary risk: A new pharmacy opens within 1.5 km before the hearing,
    invalidating the distance requirement.
    """
    risks: List[RiskFactor] = []

    nearest_pharm, nearest_dist_km = context.nearest_pharmacy(
        candidate.latitude, candidate.longitude
    )

    if nearest_pharm is None:
        risks.append(RiskFactor(
            category="no_pharmacy_data",
            description="No pharmacy data available — cannot assess distance risk.",
            risk_level=RiskLevel.MEDIUM,
            mitigation="Verify pharmacy database is complete and up to date.",
        ))
        return risks

    margin_km = nearest_dist_km - 1.5
    margin_m = margin_km * 1000

    # Check for pharmacies in the 1.5–3.0 km ring that could relocate closer
    pharmacies_in_ring = context.pharmacies_within_radius(
        candidate.latitude, candidate.longitude, 3.0
    )
    # Exclude the nearest one already counted
    ring_count = len([p for p, d in pharmacies_in_ring if d > nearest_dist_km])

    if margin_m < 100:
        risk_level = RiskLevel.HIGH
        desc = (
            f"Nearest pharmacy is only {nearest_dist_km:.3f} km away "
            f"(margin: {margin_m:.0f} m above 1.5 km threshold). "
            f"Any measurement variation or pharmacy relocation could fail this check."
        )
    elif margin_m < 300:
        risk_level = RiskLevel.MEDIUM
        desc = (
            f"Nearest pharmacy is {nearest_dist_km:.3f} km away "
            f"(margin: {margin_m:.0f} m). Moderate risk of new pharmacy "
            f"opening in the gap."
        )
    else:
        risk_level = RiskLevel.LOW
        desc = (
            f"Nearest pharmacy is {nearest_dist_km:.3f} km away "
            f"(margin: {margin_m:.0f} m). Comfortable margin."
        )

    risks.append(RiskFactor(
        category="new_pharmacy_opens",
        description=desc,
        risk_level=risk_level,
        mitigation=(
            "Monitor ACPA meeting outcomes for new pharmacy approvals in the area. "
            "Check PBS Approved Suppliers list monthly."
        ),
        data_points={
            "nearest_pharmacy_km": round(nearest_dist_km, 4),
            "margin_m": round(margin_m, 1),
            "pharmacies_within_3km": len(pharmacies_in_ring),
            "pharmacies_in_ring_1_5_to_3km": ring_count,
        },
    ))

    # Also check GP / supermarket stability for 130(b)
    gps_500m = context.gps_within_radius(
        candidate.latitude, candidate.longitude, 0.5
    )
    if len(gps_500m) <= 1:
        risks.append(RiskFactor(
            category="gp_departure",
            description=(
                f"Only {len(gps_500m)} GP practice(s) within 500 m. "
                f"If the GP leaves, Item 130(b)(i) may fail."
            ),
            risk_level=RiskLevel.HIGH if len(gps_500m) == 1 else RiskLevel.MEDIUM,
            mitigation=(
                "Verify GP's lease term and intention to stay. "
                "Check for backup GP within 500 m."
            ),
            data_points={"gp_count_500m": len(gps_500m)},
        ))

    return risks


def _assess_item_132_risks(
    candidate: "Candidate",
    context: "EvaluationContext",
) -> List[RiskFactor]:
    """
    Item 132 risks: New additional pharmacy (≥ 10 km).

    Primary risk: GP count drops below 4 FTE in the town before hearing.
    """
    risks: List[RiskFactor] = []

    # GP risk — need ≥ 4 FTE prescribing GPs in same town
    num_gps = getattr(candidate, "num_gps", None) or 0
    total_fte = getattr(candidate, "total_fte", None) or 0.0

    if total_fte > 0:
        fte_margin = total_fte - 4.0

        if fte_margin < 0.5:
            risk_level = RiskLevel.HIGH
            desc = (
                f"Town has {total_fte:.1f} FTE GPs (margin: {fte_margin:.1f} above "
                f"4.0 threshold). Loss of a single part-time GP could fail this requirement."
            )
        elif fte_margin < 1.5:
            risk_level = RiskLevel.MEDIUM
            desc = (
                f"Town has {total_fte:.1f} FTE GPs (margin: {fte_margin:.1f}). "
                f"Moderate buffer but GP workforce is volatile."
            )
        else:
            risk_level = RiskLevel.LOW
            desc = (
                f"Town has {total_fte:.1f} FTE GPs (margin: {fte_margin:.1f}). "
                f"Comfortable buffer."
            )

        risks.append(RiskFactor(
            category="gp_count_drop",
            description=desc,
            risk_level=risk_level,
            mitigation=(
                "Monitor GP practice websites and AHPRA registrations. "
                "Confirm GPs' contracts extend beyond expected hearing date."
            ),
            data_points={
                "total_fte": total_fte,
                "num_gps": num_gps,
                "fte_margin": round(fte_margin, 2),
            },
        ))
    else:
        risks.append(RiskFactor(
            category="gp_data_missing",
            description="No GP FTE data available for the town. Cannot assess risk.",
            risk_level=RiskLevel.MEDIUM,
            mitigation="Obtain verified GP FTE data before lodging application.",
        ))

    # Road distance risk — new pharmacy could open within 10 km road distance
    risks.append(RiskFactor(
        category="new_pharmacy_opens",
        description=(
            "Risk that a new pharmacy opens reducing road distance to < 10 km "
            "from another pharmacy (not the nearest in town)."
        ),
        risk_level=RiskLevel.LOW,
        mitigation="Monitor ACPA approvals in surrounding towns.",
    ))

    return risks


def _assess_item_136_risks(
    candidate: "Candidate",
    context: "EvaluationContext",
) -> List[RiskFactor]:
    """
    Item 136 risks: Large medical centre.

    Primary risk: PBS prescriber count drops below 8 FTE (or 7 medical
    practitioners) at any point during the 2-month pre-application period
    or between application and hearing.

    This is the HIGHEST-RISK item because:
    - 8 FTE = 304 hours/week of PBS prescriber time
    - Must be maintained for 2+ months BEFORE application
    - Must STILL be met on hearing day
    - Telehealth only counts if GP is physically at the centre
    - Admin time, hospital rounds, other centres DO NOT count
    """
    risks: List[RiskFactor] = []

    total_fte = getattr(candidate, "total_fte", None) or 0.0
    num_gps = getattr(candidate, "num_gps", None) or 0

    # PBS prescriber FTE risk
    if total_fte > 0:
        fte_margin = total_fte - 8.0

        if fte_margin < 0:
            risk_level = RiskLevel.HIGH
            desc = (
                f"Medical centre currently has {total_fte:.1f} FTE PBS prescribers, "
                f"which is BELOW the 8.0 FTE threshold. "
                f"Application cannot be lodged until 8 FTE is sustained for 2 months."
            )
        elif fte_margin < 1.0:
            risk_level = RiskLevel.HIGH
            desc = (
                f"Medical centre has {total_fte:.1f} FTE PBS prescribers "
                f"(margin: {fte_margin:.1f}). A single GP taking leave, "
                f"reducing hours, or departing could drop below 8 FTE. "
                f"This is the most common reason Item 136 applications fail."
            )
        elif fte_margin < 2.0:
            risk_level = RiskLevel.MEDIUM
            desc = (
                f"Medical centre has {total_fte:.1f} FTE PBS prescribers "
                f"(margin: {fte_margin:.1f}). Some buffer exists but "
                f"annual leave / sick leave patterns could still cause issues."
            )
        else:
            risk_level = RiskLevel.LOW
            desc = (
                f"Medical centre has {total_fte:.1f} FTE PBS prescribers "
                f"(margin: {fte_margin:.1f}). Good buffer against leave and departures."
            )

        risks.append(RiskFactor(
            category="prescriber_drop",
            description=desc,
            risk_level=risk_level,
            mitigation=(
                "Obtain written commitments from GPs to maintain hours. "
                "Confirm locum arrangements are in place for leave cover. "
                "Track weekly FTE from 2 months before intended application date. "
                "The Authority WILL contact the medical centre directly to verify hours."
            ),
            data_points={
                "total_fte": total_fte,
                "num_gps": num_gps,
                "fte_margin": round(fte_margin, 2),
                "required_hours_per_week": 304,
                "max_non_medical_hours": 38,
            },
        ))
    else:
        risks.append(RiskFactor(
            category="prescriber_data_missing",
            description=(
                "No FTE data for the medical centre. "
                "Item 136(d) requires detailed roster evidence — this must be gathered."
            ),
            risk_level=RiskLevel.HIGH,
            mitigation=(
                "Obtain complete roster/timesheet data for all PBS prescribers "
                "for the preceding 2 months. Verify hours exclude admin, "
                "hospital rounds, other centres, and non-physical telehealth."
            ),
        ))

    # Operating hours risk
    hours_per_week = getattr(candidate, "hours_per_week", None) or 0.0
    if hours_per_week > 0 and hours_per_week < 70:
        risks.append(RiskFactor(
            category="operating_hours",
            description=(
                f"Medical centre operates {hours_per_week:.0f} hours/week, "
                f"below the 70-hour requirement for 'large medical centre' definition."
            ),
            risk_level=RiskLevel.HIGH,
            mitigation="Centre must extend operating hours to ≥ 70 hrs/week.",
            data_points={"hours_per_week": hours_per_week},
        ))
    elif hours_per_week >= 70 and hours_per_week < 75:
        risks.append(RiskFactor(
            category="operating_hours",
            description=(
                f"Medical centre operates {hours_per_week:.0f} hours/week "
                f"(margin: {hours_per_week - 70:.0f} hrs above 70-hour threshold). "
                f"Small buffer — any reduction could disqualify."
            ),
            risk_level=RiskLevel.MEDIUM,
            mitigation="Monitor centre hours. Ensure public holidays don't drop below 70.",
            data_points={"hours_per_week": hours_per_week},
        ))

    # Distance risk (300 m from nearest non-exempt pharmacy)
    nearest_pharm, nearest_dist_km = context.nearest_pharmacy(
        candidate.latitude, candidate.longitude
    )
    if nearest_pharm and nearest_dist_km < 0.5:
        margin_m = (nearest_dist_km - 0.3) * 1000
        if margin_m < 50:
            risks.append(RiskFactor(
                category="distance_risk",
                description=(
                    f"Nearest pharmacy is {nearest_dist_km * 1000:.0f} m away "
                    f"(margin: {margin_m:.0f} m above 300 m threshold). "
                    f"Tight margin — verify with surveyor measurement."
                ),
                risk_level=RiskLevel.MEDIUM if margin_m > 0 else RiskLevel.HIGH,
                mitigation="Commission surveyor measurement before lodging.",
                data_points={"nearest_pharmacy_m": round(nearest_dist_km * 1000, 1)},
            ))

    return risks


def _assess_generic_risks(
    candidate: "Candidate",
    context: "EvaluationContext",
    item: str,
) -> List[RiskFactor]:
    """Generic risks that apply to multiple items."""
    risks: List[RiskFactor] = []

    # Timeline risk — ACPA meets ~9 times per year
    risks.append(RiskFactor(
        category="timeline",
        description=(
            "The ACPA meets approximately 9 times per year. "
            "Expect 2–6 months between application lodgement and hearing. "
            "All conditions must hold at BOTH dates."
        ),
        risk_level=RiskLevel.LOW,
        mitigation=(
            "Lodge application as soon as all evidence is gathered. "
            "Set up monitoring for all 'all relevant times' conditions."
        ),
    ))

    return risks


# ================================================================== #
#  Item-specific dispatcher
# ================================================================== #

_ITEM_ASSESSORS = {
    "Item 130": _assess_item_130_risks,
    "Item 132": _assess_item_132_risks,
    "Item 136": _assess_item_136_risks,
}


# ================================================================== #
#  Public API
# ================================================================== #


def assess_risks(
    candidate: "Candidate",
    item: str,
    context: "EvaluationContext",
) -> RiskAssessment:
    """
    Assess "all relevant times" risks for a candidate under a specific rule item.

    Parameters
    ----------
    candidate
        The candidate premises.
    item
        Rule item string (e.g. "Item 130", "Item 136").
    context
        EvaluationContext with spatial data.

    Returns
    -------
    RiskAssessment
        Complete risk assessment with factors, overall level, and monitoring recommendations.
    """
    assessment = RiskAssessment(
        candidate_id=candidate.id,
        item=item,
    )

    # Run item-specific assessor if available
    assessor = _ITEM_ASSESSORS.get(item)
    if assessor:
        assessment.risk_factors.extend(assessor(candidate, context))

    # Add generic risks
    assessment.risk_factors.extend(_assess_generic_risks(candidate, context, item))

    # Determine overall risk level (worst-case across all factors)
    if any(rf.risk_level == RiskLevel.HIGH for rf in assessment.risk_factors):
        assessment.overall_risk = RiskLevel.HIGH
    elif any(rf.risk_level == RiskLevel.MEDIUM for rf in assessment.risk_factors):
        assessment.overall_risk = RiskLevel.MEDIUM
    else:
        assessment.overall_risk = RiskLevel.LOW

    # Build summary
    high_count = sum(1 for rf in assessment.risk_factors if rf.risk_level == RiskLevel.HIGH)
    med_count = sum(1 for rf in assessment.risk_factors if rf.risk_level == RiskLevel.MEDIUM)
    assessment.summary = (
        f"Overall risk: {assessment.overall_risk.value} "
        f"({high_count} HIGH, {med_count} MEDIUM risk factors). "
        f"Conditions must hold at application AND hearing date."
    )

    # Compile monitoring recommendations
    for rf in assessment.risk_factors:
        if rf.mitigation and rf.risk_level in (RiskLevel.HIGH, RiskLevel.MEDIUM):
            assessment.recommended_monitoring.append(
                f"[{rf.risk_level.value}] {rf.category}: {rf.mitigation}"
            )

    return assessment
