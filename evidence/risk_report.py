"""
Risk report generator for "all relevant times" assessment.

The Pharmacy Location Rules require conditions to be met at:
  1. The day the application is made
  2. The day the ACPA considers the application (2-6 months later)

This module documents what could change in that window and provides
mitigation strategies.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional


# ================================================================== #
#  Risk categories by rule item
# ================================================================== #

RISK_CATEGORIES = {
    "Item 130": [
        {
            "category": "new_pharmacy_approved",
            "description": "A new pharmacy is approved within 1.5 km before the hearing",
            "likelihood": "MEDIUM",
            "impact": "Application automatically fails — distance requirement no longer met",
            "monitoring": "Check ACPA meeting outcomes monthly; monitor PBS Approved Suppliers list",
        },
        {
            "category": "pharmacy_relocation",
            "description": "An existing pharmacy relocates closer (within 1.5 km)",
            "likelihood": "LOW",
            "impact": "Same as above — distance requirement fails",
            "monitoring": "Monitor ACPA relocation approvals in surrounding area",
        },
        {
            "category": "gp_departure",
            "description": "The GP practice within 500m closes or reduces to below 1 FTE",
            "likelihood": "MEDIUM",
            "impact": "Item 130(b)(i) fails unless backup supermarket ≥ 2,500m² exists",
            "monitoring": "Verify GP lease terms; check AHPRA registration; contact practice",
        },
        {
            "category": "supermarket_closure",
            "description": "The qualifying supermarket closes or reduces GLA below threshold",
            "likelihood": "LOW",
            "impact": "Item 130(b) may fail if no alternative qualifying anchor",
            "monitoring": "Monitor supermarket lease status and trading activity",
        },
    ],
    "Item 131": [
        {
            "category": "new_pharmacy_approved",
            "description": "A new pharmacy is approved within 10 km road distance",
            "likelihood": "LOW",
            "impact": "Application fails — road distance requirement no longer met",
            "monitoring": "Check ACPA meeting outcomes; PBS Approved Suppliers list",
        },
        {
            "category": "road_changes",
            "description": "New road construction shortens the route to < 10 km",
            "likelihood": "VERY_LOW",
            "impact": "Shortest lawful route could change — measure with current infrastructure",
            "monitoring": "Check local council road works notifications",
        },
    ],
    "Item 132": [
        {
            "category": "gp_count_drops",
            "description": "GP count in town drops below 4 FTE",
            "likelihood": "MEDIUM",
            "impact": "Item 132(b)(i) fails — need ≥ 4 FTE GPs in same town",
            "monitoring": "Monthly check of GP practice websites; AHPRA registrations",
        },
        {
            "category": "supermarket_closure",
            "description": "Supermarket(s) GLA drops below 2,500 m² combined",
            "likelihood": "LOW",
            "impact": "Item 132(b)(ii) fails",
            "monitoring": "Monitor supermarket trading status",
        },
        {
            "category": "new_pharmacy_in_town",
            "description": "Another pharmacy opens in the same town changing the 'nearest' calculation",
            "likelihood": "LOW",
            "impact": "Distance measurements and same-town status may change",
            "monitoring": "Monitor ACPA approvals for the same town/postcode",
        },
    ],
    "Item 133": [
        {
            "category": "tenant_count_drops",
            "description": "Shopping centre drops below 15 commercial establishments",
            "likelihood": "MEDIUM",
            "impact": "No longer qualifies as a 'small shopping centre'",
            "monitoring": "Monitor tenancy schedule; check for vacancies",
        },
        {
            "category": "pharmacy_opens_in_centre",
            "description": "Another pharmacy application is approved for the same centre first",
            "likelihood": "LOW",
            "impact": "Item 133(c) fails — no approved premises allowed in centre",
            "monitoring": "Check ACPA approvals for the same centre",
        },
        {
            "category": "pharmacy_opens_within_500m",
            "description": "A new pharmacy opens within 500m (not in large SC or hospital)",
            "likelihood": "LOW",
            "impact": "Item 133(b) fails",
            "monitoring": "Monitor ACPA approvals in surrounding area",
        },
    ],
    "Item 134": [
        {
            "category": "tenant_count_drops",
            "description": "Shopping centre drops below 50 commercial establishments",
            "likelihood": "LOW",
            "impact": "No longer qualifies as a 'large shopping centre'",
            "monitoring": "Monitor tenancy schedule",
        },
        {
            "category": "pharmacy_opens_in_centre",
            "description": "Another pharmacy application is approved for the same centre first",
            "likelihood": "LOW",
            "impact": "Item 134(b) fails — must have no approved premises in centre",
            "monitoring": "Check ACPA approvals for the same centre",
        },
    ],
    "Item 134A": [
        {
            "category": "tenant_count_changes",
            "description": "Tenant count changes affecting pharmacy limit bracket",
            "likelihood": "MEDIUM",
            "impact": "If 100-199 drops below 100: no longer qualifies. If count bracket changes, pharmacy limit shifts.",
            "monitoring": "Monitor tenancy schedule carefully",
        },
        {
            "category": "pharmacy_relocates_out",
            "description": "A pharmacy relocates out of the centre within 12 months before application",
            "likelihood": "LOW",
            "impact": "Item 134A(c) fails — no pharmacy can have relocated out in last 12 months",
            "monitoring": "Monitor ACPA relocation approvals involving this centre",
        },
    ],
    "Item 135": [
        {
            "category": "bed_count_drops",
            "description": "Hospital bed capacity drops below 150",
            "likelihood": "VERY_LOW",
            "impact": "No longer qualifies as 'large private hospital'",
            "monitoring": "Verify hospital licence/registration status",
        },
        {
            "category": "hospital_changes_to_public",
            "description": "Hospital changes from private to public ownership",
            "likelihood": "VERY_LOW",
            "impact": "Item 135 only applies to private hospitals",
            "monitoring": "Monitor hospital ownership announcements",
        },
    ],
    "Item 136": [
        {
            "category": "prescriber_drop_below_8fte",
            "description": "PBS prescriber count drops below 8 FTE (304 hrs/week)",
            "likelihood": "HIGH",
            "impact": "Application MUST be withdrawn or will fail. This is the #1 reason Item 136 apps fail.",
            "monitoring": "Weekly FTE tracking; locum arrangements; written GP commitments",
        },
        {
            "category": "medical_practitioner_below_7",
            "description": "Medical practitioners (not total prescribers) drop below 7 FTE",
            "likelihood": "MEDIUM",
            "impact": "Item 136(d) fails — at least 7 of 8 FTE must be medical practitioners",
            "monitoring": "Track medical vs non-medical prescriber split weekly",
        },
        {
            "category": "operating_hours_below_70",
            "description": "Medical centre reduces hours below 70 per week",
            "likelihood": "MEDIUM",
            "impact": "No longer qualifies as 'large medical centre'",
            "monitoring": "Monitor centre operating hours; watch for public holiday impacts",
        },
        {
            "category": "gp_availability_below_70hrs",
            "description": "GP availability for appointments drops below 70 hrs/week",
            "likelihood": "MEDIUM",
            "impact": "No longer qualifies as 'large medical centre' — need ≥1 GP available 70+ hrs/week",
            "monitoring": "Verify GP roster covers 70+ hrs per week",
        },
        {
            "category": "management_structure_change",
            "description": "Medical centre management structure changes (loses 'single management')",
            "likelihood": "LOW",
            "impact": "No longer qualifies as 'large medical centre'",
            "monitoring": "Monitor centre management arrangements",
        },
        {
            "category": "new_pharmacy_within_300m",
            "description": "A new pharmacy opens within 300m (not in large SC or hospital)",
            "likelihood": "LOW",
            "impact": "Item 136(c) fails",
            "monitoring": "Monitor ACPA approvals in surrounding 300m",
        },
    ],
}

# Overall risk rating map
OVERALL_RISK_MAP = {
    0: "LOW",
    1: "MEDIUM",
    2: "HIGH",
    3: "CRITICAL",
}


def _compute_overall_risk(risk_factors: List[dict]) -> str:
    """Compute overall risk from individual factors."""
    score = 0
    for factor in risk_factors:
        likelihood = factor.get("likelihood", "LOW")
        impact_severity = factor.get("impact_severity", "MEDIUM")

        lk_score = {"VERY_LOW": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(likelihood, 1)
        if lk_score >= 3:
            score = max(score, 3)
        elif lk_score >= 2:
            score = max(score, 2)
        elif lk_score >= 1:
            score = max(score, 1)

    return OVERALL_RISK_MAP.get(score, "MEDIUM")


def generate_risk_report(
    evaluation_result: dict,
    risk_assessment: Optional[dict] = None,
) -> dict:
    """
    Generate a comprehensive risk report for a candidate site.

    Parameters
    ----------
    evaluation_result : dict
        The evaluation result from v2_results.
    risk_assessment : dict or None
        Optional pre-computed risk assessment from engine.risk_assessment.

    Returns
    -------
    dict with structure:
        {
            "candidate": {...},
            "assessment_date": str,
            "qualifying_rules": [...],
            "risk_factors_by_rule": {
                "Item 130": [...],
            },
            "all_relevant_times_window": {...},
            "mitigation_plan": [...],
            "overall_risk": "HIGH" | "MEDIUM" | "LOW",
            "recommendation": str,
        }
    """
    # Parse rules
    rules_json = evaluation_result.get("rules_json", "[]")
    if isinstance(rules_json, str):
        passing_rules = json.loads(rules_json)
    else:
        passing_rules = rules_json

    qualifying_items = [r["item"] for r in passing_rules]

    # Build risk factors for each qualifying rule
    risk_factors_by_rule = {}
    all_risk_factors = []

    for item in qualifying_items:
        rule_result = next((r for r in passing_rules if r["item"] == item), {})
        distances = rule_result.get("distances", {})

        # Get template risks
        template_risks = RISK_CATEGORIES.get(item, [])
        item_risks = []

        for template in template_risks:
            risk = dict(template)

            # Enrich with actual data where available
            if item == "Item 130" and template["category"] == "new_pharmacy_approved":
                nearest_km = distances.get("nearest_pharmacy_km", 0)
                if nearest_km:
                    margin_m = (nearest_km - 1.5) * 1000
                    risk["current_measurement"] = f"Nearest pharmacy: {nearest_km:.3f} km (margin: {margin_m:.0f}m)"
                    if margin_m < 100:
                        risk["likelihood"] = "HIGH"
                    elif margin_m < 300:
                        risk["likelihood"] = "MEDIUM"

            elif item == "Item 136" and template["category"] == "prescriber_drop_below_8fte":
                fte = distances.get("mc_total_fte", 0)
                if fte:
                    margin = fte - 8.0
                    risk["current_measurement"] = f"Current FTE: {fte:.1f} (margin: {margin:.1f})"
                    if margin < 1.0:
                        risk["likelihood"] = "HIGH"
                    elif margin < 2.0:
                        risk["likelihood"] = "MEDIUM"
                    else:
                        risk["likelihood"] = "LOW"

            elif item == "Item 136" and template["category"] == "operating_hours_below_70":
                hours = distances.get("mc_hours_per_week", 0)
                if hours:
                    margin = hours - 70
                    risk["current_measurement"] = f"Current hours: {hours:.0f}/week (margin: {margin:.0f})"
                    if margin < 3:
                        risk["likelihood"] = "HIGH"
                    elif margin < 5:
                        risk["likelihood"] = "MEDIUM"

            elif item == "Item 132" and template["category"] == "gp_count_drops":
                fte = distances.get("mc_total_fte", 0)
                if fte:
                    margin = fte - 4.0
                    risk["current_measurement"] = f"Current FTE: {fte:.1f} (margin: {margin:.1f})"

            item_risks.append(risk)

        risk_factors_by_rule[item] = item_risks
        all_risk_factors.extend(item_risks)

    # Include pre-computed risk assessment if available
    if risk_assessment:
        pre_factors = risk_assessment.get("risk_factors", [])
        for pf in pre_factors:
            if pf not in all_risk_factors:
                all_risk_factors.append(pf)

    # "All relevant times" window
    acpa_info = {
        "description": "Conditions must be met at BOTH: (1) application date, AND (2) ACPA hearing date",
        "typical_gap_months": "2-6 months",
        "acpa_meetings_per_year": 9,
        "cutoff_before_meeting": "~5 weeks",
        "note": "The ACPA will independently verify conditions at hearing. "
                "The Secretariat contacts medical centres directly to check hours.",
        "what_changes": [
            "New pharmacy approvals in the area",
            "GP departures or reduced hours",
            "Shopping centre tenant count changes",
            "Medical centre prescriber roster changes",
            "Supermarket closures or GLA reductions",
            "Road/infrastructure changes (for road distance items)",
        ],
    }

    # Build mitigation plan
    mitigation_plan = []
    for item, risks in risk_factors_by_rule.items():
        for risk in risks:
            if risk.get("likelihood") in ("MEDIUM", "HIGH"):
                mitigation_plan.append({
                    "rule": item,
                    "risk": risk["category"],
                    "action": risk.get("monitoring", ""),
                    "frequency": "Monthly" if risk["likelihood"] == "MEDIUM" else "Weekly",
                    "responsible": "Applicant / Monitoring System",
                })

    # Overall risk
    overall_risk = _compute_overall_risk(all_risk_factors)

    # Recommendation
    if overall_risk == "LOW":
        recommendation = (
            "Low risk profile. Proceed with application when evidence is gathered. "
            "Set up standard monitoring for the application-to-hearing window."
        )
    elif overall_risk == "MEDIUM":
        recommendation = (
            "Moderate risk. Review the identified risk factors before lodging. "
            "Implement the mitigation plan and monitor conditions monthly. "
            "Consider whether borderline measurements warrant a surveyor's report."
        )
    elif overall_risk == "HIGH":
        recommendation = (
            "High risk. Address the HIGH-likelihood factors before lodging. "
            "Strongly recommended to implement weekly monitoring for the most volatile conditions. "
            "Consider waiting for more stable conditions or applying under an alternative rule item."
        )
    else:
        recommendation = (
            "Critical risk. Multiple high-likelihood risks identified. "
            "Strongly consider delaying the application until conditions stabilise. "
            "Seek legal advice before proceeding."
        )

    return {
        "candidate": {
            "id": evaluation_result.get("id", ""),
            "name": evaluation_result.get("name", ""),
            "address": evaluation_result.get("address", ""),
        },
        "assessment_date": datetime.now().isoformat(),
        "qualifying_rules": qualifying_items,
        "risk_factors_by_rule": risk_factors_by_rule,
        "all_relevant_times_window": acpa_info,
        "mitigation_plan": mitigation_plan,
        "overall_risk": overall_risk,
        "recommendation": recommendation,
    }
