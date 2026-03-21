"""
Evidence checklist generator based on ACPA Handbook V1.10.

For each qualifying rule item, generates a comprehensive list of:
  - All requirements with pass/fail/pending status
  - Evidence items needed (from handbook)
  - Evidence items already available (from our data)
  - Evidence items requiring manual verification
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


# ================================================================== #
#  General Requirements (apply to ALL items)
# ================================================================== #

GENERAL_REQUIREMENTS = [
    {
        "id": "GR-1",
        "section": "Part 2, Section 10(3)(a)",
        "requirement": "Proposed premises are not approved premises",
        "evidence_needed": [
            "Statement confirming premises are not currently approved for PBS supply",
            "Check PBS Approved Suppliers list for the address",
        ],
        "auto_checkable": True,
        "auto_field": "not_approved_premises",
    },
    {
        "id": "GR-2",
        "section": "Part 2, Section 10(3)(b)",
        "requirement": "Applicant has legal right to occupy proposed premises at all relevant times",
        "evidence_needed": [
            "Fully executed lease or agreement to lease (signed by lessor and lessee)",
            "OR title deed / sales contract if purchasing",
            "If sub-lease: evidence head lease permits sub-leasing or head lessor consent",
            "All applicants must be named in the lease",
            "NOTE: Unsigned or non-binding leases are NOT sufficient",
        ],
        "auto_checkable": False,
        "auto_field": None,
    },
    {
        "id": "GR-3",
        "section": "Part 2, Section 10(3)(c)",
        "requirement": "Council approval to use premises for pharmacy operation",
        "evidence_needed": [
            "Council decision notice approving pharmacy use, OR",
            "Letter from council confirming zoning permits pharmacy without DA, OR",
            "Property Planning Report showing suitable zoning with relevant schedules",
            "NOTE: Landlord approval alone is NOT sufficient",
            "NOTE: Lodged-but-undecided DA is NOT sufficient",
        ],
        "auto_checkable": False,
        "auto_field": None,
    },
    {
        "id": "GR-4",
        "section": "Part 2, Section 10(3)(d)",
        "requirement": "Proposed premises accessible by the public",
        "evidence_needed": [
            "Floor plan showing public access points",
            "Confirmation pharmacy is open to all public (not restricted to patients/staff)",
            "No physical barriers preventing public access",
        ],
        "auto_checkable": False,
        "auto_field": None,
    },
    {
        "id": "GR-5",
        "section": "Part 2, Section 10(3)(e)",
        "requirement": "Ready to trade within 6 months of ACPA recommendation",
        "evidence_needed": [
            "Council-approved plans for fit-out",
            "Formal building/fit-out schedule with completion date",
            "Quotes and/or contracts for works",
            "Dated photographs showing current premises status",
        ],
        "auto_checkable": False,
        "auto_field": None,
    },
    {
        "id": "GR-6",
        "section": "Part 2, Section 10(3)(f)",
        "requirement": "Premises NOT directly accessible from within a supermarket",
        "evidence_needed": [
            "Floor plan showing premises and adjacent shops",
            "Public access points to pharmacy clearly marked",
            "Information about type of adjacent/adjoining shops",
        ],
        "auto_checkable": True,
        "auto_field": "not_accessible_from_supermarket",
    },
]


# ================================================================== #
#  Item-Specific Requirements
# ================================================================== #

ITEM_REQUIREMENTS = {
    "Item 130": [
        {
            "id": "130-a",
            "section": "Item 130(a)",
            "requirement": "Premises ≥ 1.5 km straight line from nearest approved premises",
            "evidence_needed": [
                "If substantially > 1.5 km: scaled map with locations marked and distance noted",
                "If near 1.5 km: licensed surveyor's report including:",
                "  - Surveyor's licence/registration number",
                "  - Clearly scaled map",
                "  - Methodology and equipment description",
                "  - Straight line distance measured",
                "  - Margin for error",
                "  - Public access door details for both premises",
                "  - Confirmation: midpoint at ground level of public access door",
            ],
            "auto_checkable": True,
            "auto_field": "nearest_pharmacy_distance",
            "threshold_m": 1500,
        },
        {
            "id": "130-b",
            "section": "Item 130(b)",
            "requirement": "Within 500m: (i) ≥1 FTE prescribing GP AND supermarket ≥1,000m² GLA, OR (ii) supermarket ≥2,500m² GLA",
            "evidence_needed": [
                "Option (i) GP evidence:",
                "  - Statutory declaration from GP/practice manager re: operating hours",
                "  - Hours GP available for appointments and prescribing",
                "  - Practice information sheet and provider number(s)",
                "Option (i) or (ii) Supermarket evidence:",
                "  - Scaled map showing premises and supermarket within 500m",
                "  - GLA evidence from council, retail strategies, or proprietary databases",
                "  - Actual GLA figure (not just 'greater than' threshold)",
                "If near 500m: surveyor's report for distance",
            ],
            "auto_checkable": True,
            "auto_field": "gp_and_supermarket_500m",
        },
    ],
    "Item 131": [
        {
            "id": "131-a",
            "section": "Item 131",
            "requirement": "Premises ≥ 10 km by shortest lawful access route from nearest approved premises",
            "evidence_needed": [
                "If substantially > 10 km: scaled map with route and distance",
                "If near 10 km: licensed surveyor's report including:",
                "  - Surveyor's licence/registration number",
                "  - Clearly scaled map",
                "  - Methodology and equipment description",
                "  - Shortest lawful access route distance measured",
                "  - Margin for error",
                "  - Public access door details",
                "  - Confirmation: midpoint at ground level of public access door",
                "Route must be 'generally available to average persons'",
            ],
            "auto_checkable": True,
            "auto_field": "road_distance_nearest",
            "threshold_km": 10.0,
        },
    ],
    "Item 132": [
        {
            "id": "132-a-i",
            "section": "Item 132(a)(i)",
            "requirement": "Premises in same town (name + postcode) as an approved premises",
            "evidence_needed": [
                "Map showing both premises in same town",
                "Confirm same town name AND same postcode",
            ],
            "auto_checkable": True,
            "auto_field": "same_town",
        },
        {
            "id": "132-a-ii",
            "section": "Item 132(a)(ii)",
            "requirement": "Premises ≥ 200m straight line from nearest approved premises",
            "evidence_needed": [
                "If substantially > 200m: scaled map with distance",
                "If near 200m: surveyor's report (same format as Item 130)",
            ],
            "auto_checkable": True,
            "auto_field": "nearest_pharmacy_200m",
            "threshold_m": 200,
        },
        {
            "id": "132-a-iii",
            "section": "Item 132(a)(iii)",
            "requirement": "≥ 10 km by road from ALL other approved premises (except nearest)",
            "evidence_needed": [
                "Map showing all other pharmacies and road distances",
                "If near 10 km for any: surveyor's report",
            ],
            "auto_checkable": True,
            "auto_field": "road_distance_all_others",
            "threshold_km": 10.0,
        },
        {
            "id": "132-b-i",
            "section": "Item 132(b)(i)",
            "requirement": "≥ 4 FTE prescribing medical practitioners practising in same town",
            "evidence_needed": [
                "Statutory declaration from each GP/practice manager",
                "Hours each GP available for appointments and prescribing",
                "Provider numbers for each GP",
                "Confirmation GPs practise in the same town (not just live there)",
            ],
            "auto_checkable": True,
            "auto_field": "town_gp_fte",
            "threshold": 4.0,
        },
        {
            "id": "132-b-ii",
            "section": "Item 132(b)(ii)",
            "requirement": "1-2 supermarkets with combined GLA ≥ 2,500 m² in same town",
            "evidence_needed": [
                "Scaled map showing supermarket(s) in town",
                "GLA evidence from council or proprietary databases",
                "Actual GLA figure (not just 'greater than 2,500 m²')",
            ],
            "auto_checkable": True,
            "auto_field": "town_supermarket_gla",
            "threshold_sqm": 2500,
        },
    ],
    "Item 133": [
        {
            "id": "133-a",
            "section": "Item 133(a)",
            "requirement": "Premises in a small shopping centre",
            "evidence_needed": [
                "Floor plan of shopping centre",
                "Statutory declaration from centre manager confirming:",
                "  - Single management arrangement",
                "  - GLA ≥ 5,000 m²",
                "  - Contains supermarket ≥ 2,500 m² GLA",
                "  - ≥ 15 other commercial establishments",
                "  - Customer parking facilities",
                "Current tenancy schedule listing name, type, leasing/trading status",
            ],
            "auto_checkable": True,
            "auto_field": "in_small_shopping_centre",
        },
        {
            "id": "133-b",
            "section": "Item 133(b)",
            "requirement": "≥ 500m straight line from nearest approved premises (excl. large SC or private hospital)",
            "evidence_needed": [
                "Scaled map or surveyor's report for distance",
                "If any pharmacy within 500m claimed to be in large SC or private hospital: evidence of that",
            ],
            "auto_checkable": True,
            "auto_field": "nearest_pharmacy_500m_excl",
            "threshold_m": 500,
        },
        {
            "id": "133-c",
            "section": "Item 133(c)",
            "requirement": "No approved premises in the shopping centre",
            "evidence_needed": [
                "Statutory declaration from centre manager confirming no pharmacy in centre",
            ],
            "auto_checkable": True,
            "auto_field": "no_pharmacy_in_centre",
        },
    ],
    "Item 134": [
        {
            "id": "134-a",
            "section": "Item 134(a)",
            "requirement": "Premises in a large shopping centre",
            "evidence_needed": [
                "Floor plan of shopping centre",
                "Statutory declaration from centre manager confirming:",
                "  - Single management arrangement",
                "  - GLA ≥ 5,000 m²",
                "  - Contains supermarket ≥ 2,500 m² GLA",
                "  - ≥ 50 other commercial establishments",
                "  - Customer parking facilities",
                "Current tenancy schedule listing name, type, leasing/trading status",
            ],
            "auto_checkable": True,
            "auto_field": "in_large_shopping_centre",
        },
        {
            "id": "134-b",
            "section": "Item 134(b)",
            "requirement": "No approved premises in the large shopping centre",
            "evidence_needed": [
                "Statutory declaration from centre manager confirming no pharmacy in centre",
            ],
            "auto_checkable": True,
            "auto_field": "no_pharmacy_in_centre",
        },
    ],
    "Item 134A": [
        {
            "id": "134A-a",
            "section": "Item 134A(a)",
            "requirement": "Premises in a large shopping centre",
            "evidence_needed": [
                "Same as Item 134(a)",
            ],
            "auto_checkable": True,
            "auto_field": "in_large_shopping_centre",
        },
        {
            "id": "134A-b",
            "section": "Item 134A(b)",
            "requirement": "100-199 tenants: max 1 existing pharmacy; 200+: max 2 existing pharmacies",
            "evidence_needed": [
                "Tenancy schedule with accurate count",
                "Statutory declaration confirming number of pharmacies currently in centre",
                "Confirmation no pharmacy relocated out in last 12 months",
            ],
            "auto_checkable": True,
            "auto_field": "sc_pharmacy_count_limit",
        },
    ],
    "Item 135": [
        {
            "id": "135-a",
            "section": "Item 135(a)",
            "requirement": "Premises in a large private hospital (≥ 150 admitted patients)",
            "evidence_needed": [
                "Floor plan showing pharmacy location and public access route",
                "Statutory declaration from hospital management confirming ≥ 150 capacity",
                "Copy of hospital's licence/registration certificate",
                "Hospital must be PRIVATE (not public)",
                "Photographs of proposed premises",
            ],
            "auto_checkable": True,
            "auto_field": "in_large_private_hospital",
        },
        {
            "id": "135-b",
            "section": "Item 135(b)",
            "requirement": "No approved premises in the hospital",
            "evidence_needed": [
                "Statutory declaration from hospital management confirming no pharmacy",
            ],
            "auto_checkable": True,
            "auto_field": "no_pharmacy_in_hospital",
        },
    ],
    "Item 136": [
        {
            "id": "136-a",
            "section": "Item 136(a)",
            "requirement": "Premises in a large medical centre (single management, ≥70 hrs/week, ≥1 GP for 70+ hrs/week)",
            "evidence_needed": [
                "Statutory declaration from centre manager/owner confirming:",
                "  - Single management arrangement",
                "  - Opening hours each day the centre operates",
                "  - Hours a prescribing GP is available for general practice consultations",
                "Floor plan of medical centre showing pharmacy location",
            ],
            "auto_checkable": True,
            "auto_field": "in_large_medical_centre",
        },
        {
            "id": "136-b",
            "section": "Item 136(b)",
            "requirement": "No approved premises in the medical centre",
            "evidence_needed": [
                "Statutory declaration from centre manager confirming no pharmacy in centre",
            ],
            "auto_checkable": True,
            "auto_field": "no_pharmacy_in_medical_centre",
        },
        {
            "id": "136-c",
            "section": "Item 136(c)",
            "requirement": "≥ 300m straight line from nearest approved premises (with exemptions for large SC/private hospital)",
            "evidence_needed": [
                "Scaled map or surveyor's report for distance measurement",
                "If any pharmacy within 300m: evidence it's in a DIFFERENT large SC or private hospital",
                "NOTE: This is a common failure point — ensure evidence covers ALL pharmacies within 300m",
            ],
            "auto_checkable": True,
            "auto_field": "nearest_pharmacy_300m",
            "threshold_m": 300,
        },
        {
            "id": "136-d",
            "section": "Item 136(d)",
            "requirement": "≥ 8 FTE PBS prescribers (≥7 medical practitioners) for 2 months before application AND until hearing",
            "evidence_needed": [
                "Statutory declaration from centre owner/manager re: operating hours and each PBS prescriber's hours",
                "Copies of rosters/timesheets for EVERY WEEK of 2 months pre-application",
                "Summary of roster data",
                "Start/finish times including breaks for each prescriber",
                "Confirmation hours EXCLUDE: other centres, hospital duties, nursing homes, admin",
                "Telehealth: only if GP physically present at centre AND patient is a centre patient",
                "Minimum 304 hours/week total; max 38 hrs from non-medical PBS prescribers",
                "NOTE: Authority WILL contact medical centre directly to verify",
            ],
            "auto_checkable": True,
            "auto_field": "pbs_prescriber_fte",
            "threshold": 8.0,
        },
        {
            "id": "136-e",
            "section": "Item 136(e)",
            "requirement": "Reasonable attempts to match medical centre operating hours",
            "evidence_needed": [
                "Agreement between applicant and centre management regarding hours",
                "Statement that pharmacy will endeavour to match centre's operating hours",
                "Intention to open outside normal hours if sufficient need exists",
            ],
            "auto_checkable": False,
            "auto_field": None,
        },
    ],
}


# ================================================================== #
#  Status determination helpers
# ================================================================== #

def _determine_status(
    req: dict,
    rule_result: Optional[dict],
    distances: Optional[dict],
) -> str:
    """
    Determine pass/fail/pending status for a requirement.

    Returns one of: 'PASS', 'FAIL', 'PENDING', 'NOT_ASSESSED'
    """
    if not req.get("auto_checkable"):
        return "PENDING"  # Requires manual verification

    if rule_result is None:
        return "NOT_ASSESSED"

    # Check if the rule passed overall
    field = req.get("auto_field", "")
    reasons = rule_result.get("reasons", [])

    # Look for explicit PASS/FAIL mentions in reasons
    req_id = req["id"]
    section_ref = req.get("section", "")

    for reason in reasons:
        # Match section references like "(a)", "(b)", "(c)", "(d)"
        section_letter = ""
        if "(" in section_ref:
            parts = section_ref.split("(")
            if len(parts) > 1:
                section_letter = "(" + parts[-1]

        if section_letter and section_letter.lower() in reason.lower():
            if "PASS" in reason:
                return "PASS"
            elif "FAIL" in reason:
                return "FAIL"

    # If the overall rule passed and we can't find specific mention
    if rule_result.get("passed", False):
        return "PASS"

    # Check by specific requirement patterns
    if "distance" in field or "nearest_pharmacy" in field:
        threshold_key = "threshold_m" if "threshold_m" in req else "threshold_km"
        if threshold_key in req:
            # Look in distances dict for relevant measurements
            if distances:
                for k, v in distances.items():
                    if "nearest_pharmacy" in k and ("km" in k or "_m" in k):
                        if isinstance(v, (int, float)):
                            if "threshold_m" in req:
                                val_m = v * 1000 if v < 100 else v  # heuristic
                                if val_m >= req["threshold_m"]:
                                    return "PASS"
                                else:
                                    return "FAIL"

    return "PENDING"


def _categorise_evidence(req: dict, status: str) -> dict:
    """Categorise evidence items into available/needed/manual."""
    evidence_items = req.get("evidence_needed", [])

    available = []
    needed = []
    manual = []

    for item in evidence_items:
        # Items we can provide from our data
        if any(kw in item.lower() for kw in ["scaled map", "map showing", "map highlighting"]):
            if status in ("PASS", "FAIL"):
                available.append(item + " [AUTO-GENERATED from our data]")
            else:
                needed.append(item)
        elif any(kw in item.lower() for kw in ["surveyor", "statutory declaration", "lease",
                                                  "title deed", "council", "letter from",
                                                  "photographs", "floor plan", "rosters",
                                                  "timesheets", "agreement"]):
            manual.append(item)
        else:
            needed.append(item)

    return {
        "available_from_data": available,
        "still_needed": needed,
        "requires_manual_verification": manual,
    }


# ================================================================== #
#  Public API
# ================================================================== #

def generate_checklist(evaluation_result: dict) -> dict:
    """
    Generate a comprehensive evidence checklist for a candidate site.

    Parameters
    ----------
    evaluation_result : dict
        The evaluation result dict from v2_results. Must contain:
        - rules_json (list of passing rule dicts)
        - all_rules_json (list of all rule dicts)
        - Plus standard fields: id, name, address, etc.

    Returns
    -------
    dict with structure:
        {
            "candidate": {...},
            "general_requirements": [...],
            "qualifying_rules": {
                "Item 130": {
                    "passed": True,
                    "confidence": 0.85,
                    "requirements": [...],
                },
                ...
            },
            "summary": {
                "total_requirements": N,
                "passed": N,
                "failed": N,
                "pending": N,
            }
        }
    """
    import json

    # Parse rule results
    rules_json = evaluation_result.get("rules_json", "[]")
    all_rules_json = evaluation_result.get("all_rules_json", "[]")

    if isinstance(rules_json, str):
        passing_rules = json.loads(rules_json)
    else:
        passing_rules = rules_json

    if isinstance(all_rules_json, str):
        all_rules = json.loads(all_rules_json)
    else:
        all_rules = all_rules_json

    # Build rule result lookup
    rule_lookup = {r["item"]: r for r in all_rules}

    # Process general requirements
    general_checklist = []
    for req in GENERAL_REQUIREMENTS:
        status = "PENDING"
        if req["auto_checkable"]:
            # Check from any passing rule result
            if passing_rules:
                status = "PASS"  # If it passed any rule, general reqs are assumed met by the engine
            else:
                status = "PENDING"

        evidence_cat = _categorise_evidence(req, status)

        general_checklist.append({
            "id": req["id"],
            "section": req["section"],
            "requirement": req["requirement"],
            "status": status,
            "evidence": evidence_cat,
        })

    # Process item-specific requirements for qualifying rules
    qualifying_rules = {}
    for rule_result in passing_rules:
        item = rule_result["item"]
        item_reqs = ITEM_REQUIREMENTS.get(item, [])
        distances = rule_result.get("distances", {})

        req_checklist = []
        for req in item_reqs:
            status = _determine_status(req, rule_result, distances)
            evidence_cat = _categorise_evidence(req, status)

            req_checklist.append({
                "id": req["id"],
                "section": req["section"],
                "requirement": req["requirement"],
                "status": status,
                "evidence": evidence_cat,
            })

        qualifying_rules[item] = {
            "passed": rule_result.get("passed", False),
            "confidence": rule_result.get("confidence", 0.0),
            "evidence_needed_from_engine": rule_result.get("evidence_needed", []),
            "requirements": req_checklist,
        }

    # Summary counts
    all_reqs = general_checklist[:]
    for item_data in qualifying_rules.values():
        all_reqs.extend(item_data["requirements"])

    total = len(all_reqs)
    passed = sum(1 for r in all_reqs if r["status"] == "PASS")
    failed = sum(1 for r in all_reqs if r["status"] == "FAIL")
    pending = sum(1 for r in all_reqs if r["status"] in ("PENDING", "NOT_ASSESSED"))

    return {
        "candidate": {
            "id": evaluation_result.get("id", ""),
            "name": evaluation_result.get("name", ""),
            "address": evaluation_result.get("address", ""),
        },
        "general_requirements": general_checklist,
        "qualifying_rules": qualifying_rules,
        "summary": {
            "total_requirements": total,
            "passed": passed,
            "failed": failed,
            "pending": pending,
            "readiness_pct": round(passed / total * 100 if total > 0 else 0, 1),
        },
    }
