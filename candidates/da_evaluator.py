"""
DA Evaluator: Run rules engine hypothetically on Council DAs.

For each DA with pharmacy_potential > 0.5: "If this development is built,
would a pharmacy here qualify?" Checks Items 130, 133, 134, 135, 136.
"""
import os
import sys
from typing import Any, Dict, List, Optional

# Add project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.models import Candidate, EvaluationResult
from engine.evaluator import evaluate_candidate
from engine.context import EvaluationContext
from engine.scoring import score_commercial

from candidates.council_da import get_das_for_evaluation

DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")
RELEVANT_ITEMS = {"Item 130", "Item 133", "Item 134", "Item 135", "Item 136"}


def _da_to_candidate(da: Dict[str, Any]) -> Candidate:
    """Build Candidate from council_da row for hypothetical evaluation."""
    dev_type = da.get("development_type") or "retail"
    gla = da.get("estimated_gla_sqm")
    desc = da.get("description") or ""

    complex_type = None
    if dev_type == "shopping_centre":
        complex_type = "large_sc" if (gla and gla >= 10000) or "50" in desc or "large" in desc.lower() else "small_sc"
    elif dev_type == "medical_centre":
        complex_type = "large_medical"
    elif dev_type == "retail" and "hospital" in desc.lower():
        complex_type = "private_hospital"

    estimated_tenants = None
    if gla and gla >= 5000:
        estimated_tenants = 50 if "50" in desc or "large" in desc.lower() else 15

    return Candidate(
        id=f"da_{da.get('da_number', da.get('id', ''))}",
        latitude=float(da["lat"]),
        longitude=float(da["lon"]),
        name=da.get("address", "")[:200] or "DA Development",
        address=da.get("address", ""),
        source_type="council_da",
        source_id=da.get("id"),
        state=da.get("state", ""),
        gla_sqm=gla,
        estimated_tenants=estimated_tenants,
        complex_type=complex_type,
    )


def evaluate_da(da: Dict[str, Any], context: EvaluationContext) -> Dict[str, Any]:
    """
    Run rules engine on a DA. Returns dict with da_number, qualifying_items,
    confidence, notes. Only considers Items 130, 133, 134, 135, 136.
    """
    candidate = _da_to_candidate(da)
    result = evaluate_candidate(candidate, context)

    qualifying = []
    for r in result.rule_results:
        if r.item in RELEVANT_ITEMS and r.passed:
            qualifying.append(r.item)

    if result.passed_any:
        result.commercial_score = score_commercial(result, context)

    notes = []
    for r in result.rule_results:
        if r.item in RELEVANT_ITEMS:
            status = "PASS" if r.passed else "FAIL"
            reasons = "; ".join(r.reasons[:2]) if r.reasons else "-"
            notes.append(f"{r.item}: {status} ({reasons})")

    return {
        "da_number": da.get("da_number", da.get("id")),
        "qualifying_items": qualifying,
        "confidence": result.best_confidence if qualifying else 0.0,
        "passed_any": result.passed_any,
        "commercial_score": result.commercial_score,
        "notes": " | ".join(notes),
    }


def evaluate_all_das(
    min_potential: float = 0.5,
    state: Optional[str] = None,
    db_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Evaluate all DAs with pharmacy_potential >= min_potential.
    Returns list of evaluation result dicts.
    """
    das = get_das_for_evaluation(min_potential=min_potential, state=state, db_path=db_path)
    if not das:
        return []

    context = EvaluationContext(db_path=db_path or DB_PATH)
    results = []
    for da in das:
        try:
            ev = evaluate_da(da, context)
            results.append(ev)
        except Exception as e:
            results.append({
                "da_number": da.get("da_number"),
                "qualifying_items": [],
                "confidence": 0.0,
                "notes": f"Error: {e}",
            })
    return results
