"""
Evaluation routes — evaluate addresses or existing sites against the rules engine.
"""
import json
import sqlite3
import os
import sys
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# Add project root to path for engine imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.models import Candidate, EvaluationResult
from engine.context import EvaluationContext
from engine.evaluator import evaluate_candidate
from engine.scoring import score_commercial

DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")

router = APIRouter(tags=["evaluate"])

# Lazy-loaded context (expensive — loads all reference data)
_context: Optional[EvaluationContext] = None


def _get_context() -> EvaluationContext:
    global _context
    if _context is None:
        _context = EvaluationContext(db_path=DB_PATH)
    return _context


class EvaluateRequest(BaseModel):
    address: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None


async def _geocode(address: str) -> dict:
    """Geocode an address using Nominatim (local first, then public fallback)."""
    params = {"q": address, "format": "json", "limit": 1, "countrycodes": "au"}

    # Try local Nominatim first
    for base_url in ["http://localhost:8088/search", "https://nominatim.openstreetmap.org/search"]:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                headers = {"User-Agent": "PharmacyFinder/3.0"}
                resp = await client.get(base_url, params=params, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        return {
                            "lat": float(data[0]["lat"]),
                            "lon": float(data[0]["lon"]),
                            "display_name": data[0].get("display_name", address),
                        }
        except Exception:
            continue

    raise HTTPException(status_code=400, detail=f"Could not geocode address: {address}")


def _candidate_from_coords(lat: float, lon: float, name: str = "", address: str = "") -> Candidate:
    """Create a Candidate from coordinates."""
    # Determine state from coordinates (rough bounding boxes)
    state = _guess_state(lat, lon)
    return Candidate(
        id=f"eval_{lat:.6f}_{lon:.6f}",
        latitude=lat,
        longitude=lon,
        name=name or f"Custom evaluation ({lat:.4f}, {lon:.4f})",
        address=address,
        state=state,
        source_type="manual",
    )


def _guess_state(lat: float, lon: float) -> str:
    """Rough state guess from coordinates."""
    if lat > -10:
        return "NT"
    if lat > -20:
        if lon < 138:
            return "NT"
        return "QLD"
    if lat > -29:
        if lon < 129:
            return "WA"
        if lon < 141:
            return "SA"
        if lon < 149:
            return "NSW"
        return "QLD"
    if lat > -34:
        if lon < 129:
            return "WA"
        if lon < 141:
            return "SA"
        return "NSW"
    if lat > -39:
        if lon < 129:
            return "WA"
        if lon < 141:
            return "SA"
        if lon < 147:
            return "VIC"
        return "NSW"
    if lat > -44:
        if lon < 144:
            return "VIC"
        return "TAS"
    return "TAS"


def _format_result(result: EvaluationResult) -> dict:
    """Format evaluation result for API response."""
    passing_rules = []
    all_rules = []
    for r in result.rule_results:
        rule_dict = r.to_dict()
        all_rules.append(rule_dict)
        if r.passed:
            passing_rules.append(rule_dict)

    response = {
        "id": result.candidate.id,
        "name": result.candidate.name,
        "address": result.candidate.address,
        "latitude": result.candidate.latitude,
        "longitude": result.candidate.longitude,
        "state": result.candidate.state,
        "passed_any": result.passed_any,
        "primary_rule": result.primary_rule,
        "commercial_score": round(result.commercial_score, 4),
        "best_confidence": round(result.best_confidence, 3),
        "qualifying_rules": passing_rules,
        "all_rules": all_rules,
    }

    # Risk assessment
    if hasattr(result, "risk_assessment") and result.risk_assessment:
        response["risk_assessment"] = result.risk_assessment
    
    # Ministerial potential
    if hasattr(result, "ministerial_assessments") and result.ministerial_assessments:
        response["ministerial_potential"] = result.ministerial_assessments

    return response


@router.post("/evaluate")
async def evaluate_address(req: EvaluateRequest):
    """Evaluate an address or coordinates against all pharmacy rules."""
    if req.address:
        geo = await _geocode(req.address)
        lat, lon = geo["lat"], geo["lon"]
        address = geo["display_name"]
    elif req.lat is not None and req.lon is not None:
        lat, lon = req.lat, req.lon
        address = f"{lat:.6f}, {lon:.6f}"
    else:
        raise HTTPException(status_code=400, detail="Provide 'address' or both 'lat' and 'lon'")

    ctx = _get_context()
    candidate = _candidate_from_coords(lat, lon, address=address)

    result = evaluate_candidate(candidate, ctx)

    # Compute commercial score
    if result.passed_any:
        result.commercial_score = score_commercial(result, ctx)

    response = _format_result(result)
    response["geocoded_address"] = address
    return response


@router.get("/evaluate/{site_id}")
async def evaluate_existing_site(site_id: str):
    """Evaluate an existing v2_results entry by its ID."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM v2_results WHERE id = ?", (site_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")

    row = dict(row)

    # Return stored evaluation data
    rules_json = json.loads(row.get("rules_json", "[]") or "[]")
    all_rules_json = json.loads(row.get("all_rules_json", "[]") or "[]")

    return {
        "id": row["id"],
        "name": row["name"],
        "address": row["address"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "state": row["state"],
        "passed_any": bool(row["passed_any"]),
        "primary_rule": row["primary_rule"],
        "commercial_score": row["commercial_score"],
        "best_confidence": row["best_confidence"],
        "qualifying_rules": rules_json,
        "all_rules": all_rules_json,
    }
