"""
Post-compliance commercial scoring.
Only applied to candidates that pass at least one rule.

Final Score =
  0.35 × legal_robustness (margin above threshold)
  0.20 × demand_potential (pop_10km from DB)
  0.15 × gp_adjacency (GP count within 5km)
  0.10 × anchor_traffic (supermarket nearby)
  0.10 × lease_economics (placeholder 0.5)
  0.05 × parking_access (placeholder 0.5)
  0.05 × area_growth (growth_indicator from DB)
"""
from engine.models import EvaluationResult
from engine.context import EvaluationContext


# Max population for normalization (Sydney metro area proxy)
MAX_POP_10KM = 200_000


def score_commercial(result: EvaluationResult, context: EvaluationContext) -> float:
    """
    Calculate commercial score (0.0 - 1.0) for a passing candidate.
    Only call this if result.passed_any is True.
    """
    candidate = result.candidate

    # --- 1. Legal robustness (0.35) ---
    # Based on best confidence score among passing rules
    legal_robustness = result.best_confidence

    # --- 2. Demand potential (0.20) ---
    pop_10km = candidate.pop_10km or 0
    demand_potential = min(pop_10km / MAX_POP_10KM, 1.0)

    # --- 3. GP adjacency (0.15) ---
    gps_nearby = context.gps_within_radius(
        candidate.latitude, candidate.longitude, 5.0
    )
    gp_count = len(gps_nearby)
    # Normalize: 20+ GPs within 5km = max
    gp_adjacency = min(gp_count / 20.0, 1.0)

    # --- 4. Anchor traffic (0.10) ---
    supers_nearby = context.supermarkets_within_radius(
        candidate.latitude, candidate.longitude, 1.0
    )
    if supers_nearby:
        # Having a supermarket nearby is good for traffic
        anchor_traffic = min(len(supers_nearby) / 3.0, 1.0)
    else:
        anchor_traffic = 0.0

    # --- 5. Lease economics (0.10) — placeholder ---
    lease_economics = 0.5

    # --- 6. Parking access (0.05) — placeholder ---
    parking_access = 0.5

    # --- 7. Area growth (0.05) ---
    growth = candidate.growth_indicator or ""
    if "high" in growth.lower():
        area_growth = 0.9
    elif "medium" in growth.lower() or "moderate" in growth.lower():
        area_growth = 0.6
    elif growth:
        area_growth = 0.4
    else:
        area_growth = 0.3  # Unknown defaults to below-average

    # --- Weighted sum ---
    score = (
        0.35 * legal_robustness +
        0.20 * demand_potential +
        0.15 * gp_adjacency +
        0.10 * anchor_traffic +
        0.10 * lease_economics +
        0.05 * parking_access +
        0.05 * area_growth
    )

    return round(score, 4)
