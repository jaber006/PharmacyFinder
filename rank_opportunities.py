#!/usr/bin/env python3
"""
Ranking Algorithm for PharmacyFinder Opportunities.

Creates a composite score (0-100) for each opportunity based on:
- Financial score (40%): ROI + profit potential
- Confidence score (20%): rule confidence level
- Population score (15%): catchment population
- Competition score (15%): fewer competitors = better
- Growth score (10%): population growth rate

Reads from financial_model_national.csv and outputs ranked_opportunities.csv.

Usage:
    python rank_opportunities.py
"""

import csv
import math
import os
import sys
from typing import Dict, List

# ================================================================
# SCORING WEIGHTS (must sum to 1.0)
# ================================================================

WEIGHTS = {
    'financial':   0.40,
    'confidence':  0.20,
    'population':  0.15,
    'competition': 0.15,
    'growth':      0.10,
}

assert abs(sum(WEIGHTS.values()) - 1.0) < 0.001, "Weights must sum to 1.0"

# ================================================================
# SCORING PARAMETERS
# ================================================================

# Financial score: combines ROI and profit
# ROI targets: 0% = 0 score, 30%+ = max score
ROI_MIN = 0.0
ROI_MAX = 0.30   # 30% ROI = perfect financial score

# Profit targets: $0 = 0, $200k+ = max
PROFIT_MIN = 0
PROFIT_MAX = 200_000

# NPV targets: $0 = 0, $1M+ = max
NPV_MIN = 0
NPV_MAX = 1_000_000

# Financial sub-weights (within the 40% financial allocation)
FINANCIAL_SUB = {
    'roi': 0.40,
    'profit': 0.35,
    'npv': 0.25,
}

# Population targets
POP_MIN = 0
POP_MAX = 30_000  # 30k+ within 5km = perfect population score

# Competition: pharmacy count within 5km
# 0 = perfect (100), 1 = 80, 2 = 60, 5+ = 0
COMP_SCORES = {
    0: 100,
    1: 80,
    2: 60,
    3: 40,
    4: 20,
}
COMP_DEFAULT = 5  # 5+ pharmacies


# ================================================================
# SCORING FUNCTIONS
# ================================================================

def normalize(value: float, min_val: float, max_val: float) -> float:
    """Normalize a value to 0-100 scale."""
    if max_val <= min_val:
        return 0.0
    score = (value - min_val) / (max_val - min_val) * 100
    return max(0.0, min(100.0, score))


def score_financial(row: Dict) -> float:
    """
    Score financial attractiveness (0-100).
    Combines ROI, profit, and NPV.
    """
    roi = float(row.get('roi', 0))
    profit = float(row.get('est_annual_profit', 0))
    npv = float(row.get('npv_10yr', 0))

    roi_score = normalize(roi, ROI_MIN, ROI_MAX)
    profit_score = normalize(profit, PROFIT_MIN, PROFIT_MAX)
    npv_score = normalize(npv, NPV_MIN, NPV_MAX)

    return (
        roi_score * FINANCIAL_SUB['roi'] +
        profit_score * FINANCIAL_SUB['profit'] +
        npv_score * FINANCIAL_SUB['npv']
    )


def score_confidence(row: Dict) -> float:
    """Score based on our rule confidence (0-100)."""
    conf_str = row.get('confidence', '50%')
    try:
        conf = float(conf_str.replace('%', '').strip())
    except (ValueError, AttributeError):
        conf = 50.0
    # Confidence is already 0-100ish. Map: 50% = 0, 100% = 100
    return max(0.0, min(100.0, (conf - 50) * 2))


def score_population(row: Dict) -> float:
    """Score based on catchment population (0-100)."""
    pop = float(row.get('catchment_pop', 0))
    return normalize(pop, POP_MIN, POP_MAX)


def score_competition(row: Dict) -> float:
    """Score based on competition — fewer = better (0-100)."""
    count = int(float(row.get('pharmacy_count_5km', 0)))
    if count in COMP_SCORES:
        return float(COMP_SCORES[count])
    return 0.0  # 5+ competitors


def score_growth(row: Dict) -> float:
    """Score based on growth corridor indicator (0-100)."""
    growth = row.get('growth_indicator', '').strip().upper()
    if growth == 'YES':
        return 100.0
    return 0.0  # Binary: in growth corridor or not


def calculate_composite_score(row: Dict) -> Dict:
    """Calculate all component scores and the weighted composite."""
    financial = score_financial(row)
    confidence = score_confidence(row)
    population = score_population(row)
    competition = score_competition(row)
    growth = score_growth(row)

    composite = (
        financial * WEIGHTS['financial'] +
        confidence * WEIGHTS['confidence'] +
        population * WEIGHTS['population'] +
        competition * WEIGHTS['competition'] +
        growth * WEIGHTS['growth']
    )

    return {
        'financial_score': round(financial, 1),
        'confidence_score': round(confidence, 1),
        'population_score': round(population, 1),
        'competition_score_rank': round(competition, 1),
        'growth_score': round(growth, 1),
        'composite_score': round(composite, 1),
    }


# ================================================================
# TIER CLASSIFICATION
# ================================================================

def classify_tier(composite: float, worst_profit: float) -> str:
    """Classify opportunity into tier based on composite score."""
    if composite >= 70 and worst_profit > 0:
        return 'A - Premium'
    elif composite >= 55:
        return 'B - Strong'
    elif composite >= 40:
        return 'C - Moderate'
    elif composite >= 25:
        return 'D - Marginal'
    else:
        return 'E - Weak'


def classify_opportunity_type(row: Dict) -> str:
    """Classify the opportunity type for quick filtering."""
    rules = row.get('qualifying_rules', '')
    if 'Item 136' in rules:
        return 'Medical Centre (Item 136)'
    elif 'Item 132' in rules:
        return 'Shopping Centre (Item 132)'
    elif 'Item 133' in rules:
        return 'Supermarket (Item 133)'
    elif 'Item 134' in rules and 'Item 134A' not in rules:
        return 'Small Centre (Item 134)'
    elif 'Item 131' in rules or 'Item 134A' in rules:
        return 'Remote/Rural'
    elif 'Item 130' in rules:
        return 'Distance (Item 130)'
    elif 'Item 135' in rules:
        return 'Hospital (Item 135)'
    else:
        return 'Other'


# ================================================================
# MAIN
# ================================================================

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')


def main():
    input_file = os.path.join(OUTPUT_DIR, 'financial_model_national.csv')
    output_file = os.path.join(OUTPUT_DIR, 'ranked_opportunities.csv')

    if not os.path.exists(input_file):
        print(f"ERROR: {input_file} not found. Run financial_model.py first.")
        sys.exit(1)

    # Read all modelled opportunities
    with open(input_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Loaded {len(rows)} opportunities from financial model.")

    # Score each opportunity
    scored = []
    for row in rows:
        scores = calculate_composite_score(row)
        worst_profit = float(row.get('worst_profit', 0))

        entry = {
            # Rank fields (will be filled after sorting)
            'national_rank': 0,
            'state_rank': 0,
            'tier': classify_tier(scores['composite_score'], worst_profit),
            'opportunity_type': classify_opportunity_type(row),

            # Scores
            **scores,

            # Key financial metrics
            'est_annual_revenue': row.get('est_annual_revenue', ''),
            'est_annual_profit': row.get('est_annual_profit', ''),
            'total_investment': row.get('total_investment', ''),
            'roi': row.get('roi', ''),
            'payback_years': row.get('payback_years', ''),
            'npv_10yr': row.get('npv_10yr', ''),

            # Sensitivity
            'worst_profit': row.get('worst_profit', ''),
            'worst_roi': row.get('worst_roi', ''),
            'best_profit': row.get('best_profit', ''),
            'best_roi': row.get('best_roi', ''),

            # Location
            'latitude': row.get('latitude', ''),
            'longitude': row.get('longitude', ''),
            'address': row.get('address', ''),
            'poi_name': row.get('poi_name', ''),
            'poi_type': row.get('poi_type', ''),
            'region': row.get('region', ''),
            'qualifying_rules': row.get('qualifying_rules', ''),
            'confidence': row.get('confidence', ''),
            'nearest_pharmacy_km': row.get('nearest_pharmacy_km', ''),
            'nearest_pharmacy_name': row.get('nearest_pharmacy_name', ''),

            # Context
            'catchment_pop': row.get('catchment_pop', ''),
            'effective_pop': row.get('effective_pop', ''),
            'pharmacy_count_5km': row.get('pharmacy_count_5km', ''),
            'market_share': row.get('market_share', ''),
            'est_annual_scripts': row.get('est_annual_scripts', ''),
            'gp_fte': row.get('gp_fte', ''),
            'growth_indicator': row.get('growth_indicator', ''),
            'growth_details': row.get('growth_details', ''),
        }
        scored.append(entry)

    # Sort by composite score descending
    scored.sort(key=lambda r: r['composite_score'], reverse=True)

    # Assign national rank
    for i, entry in enumerate(scored, 1):
        entry['national_rank'] = i

    # Assign state rank
    state_counters = {}
    for entry in scored:
        state = entry['region']
        state_counters[state] = state_counters.get(state, 0) + 1
        entry['state_rank'] = state_counters[state]

    # Write output
    fieldnames = list(scored[0].keys())
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(scored)

    print(f"Wrote {len(scored)} ranked opportunities to {output_file}")

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"RANKING SUMMARY")
    print(f"{'=' * 60}")

    # Tier distribution
    tiers = {}
    for entry in scored:
        t = entry['tier']
        tiers[t] = tiers.get(t, 0) + 1
    print(f"\nTier Distribution:")
    for tier in sorted(tiers.keys()):
        print(f"  {tier}: {tiers[tier]}")

    # Type distribution
    types = {}
    for entry in scored:
        t = entry['opportunity_type']
        types[t] = types.get(t, 0) + 1
    print(f"\nOpportunity Type Distribution:")
    for t, count in sorted(types.items(), key=lambda x: -x[1]):
        print(f"  {t}: {count}")

    # State distribution in top 50
    top50_states = {}
    for entry in scored[:50]:
        s = entry['region']
        top50_states[s] = top50_states.get(s, 0) + 1
    print(f"\nTop 50 by State:")
    for s, count in sorted(top50_states.items(), key=lambda x: -x[1]):
        print(f"  {s}: {count}")

    # Top 10 preview
    print(f"\nTop 10 Opportunities:")
    for entry in scored[:10]:
        loc = entry['address'] or entry['poi_name'] or f"{entry['latitude']},{entry['longitude']}"
        print(f"  #{entry['national_rank']:4d} | {entry['composite_score']:5.1f} | {entry['tier']:14s} | "
              f"ROI={float(entry['roi'] or 0):.0%} | {entry['region']} | {loc[:45]}")


if __name__ == '__main__':
    main()
