#!/usr/bin/env python3
"""
Financial Model for PharmacyFinder Opportunities.

Estimates revenue, costs, profit, ROI, payback period, and NPV
for each pharmacy opportunity zone identified by the scanner.

All assumptions are configurable constants at the top of the file.
Includes sensitivity analysis (worst / base / best case).

Usage:
    python financial_model.py           # Process all states
    python financial_model.py --state NSW
"""

import argparse
import csv
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ================================================================
# CONFIGURABLE FINANCIAL ASSUMPTIONS
# ================================================================

# --- Revenue Assumptions ---

# PBS scripts per capita per year (Australian average ~12-15)
SCRIPTS_PER_CAPITA_YEAR = 13  # conservative mid-range

# Market share by competition level (pharmacies within 5km)
MARKET_SHARE = {
    'sole':    0.80,   # No competitor within 5km
    'one':     0.40,   # 1 competitor within 5km
    'two':     0.30,   # 2 competitors within 5km
    'three_plus': 0.20,  # 3+ competitors within 5km (conservative)
}

# Average PBS dispensing fee per script ($AUD)
PBS_DISPENSING_FEE = 16.50  # ~$15-20 range, use conservative mid

# OTC / front-of-shop as fraction of total pharmacy revenue
# (PBS revenue is the "base", OTC adds on top)
OTC_REVENUE_FRACTION = 0.35  # 30-40%, use 35%

# Item 136 (medical centre co-location) bonus
# GPs >= 8 FTE adds script volume
ITEM_136_GP_THRESHOLD = 8
ITEM_136_SCRIPT_BONUS = 0.25  # 20-30% bonus, use 25%

# Catchment radius for revenue estimation (km)
# We use the 5km population data as primary catchment
PRIMARY_CATCHMENT_RADIUS = 5  # km

# Revenue cap per pharmacist FTE (reality check)
MAX_REVENUE_PER_PHARMACIST = 1_800_000  # single pharmacist can do ~$1.5-2M

# --- Cost Assumptions ---

# Fit-out cost for a new pharmacy
FITOUT_COST_BASE = 400_000  # $300-500k, use $400k base

# Initial stock investment
INITIAL_STOCK = 250_000  # $200-300k

# PBS approval and regulatory costs
APPROVAL_COSTS = 7_500  # $5-10k

# Annual rent by state ($/year) — conservative estimates
# Based on median retail rents for ~100-150sqm pharmacy space
STATE_ANNUAL_RENT = {
    'NSW': 85_000,
    'VIC': 75_000,
    'QLD': 65_000,
    'WA':  70_000,
    'SA':  55_000,
    'TAS': 45_000,
    'NT':  60_000,
    'ACT': 80_000,
}
DEFAULT_RENT = 65_000

# Staff costs (annual, including super + oncosts)
STAFF_COSTS = {
    'pharmacist':    95_000,   # $85-95k + super
    'dispense_tech': 62_000,   # $55-65k + super
    'retail':        55_000,   # $50-55k + super
}

# Minimum viable team
MIN_TEAM = {
    'pharmacist': 1,
    'dispense_tech': 1,
    'retail': 1,
}

# Additional pharmacist threshold — need 2nd pharmacist above this revenue
SECOND_PHARMACIST_REVENUE_THRESHOLD = 1_500_000

# Other annual operating costs
OTHER_ANNUAL_COSTS = {
    'insurance':       8_000,
    'utilities':       6_000,
    'software_it':     5_000,
    'marketing':       8_000,
    'accounting':      5_000,
    'sundry':         10_000,
}

# Annual stock replenishment as % of revenue (COGS)
COGS_FRACTION = 0.70  # Pharmacy COGS typically 68-72%

# --- NPV / Financial Assumptions ---
DISCOUNT_RATE = 0.10   # 10% discount rate
NPV_YEARS = 10         # 10-year horizon
REVENUE_GROWTH_RATE = 0.02  # 2% annual revenue growth (CPI-ish)

# --- Sensitivity Multipliers ---
SCENARIOS = {
    'worst': {
        'revenue_mult': 0.70,
        'cost_mult': 1.15,
        'label': 'Worst Case (70% rev, 115% cost)',
    },
    'base': {
        'revenue_mult': 1.00,
        'cost_mult': 1.00,
        'label': 'Base Case',
    },
    'best': {
        'revenue_mult': 1.30,
        'cost_mult': 0.90,
        'label': 'Best Case (130% rev, 90% cost)',
    },
}


# ================================================================
# HELPER FUNCTIONS
# ================================================================

def parse_confidence(conf_str: str) -> float:
    """Parse '95%' -> 0.95"""
    if not conf_str:
        return 0.5
    try:
        return float(conf_str.replace('%', '').strip()) / 100.0
    except (ValueError, AttributeError):
        return 0.5


def parse_int_safe(val: str, default: int = 0) -> int:
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def parse_float_safe(val: str, default: float = 0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def extract_gp_count(evidence: str) -> int:
    """Extract GP count from evidence text for Item 136 opportunities."""
    if not evidence:
        return 0
    # Pattern: "10 GPs (8.0 est. FTE)" or "8 GPs"
    m = re.search(r'(\d+)\s+GPs?\b', evidence)
    if m:
        return int(m.group(1))
    return 0


def extract_gp_fte(evidence: str) -> float:
    """Extract GP FTE from evidence text."""
    if not evidence:
        return 0.0
    m = re.search(r'(\d+\.?\d*)\s*est\.\s*FTE', evidence)
    if m:
        return float(m.group(1))
    return 0.0


def get_market_share(pharmacy_count_5km: int) -> float:
    """Determine market share based on nearby pharmacy count."""
    if pharmacy_count_5km == 0:
        return MARKET_SHARE['sole']
    elif pharmacy_count_5km == 1:
        return MARKET_SHARE['one']
    elif pharmacy_count_5km == 2:
        return MARKET_SHARE['two']
    else:
        return MARKET_SHARE['three_plus']


def calculate_total_investment() -> float:
    """Calculate total upfront investment needed."""
    return FITOUT_COST_BASE + INITIAL_STOCK + APPROVAL_COSTS


def calculate_annual_staff_cost(revenue: float) -> float:
    """Calculate annual staff cost based on minimum team + scale."""
    base_cost = sum(
        STAFF_COSTS[role] * count
        for role, count in MIN_TEAM.items()
    )
    # Add 2nd pharmacist if revenue warrants it
    if revenue > SECOND_PHARMACIST_REVENUE_THRESHOLD:
        base_cost += STAFF_COSTS['pharmacist']
    return base_cost


def calculate_annual_operating_costs(revenue: float, state: str) -> float:
    """Calculate total annual operating costs."""
    rent = STATE_ANNUAL_RENT.get(state, DEFAULT_RENT)
    staff = calculate_annual_staff_cost(revenue)
    other = sum(OTHER_ANNUAL_COSTS.values())
    cogs = revenue * COGS_FRACTION
    return rent + staff + other + cogs


def calculate_npv(annual_profit: float, investment: float,
                  discount_rate: float = DISCOUNT_RATE,
                  years: int = NPV_YEARS,
                  growth_rate: float = REVENUE_GROWTH_RATE) -> float:
    """Calculate Net Present Value over the horizon."""
    npv = -investment
    for year in range(1, years + 1):
        # Profit grows modestly each year
        year_profit = annual_profit * ((1 + growth_rate) ** (year - 1))
        npv += year_profit / ((1 + discount_rate) ** year)
    return npv


def estimate_revenue(row: Dict) -> Tuple[float, Dict]:
    """
    Estimate annual revenue for an opportunity.
    Returns (revenue, details_dict).
    """
    pop_5km = parse_int_safe(row.get('Pop 5km', '0'))
    pharmacy_count_5km = parse_int_safe(row.get('Pharmacy Count 5km', '0'))
    rules = row.get('Qualifying Rules', '')
    evidence = row.get('Evidence', '')
    nearest_km = parse_float_safe(row.get('Nearest Pharmacy (km)', '0'))

    # For very remote areas (Item 131/134A), use larger catchment
    is_remote = 'Item 131' in rules or 'Item 134A' in rules
    if is_remote:
        # Remote areas: use 10km or 15km population
        pop = parse_int_safe(row.get('Pop 10km', '0'))
        if pop <= 0:
            pop = parse_int_safe(row.get('Pop 15km', '0'))
        if pop <= 0:
            # Fallback: use nearest town population
            pop = parse_int_safe(row.get('Nearest Town Pop', '0'))
        if pop <= 0:
            pop = 500  # absolute minimum for remote
    else:
        pop = pop_5km
        if pop <= 0:
            pop = parse_int_safe(row.get('Nearest Town Pop', '0'))
        if pop <= 0:
            pop = 1000  # minimum fallback

    # Cap population for revenue calc — a single pharmacy can't serve millions
    effective_pop = min(pop, 50_000)

    # Market share
    market_share = get_market_share(pharmacy_count_5km)

    # PBS script revenue
    scripts = effective_pop * SCRIPTS_PER_CAPITA_YEAR * market_share
    pbs_revenue = scripts * PBS_DISPENSING_FEE

    # Item 136 bonus (medical centre co-location)
    item_136_bonus = 0.0
    gp_fte = extract_gp_fte(evidence)
    if 'Item 136' in rules and gp_fte >= ITEM_136_GP_THRESHOLD:
        item_136_bonus = pbs_revenue * ITEM_136_SCRIPT_BONUS

    total_pbs = pbs_revenue + item_136_bonus

    # OTC / front-of-shop revenue
    # OTC_REVENUE_FRACTION is fraction of TOTAL revenue, so:
    # total = pbs / (1 - otc_fraction)
    total_revenue = total_pbs / (1.0 - OTC_REVENUE_FRACTION)

    # Reality check: cap at max per pharmacist
    num_pharmacists = 1
    if total_revenue > SECOND_PHARMACIST_REVENUE_THRESHOLD:
        num_pharmacists = 2
    total_revenue = min(total_revenue, MAX_REVENUE_PER_PHARMACIST * num_pharmacists)

    details = {
        'catchment_pop': pop,
        'effective_pop': effective_pop,
        'market_share': market_share,
        'est_scripts': int(scripts),
        'pbs_revenue': round(pbs_revenue, 2),
        'item_136_bonus': round(item_136_bonus, 2),
        'otc_revenue': round(total_revenue - total_pbs, 2),
        'gp_fte': gp_fte,
        'is_remote': is_remote,
        'pharmacists_needed': num_pharmacists,
    }

    return round(total_revenue, 2), details


def model_opportunity(row: Dict) -> Dict:
    """
    Run the full financial model for a single opportunity.
    Returns dict with all financial metrics + sensitivity analysis.
    """
    state = row.get('Region', 'NSW')
    rules = row.get('Qualifying Rules', '')
    confidence = parse_confidence(row.get('Confidence', '50%'))

    # Revenue estimation
    base_revenue, rev_details = estimate_revenue(row)

    # Total investment
    investment = calculate_total_investment()

    # Build results for each scenario
    scenarios = {}
    for scenario_key, scenario in SCENARIOS.items():
        rev = base_revenue * scenario['revenue_mult']
        costs = calculate_annual_operating_costs(rev, state) * scenario['cost_mult']
        profit = rev - costs
        roi = profit / investment if investment > 0 else 0.0
        payback = investment / profit if profit > 0 else float('inf')
        npv = calculate_npv(
            annual_profit=profit,
            investment=investment,
        )

        scenarios[scenario_key] = {
            'annual_revenue': round(rev, 2),
            'annual_costs': round(costs, 2),
            'annual_profit': round(profit, 2),
            'roi': round(roi, 4),
            'payback_years': round(payback, 2) if payback != float('inf') else 999.0,
            'npv_10yr': round(npv, 2),
        }

    # Base case is the primary output
    base = scenarios['base']

    result = {
        # Location identifiers
        'latitude': row.get('Latitude', ''),
        'longitude': row.get('Longitude', ''),
        'address': row.get('Address', ''),
        'poi_name': row.get('POI Name', ''),
        'poi_type': row.get('POI Type', ''),
        'region': state,
        'qualifying_rules': rules,
        'confidence': row.get('Confidence', ''),
        'nearest_pharmacy_km': row.get('Nearest Pharmacy (km)', ''),
        'nearest_pharmacy_name': row.get('Nearest Pharmacy Name', ''),

        # Population & competition context
        'catchment_pop': rev_details['catchment_pop'],
        'effective_pop': rev_details['effective_pop'],
        'pharmacy_count_5km': parse_int_safe(row.get('Pharmacy Count 5km', '0')),
        'pharmacy_count_10km': parse_int_safe(row.get('Pharmacy Count 10km', '0')),
        'market_share': rev_details['market_share'],
        'est_annual_scripts': rev_details['est_scripts'],
        'gp_fte': rev_details['gp_fte'],
        'is_remote': rev_details['is_remote'],

        # Growth
        'growth_indicator': row.get('Growth Indicator', ''),
        'growth_details': row.get('Growth Details', ''),

        # Financial — Base Case
        'est_annual_revenue': base['annual_revenue'],
        'est_annual_costs': base['annual_costs'],
        'est_annual_profit': base['annual_profit'],
        'total_investment': investment,
        'roi': base['roi'],
        'payback_years': base['payback_years'],
        'npv_10yr': base['npv_10yr'],

        # Sensitivity — Worst Case
        'worst_revenue': scenarios['worst']['annual_revenue'],
        'worst_profit': scenarios['worst']['annual_profit'],
        'worst_roi': scenarios['worst']['roi'],
        'worst_npv': scenarios['worst']['npv_10yr'],

        # Sensitivity — Best Case
        'best_revenue': scenarios['best']['annual_revenue'],
        'best_profit': scenarios['best']['annual_profit'],
        'best_roi': scenarios['best']['roi'],
        'best_npv': scenarios['best']['npv_10yr'],
    }

    return result


# ================================================================
# MAIN PROCESSING
# ================================================================

STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')


def process_state(state: str) -> List[Dict]:
    """Process all opportunities for a state, return list of modelled results."""
    input_file = os.path.join(OUTPUT_DIR, f'population_ranked_{state}.csv')
    if not os.path.exists(input_file):
        print(f"  [SKIP] No data file for {state}")
        return []

    results = []
    with open(input_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            result = model_opportunity(row)
            results.append(result)

    return results


def write_results(all_results: List[Dict], output_path: str):
    """Write all modelled results to CSV."""
    if not all_results:
        print("  No results to write.")
        return

    fieldnames = list(all_results[0].keys())
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    print(f"  Wrote {len(all_results)} results to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Financial model for pharmacy opportunities')
    parser.add_argument('--state', type=str, help='Process a single state (e.g. NSW)')
    args = parser.parse_args()

    states_to_process = [args.state.upper()] if args.state else STATES
    all_results = []

    print("=" * 60)
    print("PHARMACY OPPORTUNITY FINANCIAL MODEL")
    print("=" * 60)
    print(f"\nAssumptions:")
    print(f"  Scripts/capita/year: {SCRIPTS_PER_CAPITA_YEAR}")
    print(f"  PBS dispensing fee:  ${PBS_DISPENSING_FEE:.2f}")
    print(f"  OTC fraction:       {OTC_REVENUE_FRACTION:.0%}")
    print(f"  COGS fraction:      {COGS_FRACTION:.0%}")
    print(f"  Discount rate:      {DISCOUNT_RATE:.0%}")
    print(f"  Fit-out cost:       ${FITOUT_COST_BASE:,.0f}")
    print(f"  Initial stock:      ${INITIAL_STOCK:,.0f}")
    print(f"  Total investment:   ${calculate_total_investment():,.0f}")
    print()

    for state in states_to_process:
        print(f"Processing {state}...")
        results = process_state(state)
        if results:
            # Write per-state file
            state_output = os.path.join(OUTPUT_DIR, f'financial_model_{state}.csv')
            write_results(results, state_output)
            all_results.extend(results)

    # Write combined national file
    if all_results:
        national_output = os.path.join(OUTPUT_DIR, 'financial_model_national.csv')
        write_results(all_results, national_output)

        # Summary stats
        profitable = [r for r in all_results if r['est_annual_profit'] > 0]
        high_roi = [r for r in all_results if r['roi'] > 0.20]
        positive_npv = [r for r in all_results if r['npv_10yr'] > 0]

        print(f"\n{'=' * 60}")
        print(f"SUMMARY")
        print(f"{'=' * 60}")
        print(f"  Total opportunities modelled: {len(all_results)}")
        print(f"  Profitable (base case):       {len(profitable)}")
        print(f"  ROI > 20%:                    {len(high_roi)}")
        print(f"  Positive NPV (10yr):          {len(positive_npv)}")

        if profitable:
            avg_profit = sum(r['est_annual_profit'] for r in profitable) / len(profitable)
            avg_roi = sum(r['roi'] for r in profitable) / len(profitable)
            print(f"  Avg profit (profitable):      ${avg_profit:,.0f}")
            print(f"  Avg ROI (profitable):         {avg_roi:.1%}")

        # Top 5 by ROI
        by_roi = sorted(all_results, key=lambda r: r['roi'], reverse=True)[:5]
        print(f"\n  Top 5 by ROI:")
        for i, r in enumerate(by_roi, 1):
            loc = r['address'] or r['poi_name'] or f"{r['latitude']},{r['longitude']}"
            print(f"    {i}. {loc[:50]:50s} ROI={r['roi']:.1%} Profit=${r['est_annual_profit']:,.0f}")

    print(f"\nDone.")


if __name__ == '__main__':
    main()
