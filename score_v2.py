#!/usr/bin/env python3
"""
PharmacyFinder Scorer v2 - People Per Pharmacy

Core metric: Population / Pharmacies = how underserviced an area is.
Secondary: Shopping centre size vs pharmacies inside.

Filters:
- Must have qualifying ACPA rule (130-136)
- Minimum population 5,000 (10km radius)
- 0 pharmacies excluded unless growth corridor flagged
- Ratio > 4,000 people/pharmacy = opportunity (Aus avg ~4,500)
"""

import sqlite3
import json
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')

# Australian average is ~1 pharmacy per 4,000-5,000 people
# Anything above this = underserviced
AUS_AVG_RATIO = 4500

# Minimum population to consider (10km radius)
MIN_POP_10KM = 500

# Minimum ratio to flag as opportunity
MIN_RATIO = 500


def score_opportunities():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""SELECT * FROM opportunities 
                   WHERE qualifying_rules IS NOT NULL AND qualifying_rules != 'NONE'
                   AND (verification IS NULL OR verification != 'FALSE POSITIVE')
                   ORDER BY pop_10km DESC""")
    
    scored = []
    for row in cur.fetchall():
        opp = dict(row)
        pop = opp['pop_10km'] or 0
        pharmacies = opp['pharmacy_10km'] or 0
        rules = opp['qualifying_rules'] or ''
        
        # --- FILTERS ---
        # Must have population
        if pop < MIN_POP_10KM:
            continue
        
        # For 0 pharmacy areas: only include if verified (deep verify already filtered junk)
        if pharmacies == 0:
            verification = opp.get('verification') or ''
            if verification != 'VERIFIED':
                continue
        
        # --- CORE METRIC: People per pharmacy ---
        if pharmacies > 0:
            ratio = pop / pharmacies
        else:
            # Growth corridor with 0 pharmacies - use population as ratio
            ratio = pop
        
        # Skip if area is well-serviced
        if ratio < MIN_RATIO:
            continue
        
        # --- SCORE (0-100) ---
        # Base score from ratio (log scale so mega-cities don't dominate)
        import math
        # ratio of 4,000 = score 0, ratio of 50,000+ = score 100
        ratio_score = min(100, max(0, (math.log(ratio / MIN_RATIO) / math.log(50000 / MIN_RATIO)) * 100))
        
        # --- BONUSES ---
        bonus = 0
        
        # Shopping centre bonus (Item 132/134)
        if '132' in rules or '134' in rules:
            bonus += 15  # Big centre = guaranteed foot traffic
        
        # Medical centre bonus (Item 136) - scripts flow directly
        if '136' in rules:
            bonus += 10
        
        # Multiple qualifying rules = stronger case
        rule_count = len([r for r in rules.split(',') if r.strip().startswith('Item')])
        if rule_count >= 2:
            bonus += 10
        
        # Growth corridor bonus
        growth = opp.get('growth_indicator') or ''
        if growth:
            bonus += 5
        
        # Population density bonus (more people = bigger market)
        if pop > 100000:
            bonus += 10
        elif pop > 50000:
            bonus += 5
        
        total_score = min(100, ratio_score + bonus)
        
        scored.append({
            'id': opp['id'],
            'name': opp['poi_name'],
            'state': opp['region'],
            'pop_10km': pop,
            'pharmacies_10km': pharmacies,
            'ratio': round(ratio),
            'nearest_pharmacy_km': opp['nearest_pharmacy_km'],
            'nearest_pharmacy': opp['nearest_pharmacy_name'],
            'rules': rules,
            'score': round(total_score, 1),
            'ratio_score': round(ratio_score, 1),
            'bonus': bonus,
            'growth': growth,
            'lat': opp['latitude'],
            'lng': opp['longitude'],
            'address': opp['address'] or '',
            'evidence': (opp['evidence'] or '')[:200],
        })
    
    # Sort by score descending, then ratio
    scored.sort(key=lambda x: (-x['score'], -x['ratio']))
    
    conn.close()
    return scored


def print_results(scored):
    print("=" * 120)
    print("PHARMACYFINDER v2 - UNDERSERVICED AREA RANKINGS")
    print(f"Metric: People per Pharmacy (10km radius) | Min pop: {MIN_POP_10KM:,} | Min ratio: {MIN_RATIO:,}")
    print(f"Australian average: ~{AUS_AVG_RATIO:,} people per pharmacy")
    print("=" * 120)
    print()
    
    print(f"{'#':<4} {'Score':<7} {'Name':<40} {'ST':<4} {'Pop 10km':<10} {'Pharma':<7} {'Ratio':<8} {'Near Ph km':<11} {'Rules'}")
    print("-" * 120)
    
    for i, o in enumerate(scored[:50], 1):
        marker = ""
        if o['ratio'] > 20000:
            marker = " [HOT]"
        elif o['ratio'] > 10000:
            marker = " [STRONG]"
        elif o['ratio'] > AUS_AVG_RATIO:
            marker = " [OK]"
        
        print(f"{i:<4} {o['score']:<7} {o['name'][:39]:<40} {o['state']:<4} {o['pop_10km']:<10,} {o['pharmacies_10km']:<7} {o['ratio']:<8,} {o['nearest_pharmacy_km']:<11.1f} {o['rules']}{marker}")
    
    print()
    print(f"Total qualifying opportunities: {len(scored)}")
    print(f"Avg ratio: {sum(o['ratio'] for o in scored) / len(scored):,.0f} people/pharmacy" if scored else "No results")
    
    # State breakdown
    print("\nBy state:")
    states = {}
    for o in scored:
        states[o['state']] = states.get(o['state'], 0) + 1
    for s, c in sorted(states.items(), key=lambda x: -x[1]):
        print(f"  {s}: {c}")


def update_db(scored):
    """Update the composite_score in the database with new v2 scores."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Reset all scores first
    cur.execute("UPDATE opportunities SET composite_score = 0")
    
    # Update scored ones
    for o in scored:
        cur.execute("UPDATE opportunities SET composite_score = ?, opp_score = ? WHERE id = ?",
                    (o['score'], o['ratio'], o['id']))
    
    conn.commit()
    conn.close()
    print(f"\nUpdated {len(scored)} opportunity scores in database.")


if __name__ == '__main__':
    scored = score_opportunities()
    print_results(scored)
    
    # Save JSON for dashboard
    with open(os.path.join('output', 'scored_v2.json'), 'w') as f:
        json.dump(scored, f, indent=2)
    print(f"\nSaved to output/scored_v2.json")
    
    # Update database
    update_db(scored)
