#!/usr/bin/env python3
"""
PharmacyFinder Scorer v3 — Improved scoring with real differentiation.

Key improvements over v2:
- Population within 10km heavily weighted (higher pop = better)
- NOT_VIABLE_REMOTE scores near 0
- VERIFIED opportunities get a boost
- Multiple qualifying rules > single rule
- Fewer nearby pharmacies = better
- Urban/suburban preferred over remote
- Score 0-100 with clear spread/differentiation
"""

import sqlite3
import json
import os
import math
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)


def score_opportunity(opp):
    """
    Score a single opportunity 0-100 with clear differentiation.
    
    Components (weights sum to 100):
    - Population density score (0-35): pop_10km relative to max
    - Pharmacy gap score (0-25): people per pharmacy ratio
    - Distance score (0-15): nearest pharmacy distance (sweet spot 2-20km)
    - Rule compliance score (0-15): based on verification status + rule count
    - Growth/urban score (0-10): urban/suburban preferred
    """
    verification = (opp['verification'] or '').upper()
    
    # Immediate filters: score near 0 for non-viable
    if 'NOT_VIABLE' in verification or 'REMOTE' in verification:
        return 1.0
    if verification in ('FAIL_PHARMACY_EXISTS', 'DUPLICATE', 'INVALID'):
        return 0.0
    
    pop_10km = opp['pop_10km'] or 0
    pop_5km = opp['pop_5km'] or 0
    pharmacy_10km = opp['pharmacy_10km'] or 0
    pharmacy_5km = opp['pharmacy_5km'] or 0
    nearest_km = opp['nearest_pharmacy_km'] or 999
    rules = opp['qualifying_rules'] or ''
    growth = (opp['growth_indicator'] or '').upper()
    
    # ===== 1. POPULATION SCORE (0-35) =====
    # Higher population = better opportunity
    if pop_10km >= 100000:
        pop_score = 35.0
    elif pop_10km >= 50000:
        pop_score = 30.0 + 5.0 * (pop_10km - 50000) / 50000
    elif pop_10km >= 20000:
        pop_score = 22.0 + 8.0 * (pop_10km - 20000) / 30000
    elif pop_10km >= 10000:
        pop_score = 15.0 + 7.0 * (pop_10km - 10000) / 10000
    elif pop_10km >= 5000:
        pop_score = 8.0 + 7.0 * (pop_10km - 5000) / 5000
    elif pop_10km >= 2000:
        pop_score = 3.0 + 5.0 * (pop_10km - 2000) / 3000
    elif pop_10km >= 500:
        pop_score = 1.0 + 2.0 * (pop_10km - 500) / 1500
    else:
        pop_score = pop_10km / 500.0  # 0-1 for very low pop
    
    # ===== 2. PHARMACY GAP SCORE (0-25) =====
    # Fewer pharmacies per capita = better opportunity
    if pharmacy_10km == 0:
        # No pharmacy within 10km — could be great or could be middle of nowhere
        if pop_10km >= 5000:
            gap_score = 25.0  # High pop, no pharmacy = gold
        elif pop_10km >= 2000:
            gap_score = 20.0
        elif pop_10km >= 500:
            gap_score = 12.0
        else:
            gap_score = 3.0  # Low pop, no pharmacy = remote
    else:
        people_per_pharmacy = pop_10km / pharmacy_10km
        if people_per_pharmacy >= 15000:
            gap_score = 25.0
        elif people_per_pharmacy >= 10000:
            gap_score = 20.0 + 5.0 * (people_per_pharmacy - 10000) / 5000
        elif people_per_pharmacy >= 7000:
            gap_score = 15.0 + 5.0 * (people_per_pharmacy - 7000) / 3000
        elif people_per_pharmacy >= 5000:
            gap_score = 10.0 + 5.0 * (people_per_pharmacy - 5000) / 2000
        elif people_per_pharmacy >= 3000:
            gap_score = 5.0 + 5.0 * (people_per_pharmacy - 3000) / 2000
        else:
            gap_score = 5.0 * people_per_pharmacy / 3000
    
    # ===== 3. DISTANCE SCORE (0-15) =====
    # Sweet spot: 2-20km from nearest pharmacy
    # Too close = saturated, too far = remote/unviable
    if nearest_km >= 200:
        dist_score = 1.0  # Very remote
    elif nearest_km >= 50:
        dist_score = 3.0  # Remote
    elif nearest_km >= 20:
        dist_score = 8.0 + 7.0 * (1 - (nearest_km - 20) / 30)  # Good rural
    elif nearest_km >= 10:
        dist_score = 15.0  # Perfect — rule 131 territory
    elif nearest_km >= 5:
        dist_score = 12.0  # Good — clear gap
    elif nearest_km >= 2:
        dist_score = 10.0  # Decent — not too close
    elif nearest_km >= 0.5:
        dist_score = 7.0  # Close but could work (shopping centre)
    elif nearest_km >= 0.2:
        dist_score = 5.0  # Very close, only works for specific rules
    else:
        dist_score = 2.0  # Basically on top of existing
    
    # ===== 4. RULE COMPLIANCE SCORE (0-15) =====
    # Count qualifying rules mentioned
    rule_items = []
    for item in ['131', '132', '133', '134A', '134', '135', '136', '130']:
        if item in rules:
            rule_items.append(item)
    
    num_rules = len(rule_items)
    
    # Verification status
    if verification == 'VERIFIED':
        rule_score = 12.0 + min(3.0, num_rules)  # 12-15
    elif verification == 'PHARMACY_EXISTS_NEARBY':
        rule_score = 3.0
    elif num_rules >= 3:
        rule_score = 10.0
    elif num_rules >= 2:
        rule_score = 8.0
    elif num_rules >= 1:
        rule_score = 5.0
    else:
        rule_score = 2.0
    
    # ===== 5. GROWTH/URBAN SCORE (0-10) =====
    # Prefer urban/suburban growth areas
    growth_score = 0.0
    
    # Population density proxy: high pop in small area = urban
    if pop_5km >= 20000:
        growth_score += 4.0  # Very urban
    elif pop_5km >= 10000:
        growth_score += 3.0  # Urban
    elif pop_5km >= 5000:
        growth_score += 2.0  # Suburban
    elif pop_5km >= 1000:
        growth_score += 1.0  # Semi-rural
    
    # Growth indicator
    if 'HIGH' in growth:
        growth_score += 4.0
    elif 'MEDIUM' in growth or 'MODERATE' in growth:
        growth_score += 3.0
    elif 'GROWTH' in growth:
        growth_score += 2.0
    elif 'LOW' in growth or 'STABLE' in growth:
        growth_score += 1.0
    
    # Bonus for having nearby supermarkets (proxy for commercial viability)
    # (We don't have direct access here, but poi_type can hint)
    poi_type = (opp.get('poi_type') or '').lower()
    if 'supermarket' in poi_type or 'shopping' in poi_type:
        growth_score += 2.0
    elif 'medical' in poi_type:
        growth_score += 1.0
    
    growth_score = min(10.0, growth_score)
    
    # ===== TOTAL =====
    total = pop_score + gap_score + dist_score + rule_score + growth_score
    total = round(min(100.0, max(0.0, total)), 1)
    
    return total


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    c.execute("""SELECT id, latitude, longitude, address, qualifying_rules, evidence,
                        nearest_pharmacy_km, nearest_pharmacy_name, poi_name, poi_type,
                        region, verification, pop_5km, pop_10km, pop_15km,
                        pharmacy_5km, pharmacy_10km, pharmacy_15km,
                        nearest_town, growth_indicator, growth_details,
                        composite_score, opp_score
                 FROM opportunities""")
    
    opps = [dict(row) for row in c.fetchall()]
    print(f"Scoring {len(opps)} opportunities with v3 algorithm...")
    
    scores = []
    for opp in opps:
        score = score_opportunity(opp)
        scores.append((score, opp['id'], opp.get('poi_name') or opp.get('address', '')[:50],
                       opp.get('verification', ''), opp.get('pop_10km', 0)))
        c.execute("UPDATE opportunities SET composite_score = ?, opp_score = ? WHERE id = ?",
                  (score, score, opp['id']))
    
    conn.commit()
    conn.close()
    
    # Sort and display
    scores.sort(key=lambda x: -x[0])
    
    print(f"\n{'='*90}")
    print(f"SCORING v3 COMPLETE — {len(scores)} opportunities scored")
    print(f"{'='*90}")
    
    # Score distribution
    brackets = {
        '90-100': 0, '80-89': 0, '70-79': 0, '60-69': 0,
        '50-59': 0, '40-49': 0, '30-39': 0, '20-29': 0,
        '10-19': 0, '0-9': 0
    }
    for s, *_ in scores:
        if s >= 90: brackets['90-100'] += 1
        elif s >= 80: brackets['80-89'] += 1
        elif s >= 70: brackets['70-79'] += 1
        elif s >= 60: brackets['60-69'] += 1
        elif s >= 50: brackets['50-59'] += 1
        elif s >= 40: brackets['40-49'] += 1
        elif s >= 30: brackets['30-39'] += 1
        elif s >= 20: brackets['20-29'] += 1
        elif s >= 10: brackets['10-19'] += 1
        else: brackets['0-9'] += 1
    
    print("\nScore distribution:")
    for bracket, count in brackets.items():
        bar = '#' * count
        print(f"  {bracket:>6}: {count:>3} {bar}")
    
    print(f"\nTop 20 opportunities:")
    print(f"{'#':<4} {'Score':<7} {'Verification':<25} {'Pop 10km':<10} {'Name'}")
    print('-' * 90)
    for i, (score, oid, name, verif, pop) in enumerate(scores[:20]):
        print(f"{i+1:<4} {score:<7.1f} {verif[:24]:<25} {pop:<10} {name[:45]}")
    
    # Save scored JSON
    scored_json = []
    for score, oid, name, verif, pop in scores:
        scored_json.append({
            'id': oid, 'score': score, 'name': name,
            'verification': verif, 'pop_10km': pop
        })
    with open(os.path.join(OUTPUT_DIR, 'scored_v3.json'), 'w', encoding='utf-8') as f:
        json.dump(scored_json, f, indent=2)
    print(f"\nSaved to output/scored_v3.json")


if __name__ == '__main__':
    main()
