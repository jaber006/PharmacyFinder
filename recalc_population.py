"""
Recalculate pop_5km, pop_10km, and nearest_town for ALL opportunities in the DB.

Uses the same Overpass + OSM approach as population_overlay.py:
- Fetches populated places per state from Overpass (cached)
- Applies distance-decay population estimation
- Updates the opportunities table in pharmacy_finder.db

Usage:
    python recalc_population.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.database import Database
from utils.overpass_cache import cached_overpass_query, CACHE_DIR
from population_overlay import (
    get_population_data_for_state,
    estimate_nearby_population,
    calculate_opportunity_score,
)

# Extend cache expiry to 90 days — population data doesn't change fast
# and we don't want to get rate-limited re-fetching the same state data
import utils.overpass_cache as _oc
_oc.CACHE_EXPIRY_DAYS = 90


def main():
    sys.stdout.reconfigure(line_buffering=True)

    db = Database()
    db.connect()

    # 1. Load all opportunities
    opps = db.get_all_opportunities()
    print(f"Loaded {len(opps)} opportunities from DB")

    # 2. Group by region/state
    by_state = {}
    for opp in opps:
        state = opp.get('region', 'UNKNOWN')
        by_state.setdefault(state, []).append(opp)

    print(f"States: {', '.join(f'{s}({len(v)})' for s, v in sorted(by_state.items()))}")

    # 3. Process state by state (one Overpass query per state)
    changes = []
    total_updated = 0

    for state, state_opps in sorted(by_state.items()):
        print(f"\n{'='*60}")
        print(f"  {state} — {len(state_opps)} opportunities")
        print(f"{'='*60}")

        # Fetch OSM populated places for this state (cached)
        places = get_population_data_for_state(state)
        print(f"  Fetched {len(places)} populated places from OSM")
        places_real = [p for p in places if not p['estimated']]
        print(f"  {len(places_real)} with real population data, {len(places) - len(places_real)} estimated")

        for opp in state_opps:
            old_pop_5km = opp.get('pop_5km') or 0
            old_pop_10km = opp.get('pop_10km') or 0
            old_nearest_town = opp.get('nearest_town') or 'Unknown'

            # Recalculate using same method as population_overlay.py
            pop_data = estimate_nearby_population(
                opp['latitude'], opp['longitude'], places
            )

            new_pop_5km = pop_data['total_pop_5km']
            new_pop_10km = pop_data['total_pop_10km']
            new_pop_15km = pop_data['total_pop_15km']
            new_nearest_town = pop_data['nearest_town']

            # Calculate opportunity score
            nearest_km = opp.get('nearest_pharmacy_km') or 0
            confidence = opp.get('confidence') or 0
            new_score = calculate_opportunity_score(pop_data, nearest_km, confidence)

            # Track significant changes (>10% or >500 people)
            delta = abs(new_pop_10km - old_pop_10km)
            pct_change = (delta / max(old_pop_10km, 1)) * 100
            if delta > 500 or pct_change > 10:
                changes.append({
                    'id': opp['id'],
                    'town': new_nearest_town,
                    'state': state,
                    'old_pop_10km': old_pop_10km,
                    'new_pop_10km': new_pop_10km,
                    'delta': new_pop_10km - old_pop_10km,
                    'pct': pct_change,
                })

            # Update DB
            cursor = db.connection.cursor()
            cursor.execute("""
                UPDATE opportunities
                SET pop_5km = ?,
                    pop_10km = ?,
                    pop_15km = ?,
                    nearest_town = ?,
                    opp_score = ?
                WHERE id = ?
            """, (new_pop_5km, new_pop_10km, new_pop_15km,
                  new_nearest_town, new_score, opp['id']))
            total_updated += 1

        db.connection.commit()
        print(f"  Updated {len(state_opps)} opportunities")

    # 4. Report
    print(f"\n{'='*60}")
    print(f"  RECALCULATION COMPLETE")
    print(f"{'='*60}")
    print(f"  Total updated: {total_updated}")
    print(f"  Significant changes (>10% or >500 people): {len(changes)}")

    if changes:
        changes.sort(key=lambda c: abs(c['delta']), reverse=True)
        print(f"\n  {'ID':>6} {'Town':<25} {'State':<5} {'Old':>8} {'New':>8} {'Delta':>8} {'%':>7}")
        print(f"  {'-'*70}")
        for c in changes[:30]:
            sign = '+' if c['delta'] >= 0 else ''
            print(f"  {c['id']:>6} {c['town']:<25} {c['state']:<5} "
                  f"{c['old_pop_10km']:>8,} {c['new_pop_10km']:>8,} "
                  f"{sign}{c['delta']:>7,} {c['pct']:>6.1f}%")

    # 5. Print new top 20 leaderboard
    print(f"\n{'='*60}")
    print(f"  TOP 20 LEADERBOARD")
    print(f"{'='*60}")

    cursor = db.connection.cursor()
    rows = cursor.execute("""
        SELECT nearest_town, region, pop_10km, pharmacy_10km,
               CASE WHEN pharmacy_10km > 0 
                    THEN CAST(pop_10km AS REAL) / pharmacy_10km 
                    ELSE pop_10km END as people_per_pharmacy,
               nearest_pharmacy_km, opp_score
        FROM opportunities
        ORDER BY opp_score DESC
        LIMIT 20
    """).fetchall()

    print(f"\n  {'#':>3} {'Town':<25} {'State':<5} {'Pop 10km':>10} {'Pharm 10km':>10} "
          f"{'Ppl/Pharm':>10} {'Score':>8}")
    print(f"  {'-'*75}")
    for i, row in enumerate(rows, 1):
        town = (row[0] or 'Unknown')[:24]
        state = row[1] or '?'
        pop = row[2] or 0
        pharm = row[3] or 0
        ppp = row[4] or 0
        score = row[6] or 0
        print(f"  {i:>3} {town:<25} {state:<5} {pop:>10,} {pharm:>10} "
              f"{ppp:>10,.0f} {score:>8,.0f}")

    db.close()
    return total_updated, len(changes)


if __name__ == '__main__':
    main()
