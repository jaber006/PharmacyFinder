"""
Pipeline: generates candidates from POIs, runs evaluation, outputs results.

CLI: python -m engine.pipeline --state NSW   (single state)
     python -m engine.pipeline --all         (all states)
"""
import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from typing import List, Dict

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from engine.models import Candidate, EvaluationResult
from engine.context import EvaluationContext
from engine.evaluator import evaluate_candidate
from engine.scoring import score_commercial


STATES = ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'ACT']

DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")


def generate_candidates(context: EvaluationContext, state_filter: str = None) -> List[Candidate]:
    """
    Generate candidate points from existing POI locations:
    - Supermarkets (potential Item 130 sites — near supermarkets by definition)
    - Medical centres (potential Item 136 sites)
    - Shopping centres (potential Item 133 sites)
    - Hospitals (potential Item 135 sites)
    - Gaps between pharmacies (potential Item 131 rural sites)
    """
    candidates = []
    seen_coords = set()

    def _coord_key(lat, lon):
        return f"{lat:.5f},{lon:.5f}"

    def _add_candidate(lat, lon, name, address, source_type, source_id=None, state="", **kwargs):
        key = _coord_key(lat, lon)
        if key in seen_coords:
            return
        seen_coords.add(key)
        cid = f"{source_type}_{source_id or len(candidates)}"
        c = Candidate(
            id=cid, latitude=lat, longitude=lon,
            name=name, address=address,
            source_type=source_type, source_id=source_id,
            state=state, **kwargs,
        )
        candidates.append(c)

    # Load population data from opportunities table for enrichment
    pop_data = _load_population_data()

    # --- Supermarkets as candidates (Item 130 sites) ---
    for s in context.supermarkets:
        state = _infer_state(s, state_filter)
        if state_filter and state != state_filter:
            continue
        pop = _get_pop(pop_data, s['latitude'], s['longitude'])
        _add_candidate(
            s['latitude'], s['longitude'],
            f"Near {s['name']}", s.get('address', ''),
            source_type="supermarket", source_id=s['id'],
            state=state, pop_10km=pop.get('pop_10km', 0),
            growth_indicator=pop.get('growth_indicator', ''),
        )

    # --- Medical centres as candidates (Item 136 sites) ---
    for mc in context.medical_centres:
        state = mc.get('state', '') or _infer_state(mc, state_filter)
        if state_filter and state != state_filter:
            continue
        pop = _get_pop(pop_data, mc['latitude'], mc['longitude'])
        _add_candidate(
            mc['latitude'], mc['longitude'],
            mc['name'], mc.get('address', ''),
            source_type="medical_centre", source_id=mc['id'],
            state=state,
            num_gps=mc.get('num_gps', 0),
            total_fte=mc.get('total_fte', 0),
            hours_per_week=mc.get('hours_per_week', 0),
            pop_10km=pop.get('pop_10km', 0),
            growth_indicator=pop.get('growth_indicator', ''),
        )

    # --- Shopping centres as candidates (Item 133 sites) ---
    for sc in context.shopping_centres:
        state = _infer_state(sc, state_filter)
        if state_filter and state != state_filter:
            continue
        pop = _get_pop(pop_data, sc['latitude'], sc['longitude'])
        _add_candidate(
            sc['latitude'], sc['longitude'],
            sc['name'], sc.get('address', ''),
            source_type="shopping_centre", source_id=sc['id'],
            state=state,
            gla_sqm=sc.get('estimated_gla') or sc.get('gla_sqm'),
            estimated_tenants=sc.get('estimated_tenants'),
            centre_class=sc.get('centre_class'),
            pop_10km=pop.get('pop_10km', 0),
            growth_indicator=pop.get('growth_indicator', ''),
        )

    # --- Hospitals as candidates (Item 135 sites) ---
    for h in context.hospitals:
        state = _infer_state(h, state_filter)
        if state_filter and state != state_filter:
            continue
        pop = _get_pop(pop_data, h['latitude'], h['longitude'])
        _add_candidate(
            h['latitude'], h['longitude'],
            h['name'], h.get('address', ''),
            source_type="hospital", source_id=h['id'],
            state=state,
            bed_count=h.get('bed_count'),
            pop_10km=pop.get('pop_10km', 0),
            growth_indicator=pop.get('growth_indicator', ''),
        )

    # --- Gap analysis: midpoints between distant pharmacies (Item 131 rural) ---
    if not state_filter or state_filter in ('NT', 'WA', 'QLD', 'SA', 'TAS'):
        _add_gap_candidates(context, candidates, seen_coords, pop_data, state_filter)

    print(f"[Pipeline] Generated {len(candidates)} candidates"
          f"{f' for {state_filter}' if state_filter else ' nationally'}")
    return candidates


def _add_gap_candidates(context, candidates, seen_coords, pop_data, state_filter):
    """Add midpoint candidates between pharmacies that are far apart (rural gaps)."""
    # Find pharmacy pairs > 20km apart (potential Item 131 territory)
    pharmas = context.pharmacies
    if state_filter:
        pharmas = [p for p in pharmas if (p.get('state') or '') == state_filter]

    # Sort by latitude for efficient pair finding
    pharmas_sorted = sorted(pharmas, key=lambda p: (p['latitude'], p['longitude']))

    gap_count = 0
    max_gaps = 200  # Limit to avoid explosion

    for i, p1 in enumerate(pharmas_sorted):
        if gap_count >= max_gaps:
            break
        for j in range(i + 1, min(i + 10, len(pharmas_sorted))):
            p2 = pharmas_sorted[j]
            dist = context.geodesic_km(
                p1['latitude'], p1['longitude'],
                p2['latitude'], p2['longitude']
            )
            if 15 < dist < 100:  # Sweet spot for rural gaps
                mid_lat = (p1['latitude'] + p2['latitude']) / 2
                mid_lon = (p1['longitude'] + p2['longitude']) / 2

                key = f"{mid_lat:.5f},{mid_lon:.5f}"
                if key in seen_coords:
                    continue
                seen_coords.add(key)

                pop = _get_pop(pop_data, mid_lat, mid_lon)
                cid = f"gap_{p1['id']}_{p2['id']}"
                c = Candidate(
                    id=cid, latitude=mid_lat, longitude=mid_lon,
                    name=f"Gap: {p1['name']} ↔ {p2['name']}",
                    address=f"Midpoint between {p1.get('suburb','')} and {p2.get('suburb','')}",
                    source_type="gap",
                    state=p1.get('state', ''),
                    pop_10km=pop.get('pop_10km', 0),
                    growth_indicator=pop.get('growth_indicator', ''),
                )
                candidates.append(c)
                gap_count += 1


def _load_population_data() -> Dict:
    """Load population data from opportunities table for enrichment."""
    pop = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT latitude, longitude, pop_10km, growth_indicator FROM opportunities")
        for row in cur.fetchall():
            key = f"{row['latitude']:.4f},{row['longitude']:.4f}"
            pop[key] = {
                'pop_10km': row['pop_10km'] or 0,
                'growth_indicator': row['growth_indicator'] or '',
            }
        conn.close()
    except Exception:
        pass
    return pop


def _get_pop(pop_data: Dict, lat: float, lon: float) -> Dict:
    """Get population data for a coordinate, with fuzzy matching."""
    key = f"{lat:.4f},{lon:.4f}"
    if key in pop_data:
        return pop_data[key]
    # Try nearby keys (0.01 degree ≈ 1km)
    for dlat in (-0.01, 0, 0.01):
        for dlon in (-0.01, 0, 0.01):
            k = f"{lat+dlat:.4f},{lon+dlon:.4f}"
            if k in pop_data:
                return pop_data[k]
    return {'pop_10km': 0, 'growth_indicator': ''}


def _infer_state(poi: Dict, state_filter: str = None) -> str:
    """Infer state from POI data or address."""
    if poi.get('state'):
        return poi['state']
    addr = (poi.get('address') or '').upper()
    for st in STATES:
        if f', {st},' in addr or f', {st} ' in addr or addr.endswith(f', {st}'):
            return st
    # Rough coordinate-based fallback
    lat = poi.get('latitude', 0)
    lon = poi.get('longitude', 0)
    if lat > -20 and lon > 129 and lon < 138:
        return 'NT'
    if lat > -29 and lon > 138 and lon < 154:
        return 'QLD'
    if lat > -37 and lat < -28 and lon > 141:
        return 'NSW'
    if lat > -39.5 and lat < -34 and lon > 141 and lon < 150:
        return 'VIC'
    if lat < -40:
        return 'TAS'
    if lon < 129:
        return 'WA'
    if lon > 138 and lon < 141:
        return 'SA'
    return state_filter or ''


def run_pipeline(state_filter: str = None) -> List[EvaluationResult]:
    """Run the full pipeline: generate → evaluate → score → output."""
    print(f"\n{'='*60}")
    print(f"PharmacyFinder V2 Rules Engine Pipeline")
    print(f"State: {state_filter or 'ALL'}")
    print(f"Time: {datetime.now().isoformat()}")
    print(f"{'='*60}\n")

    # Load context (all reference data)
    context = EvaluationContext(db_path=DB_PATH)

    # Generate candidates
    candidates = generate_candidates(context, state_filter)

    # Evaluate each candidate
    results: List[EvaluationResult] = []
    passing = 0
    t0 = time.time()

    for i, candidate in enumerate(candidates):
        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            print(f"  Evaluated {i+1}/{len(candidates)} ({elapsed:.1f}s, {passing} passing)...")

        result = evaluate_candidate(candidate, context)

        if result.passed_any:
            # Score commercially
            result.commercial_score = score_commercial(result, context)
            passing += 1

        results.append(result)

    elapsed = time.time() - t0
    print(f"\n[Pipeline] Evaluated {len(candidates)} candidates in {elapsed:.1f}s")
    print(f"[Pipeline] {passing} candidates pass at least one rule")

    # Sort by commercial score (passing first)
    results.sort(key=lambda r: (-int(r.passed_any), -r.commercial_score))

    # Output results
    _output_results(results, state_filter)

    # Save to DB
    _save_to_db(results, state_filter)

    return results


def _output_results(results: List[EvaluationResult], state_filter: str):
    """Output results to JSON file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    passing = [r for r in results if r.passed_any]
    suffix = f"_{state_filter}" if state_filter else "_national"
    outfile = os.path.join(OUTPUT_DIR, f"v2_results{suffix}.json")

    data = {
        "generated": datetime.now().isoformat(),
        "state": state_filter or "ALL",
        "total_candidates": len(results),
        "total_passing": len(passing),
        "results": [r.to_dict() for r in passing],
    }

    with open(outfile, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"[Pipeline] Results written to {outfile} ({len(passing)} passing)")

    # Summary by rule
    rule_counts = {}
    for r in passing:
        for rr in r.passing_rules:
            rule_counts[rr.item] = rule_counts.get(rr.item, 0) + 1

    print("\n--- Results by Rule ---")
    for rule, count in sorted(rule_counts.items()):
        print(f"  {rule}: {count} sites")


def _save_to_db(results: List[EvaluationResult], state_filter: str):
    """Save v2 results to a new table in the DB."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Create v2 results table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS v2_results (
            id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            latitude REAL,
            longitude REAL,
            state TEXT,
            source_type TEXT,
            passed_any INTEGER,
            primary_rule TEXT,
            commercial_score REAL,
            best_confidence REAL,
            rules_json TEXT,
            all_rules_json TEXT,
            date_evaluated TEXT
        )
    """)

    # Clear old results for this state
    if state_filter:
        cur.execute("DELETE FROM v2_results WHERE state = ?", (state_filter,))
    else:
        cur.execute("DELETE FROM v2_results")

    # Insert passing results
    for r in results:
        if not r.passed_any:
            continue
        cur.execute("""
            INSERT OR REPLACE INTO v2_results
            (id, name, address, latitude, longitude, state, source_type,
             passed_any, primary_rule, commercial_score, best_confidence,
             rules_json, all_rules_json, date_evaluated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r.candidate.id,
            r.candidate.name,
            r.candidate.address,
            r.candidate.latitude,
            r.candidate.longitude,
            r.candidate.state,
            r.candidate.source_type,
            1,
            r.primary_rule,
            r.commercial_score,
            r.best_confidence,
            json.dumps([rr.to_dict() for rr in r.passing_rules]),
            json.dumps([rr.to_dict() for rr in r.rule_results]),
            datetime.now().isoformat(),
        ))

    conn.commit()
    count = cur.execute("SELECT COUNT(*) FROM v2_results").fetchone()[0]
    conn.close()
    print(f"[Pipeline] Saved {count} passing results to v2_results table")


def main():
    parser = argparse.ArgumentParser(description="PharmacyFinder V2 Rules Engine Pipeline")
    parser.add_argument("--state", type=str, help="State filter (e.g. NSW, VIC, TAS)")
    parser.add_argument("--all", action="store_true", help="Run for all states")
    args = parser.parse_args()

    if args.all:
        all_results = []
        for state in STATES:
            results = run_pipeline(state)
            all_results.extend([r for r in results if r.passed_any])
        # Also output combined national results
        print(f"\n{'='*60}")
        print(f"NATIONAL SUMMARY: {len(all_results)} total passing sites")
        _output_results(
            sorted(all_results, key=lambda r: -r.commercial_score),
            None,
        )
    elif args.state:
        run_pipeline(args.state.upper())
    else:
        # Default: run all
        run_pipeline()


if __name__ == "__main__":
    main()
