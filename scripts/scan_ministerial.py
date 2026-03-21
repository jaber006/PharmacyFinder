"""
Ministerial Candidate Scanner
==============================
Loads FAILED v2_results from the database, runs ministerial discretion
scoring on borderline sites, and outputs the top candidates with gap analysis.

Usage: py -3.12 scripts/scan_ministerial.py
"""
import sys, os, json, sqlite3
from datetime import datetime

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from engine.context import EvaluationContext
from engine.models import Candidate, RuleResult
from engine.ministerial import (
    assess_ministerial_discretion,
    RULE_THRESHOLDS,
    MAX_GAP_PERCENTAGE,
    MIN_COMMUNITY_NEED,
)


def load_failed_results(db_path: str):
    """
    Load v2_results that have failed rules worth ministerial assessment.
    
    If there are fully-failed sites (passed_any = 0), use those.
    Otherwise, load ALL results and extract failed rules from each —
    even sites that passed one rule may have borderline failures on others.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Check if there are any fully-failed sites
    cur.execute("SELECT COUNT(*) FROM v2_results WHERE passed_any = 0")
    fully_failed = cur.fetchone()[0]
    
    if fully_failed > 0:
        print(f"  Found {fully_failed} fully-failed sites (passed_any = 0)")
        cur.execute("""
            SELECT * FROM v2_results 
            WHERE passed_any = 0 
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
        """)
    else:
        # All sites passed at least one rule — assess borderline failures anyway
        print("  No fully-failed sites. Scanning ALL results for borderline rule failures...")
        cur.execute("""
            SELECT * FROM v2_results 
            WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL
        """)
    
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def row_to_candidate(row: dict) -> Candidate:
    """Convert a v2_results row to a Candidate object."""
    return Candidate(
        id=row["id"],
        latitude=row["latitude"],
        longitude=row["longitude"],
        name=row.get("name", ""),
        address=row.get("address", ""),
        source_type=row.get("source_type", ""),
        state=row.get("state", ""),
    )


def parse_rule_results(row: dict) -> list[RuleResult]:
    """Parse all_rules_json from a v2_results row into RuleResult objects."""
    raw = row.get("all_rules_json", "[]")
    try:
        rules_data = json.loads(raw) if raw else []
    except json.JSONDecodeError:
        return []

    results = []
    for rd in rules_data:
        results.append(RuleResult(
            item=rd.get("item", ""),
            passed=rd.get("passed", False),
            reasons=rd.get("reasons", []),
            evidence_needed=rd.get("evidence_needed", []),
            confidence=rd.get("confidence", 0.0),
            distances=rd.get("distances", {}),
        ))
    return results


def main():
    print("=" * 70)
    print("MINISTERIAL CANDIDATE SCANNER")
    print(f"Run: {datetime.now().isoformat()}")
    print("=" * 70)

    db_path = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")

    # Load failed results
    print(f"\nLoading failed evaluations from {db_path}...")
    failed_rows = load_failed_results(db_path)
    print(f"  Found {len(failed_rows)} FAILED evaluations")

    # Load context for spatial queries
    print("\nLoading evaluation context...")
    context = EvaluationContext(db_path=db_path)

    # Process each failed result
    all_assessments = []
    candidates_checked = 0
    ministerial_count = 0

    print(f"\nAssessing {len(failed_rows)} failed sites for ministerial discretion...")

    for row in failed_rows:
        candidate = row_to_candidate(row)
        rule_results = parse_rule_results(row)

        # Filter to only failed rules that have thresholds defined
        failed_rules = [
            rr for rr in rule_results
            if not rr.passed
            and rr.item in RULE_THRESHOLDS
            and rr.distances  # need distance data for gap analysis
        ]

        if not failed_rules:
            continue

        candidates_checked += 1

        for rr in failed_rules:
            assessment = assess_ministerial_discretion(candidate, rr, context)
            assessment_dict = assessment.to_dict()
            assessment_dict["candidate_name"] = candidate.name
            assessment_dict["candidate_address"] = candidate.address
            assessment_dict["candidate_state"] = candidate.state
            assessment_dict["source_type"] = candidate.source_type

            if assessment.ministerial_candidate:
                ministerial_count += 1

            all_assessments.append(assessment_dict)

    # Sort: ministerial candidates first, then by smallest gap percentage
    def sort_key(a):
        is_candidate = 1 if a["ministerial_candidate"] else 0
        # Get the max gap percentage (lower is better)
        max_gap = 0
        for gap in a["gap_analyses"]:
            if not gap["passed"]:
                max_gap = max(max_gap, gap["gap_percentage"])
        return (-is_candidate, max_gap)

    all_assessments.sort(key=sort_key)

    print(f"\n{'=' * 70}")
    print(f"RESULTS:")
    print(f"  Candidates checked: {candidates_checked}")
    print(f"  Total assessments: {len(all_assessments)}")
    print(f"  MINISTERIAL CANDIDATES: {ministerial_count}")
    print(f"{'=' * 70}")

    # Print ministerial candidates
    candidates_list = [a for a in all_assessments if a["ministerial_candidate"]]
    if candidates_list:
        print(f"\n* MINISTERIAL CANDIDATES ({len(candidates_list)}):")
        print(f"{'-' * 70}")
        for i, a in enumerate(candidates_list, 1):
            print(f"\n#{i} - {a['candidate_name']}")
            print(f"  Address: {a['candidate_address']}")
            print(f"  State:   {a['candidate_state']} | Source: {a['source_type']}")
            print(f"  Rule:    {a['item']}")
            print(f"  Community Need: {a['community_need_score']:.2f} "
                  f"(Pop/Pharmacy: {a['population_per_pharmacy']:,.0f})")
            print(f"  Gap Analysis:")
            for gap in a["gap_analyses"]:
                status = "[PASS]" if gap["passed"] else "[FAIL]"
                print(f"    {status} {gap['threshold_name']}: "
                      f"needed {gap['threshold_value']}, got {gap['actual_value']:.4f} "
                      f"(gap: {gap['gap_percentage']:.1f}%)")
            print(f"  Precedent: {a['precedent_score']:.2f} - {a['precedent_notes'][:80]}")
    else:
        print("\n  No ministerial candidates found.")

    # Print borderline (close but not qualifying)
    borderline = [a for a in all_assessments 
                  if not a["ministerial_candidate"]
                  and any(g["gap_percentage"] < 25 and not g["passed"] for g in a["gap_analyses"])]
    if borderline:
        print(f"\n\nBORDERLINE SITES (gap < 25% but didn't qualify):")
        print(f"{'-' * 70}")
        for i, a in enumerate(borderline[:20], 1):
            max_gap = max(
                (g["gap_percentage"] for g in a["gap_analyses"] if not g["passed"]),
                default=0
            )
            print(f"  #{i} [{a['item']}] {a['candidate_name']} "
                  f"({a['candidate_state']}) — max gap: {max_gap:.1f}%, "
                  f"community need: {a['community_need_score']:.2f}")

    # Save JSON
    output_dir = os.path.join(PROJECT_ROOT, "output")
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(output_dir, "ministerial_candidates.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "scan_date": datetime.now().isoformat(),
            "total_failed_sites": len(failed_rows),
            "candidates_assessed": candidates_checked,
            "total_assessments": len(all_assessments),
            "ministerial_candidates_count": ministerial_count,
            "thresholds_used": {
                "max_gap_percentage": MAX_GAP_PERCENTAGE,
                "min_community_need": MIN_COMMUNITY_NEED,
            },
            "ministerial_candidates": [a for a in all_assessments if a["ministerial_candidate"]],
            "borderline_sites": borderline[:50] if borderline else [],
            "all_assessments": all_assessments,
        }, f, indent=2, ensure_ascii=False, default=str)
    print(f"\n[OK] Saved to {json_path}")

    # Summary stats
    print(f"\n{'=' * 70}")
    print("SUMMARY BY RULE ITEM:")
    from collections import Counter
    item_counts = Counter(a["item"] for a in all_assessments)
    for item, count in item_counts.most_common():
        cands = sum(1 for a in all_assessments if a["item"] == item and a["ministerial_candidate"])
        print(f"  {item}: {count} assessed, {cands} ministerial candidates")

    print("\nSUMMARY BY STATE:")
    state_counts = Counter(a["candidate_state"] for a in all_assessments)
    for state, count in state_counts.most_common():
        cands = sum(1 for a in all_assessments if a["candidate_state"] == state and a["ministerial_candidate"])
        print(f"  {state}: {count} assessed, {cands} ministerial candidates")


if __name__ == "__main__":
    main()
