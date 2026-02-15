"""
Ministerial Opportunity Scanner

Scans ALL opportunities in the database that failed standard ACPA rules
(qualifying_rules = 'NONE', verification = 'NO_QUALIFYING_RULE') and
evaluates them for ministerial approval potential based on community need.

Usage:
    python run_ministerial_scan.py                  # Full scan
    python run_ministerial_scan.py --state TAS      # Tasmania only
    python run_ministerial_scan.py --top 50         # Top 50 results
    python run_ministerial_scan.py --min-score 40   # Higher threshold
"""
import argparse
import os
import sys
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

from utils.database import Database
from rules.item_ministerial import ItemMinisterialRule, MinisterialOpportunity
from utils.distance import format_distance
import config


def get_no_qualifying_opportunities(db: Database, state: str = None) -> List[Dict]:
    """Fetch all opportunities that failed standard rules."""
    cursor = db.connection.cursor()
    if state:
        cursor.execute(
            "SELECT * FROM opportunities WHERE qualifying_rules = 'NONE' AND region = ? ORDER BY id",
            (state.upper(),),
        )
    else:
        cursor.execute(
            "SELECT * FROM opportunities WHERE qualifying_rules = 'NONE' ORDER BY id"
        )
    return [dict(row) for row in cursor.fetchall()]


def update_opportunity_ministerial(db: Database, opp_id: int,
                                   score: float, evidence: str,
                                   categories: str):
    """Update an opportunity in the DB with ministerial findings."""
    cursor = db.connection.cursor()
    cursor.execute("""
        UPDATE opportunities
        SET qualifying_rules = 'Ministerial',
            evidence = ?,
            opp_score = ?,
            verification = 'MINISTERIAL',
            verification_notes = ?
        WHERE id = ?
    """, (evidence, score, f"Categories: {categories}", opp_id))
    db.connection.commit()


def generate_markdown_report(
    results: List[Tuple[Dict, MinisterialOpportunity]],
    scan_stats: Dict,
    output_path: str,
):
    """Generate the output/ministerial_opportunities.md report."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    lines = []
    lines.append("# Ministerial Pharmacy Opportunities Report")
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    lines.append("")

    # ── Summary ──────────────────────────────────────────────────
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"- **Total opportunities scanned:** {scan_stats['total_scanned']:,}")
    lines.append(f"- **Ministerial opportunities found:** {scan_stats['total_found']:,}")
    lines.append(f"- **Conversion rate:** {scan_stats['total_found'] / max(scan_stats['total_scanned'], 1) * 100:.1f}%")
    lines.append("")

    # By category
    lines.append("### By Category")
    lines.append("")
    lines.append("| Category | Description | Count |")
    lines.append("|----------|-------------|-------|")
    for cat, desc in [
        ("A", "Near-miss on standard rule distance"),
        ("B", "Underserviced high-population area"),
        ("C", "Medical need indicators"),
        ("D", "Growth corridor / poor pharmacy ratio"),
    ]:
        count = scan_stats['by_category'].get(cat, 0)
        lines.append(f"| {cat} | {desc} | {count} |")
    lines.append("")

    # By state
    lines.append("### By State")
    lines.append("")
    lines.append("| State | Count | Avg Score |")
    lines.append("|-------|-------|-----------|")
    for state in ['TAS', 'NSW', 'VIC', 'QLD', 'WA', 'SA', 'NT', 'ACT']:
        state_results = [r for r in results if r[0].get('region') == state]
        if state_results:
            avg_score = sum(r[1].score for r in state_results) / len(state_results)
            marker = " ⭐" if state == 'TAS' else ""
            lines.append(f"| {state}{marker} | {len(state_results)} | {avg_score:.1f} |")
    lines.append("")

    # ── Tasmania Focus ────────────────────────────────────────────
    tas_results = [r for r in results if r[0].get('region') == 'TAS']
    if tas_results:
        tas_results.sort(key=lambda x: x[1].score, reverse=True)
        lines.append("## 🏔️ Tasmania Opportunities")
        lines.append("")
        lines.append(f"**{len(tas_results)} ministerial opportunities found in Tasmania**")
        lines.append("")
        for opp_data, mopp in tas_results:
            lines.append(mopp.build_case_summary(opp_data))
            lines.append("")

    # ── Top 20 Nationally ────────────────────────────────────────
    results.sort(key=lambda x: x[1].score, reverse=True)
    lines.append("## 🏆 Top 20 Nationally")
    lines.append("")
    for i, (opp_data, mopp) in enumerate(results[:20], 1):
        lines.append(f"### #{i}")
        lines.append(mopp.build_case_summary(opp_data))
        lines.append("")

    # ── All Results by State ─────────────────────────────────────
    lines.append("## All Results by State")
    lines.append("")

    # TAS first, then alphabetically
    state_order = ['TAS'] + sorted(
        [s for s in set(r[0].get('region', '') for r in results) if s != 'TAS']
    )

    for state in state_order:
        state_results = [r for r in results if r[0].get('region') == state]
        if not state_results:
            continue
        state_name = config.AUSTRALIAN_STATES.get(state, state)
        state_results.sort(key=lambda x: x[1].score, reverse=True)

        lines.append(f"### {state} — {state_name} ({len(state_results)} opportunities)")
        lines.append("")
        lines.append("| # | Address | Score | Categories | Near-Miss | Nearest Pharmacy |")
        lines.append("|---|---------|-------|------------|-----------|-----------------|")

        for i, (opp_data, mopp) in enumerate(state_results, 1):
            address = opp_data.get('address', 'Unknown')[:60]
            nearest_km = opp_data.get('nearest_pharmacy_km', 0) or 0
            nearest_name = opp_data.get('nearest_pharmacy_name', 'Unknown')[:25]
            near_miss = mopp.near_miss_rule or "—"
            near_miss_short = near_miss.split("(")[0].strip()[:25] if near_miss != "—" else "—"
            lines.append(
                f"| {i} | {address} | {mopp.score:.1f} | {mopp.category_string} | "
                f"{near_miss_short} | {nearest_name} ({format_distance(nearest_km)}) |"
            )
        lines.append("")

    # Write
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))

    return output_path


def main():
    parser = argparse.ArgumentParser(description="Scan for ministerial pharmacy opportunities")
    parser.add_argument('--state', type=str, help="Filter by state (e.g. TAS, NSW)")
    parser.add_argument('--top', type=int, default=20, help="Number of top results to show (default: 20)")
    parser.add_argument('--min-score', type=float, default=25.0, help="Minimum score threshold (default: 25)")
    parser.add_argument('--dry-run', action='store_true', help="Don't update the database")
    parser.add_argument('--db', type=str, default='pharmacy_finder.db', help="Database path")
    args = parser.parse_args()

    print("=" * 70)
    print("  MINISTERIAL PHARMACY OPPORTUNITY SCANNER")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    # Connect
    db = Database(args.db)
    db.connect()

    # Get reference data stats
    stats = db.get_reference_data_stats()
    print("Reference data loaded:")
    for table, count in stats.items():
        print(f"  {table}: {count:,}")
    print()

    # Get opportunities that failed standard rules
    opps = get_no_qualifying_opportunities(db, args.state)
    print(f"Opportunities to scan: {len(opps)}")
    if args.state:
        print(f"  (filtered to {args.state.upper()})")
    print()

    # Initialize the ministerial rule
    rule = ItemMinisterialRule(db)

    # Scan
    results: List[Tuple[Dict, MinisterialOpportunity]] = []
    category_counts = defaultdict(int)
    state_counts = defaultdict(int)

    for i, opp in enumerate(opps):
        if (i + 1) % 100 == 0 or i == 0:
            print(f"  Scanning {i + 1}/{len(opps)}...")

        prop_data = {
            'latitude': opp['latitude'],
            'longitude': opp['longitude'],
            'pop_5km': opp.get('pop_5km', 0),
            'pop_10km': opp.get('pop_10km', 0),
            'pharmacy_5km': opp.get('pharmacy_5km', 0),
            'pharmacy_10km': opp.get('pharmacy_10km', 0),
            'growth_indicator': opp.get('growth_indicator', ''),
            'growth_details': opp.get('growth_details', ''),
        }

        mopp = rule.check_ministerial(prop_data)

        if mopp and mopp.score >= args.min_score:
            results.append((opp, mopp))

            for cat in mopp.categories:
                category_counts[cat] += 1
            state_counts[opp.get('region', 'Unknown')] += 1

            # Update DB unless dry-run
            if not args.dry_run:
                evidence = rule._build_evidence(mopp, prop_data,
                    {'name': opp.get('nearest_pharmacy_name', 'Unknown')},
                    opp.get('nearest_pharmacy_km', 0))
                update_opportunity_ministerial(
                    db, opp['id'], mopp.score, evidence, mopp.category_string
                )

    print(f"\nScan complete!")
    print(f"  Total scanned: {len(opps)}")
    print(f"  Ministerial opportunities: {len(results)}")
    print()

    # Stats
    scan_stats = {
        'total_scanned': len(opps),
        'total_found': len(results),
        'by_category': dict(category_counts),
        'by_state': dict(state_counts),
    }

    print("By category:")
    for cat in ['A', 'B', 'C', 'D']:
        print(f"  Category {cat}: {category_counts.get(cat, 0)}")

    print("\nBy state:")
    for state in ['TAS', 'NSW', 'VIC', 'QLD', 'WA', 'SA', 'NT', 'ACT']:
        count = state_counts.get(state, 0)
        if count > 0:
            marker = " *" if state == 'TAS' else ""
            print(f"  {state}: {count}{marker}")

    # Top results
    results.sort(key=lambda x: x[1].score, reverse=True)
    print(f"\nTop {min(args.top, len(results))} opportunities:")
    print("-" * 90)
    for i, (opp_data, mopp) in enumerate(results[:args.top], 1):
        address = opp_data.get('address', 'Unknown')[:55]
        region = opp_data.get('region', '??')
        nearest_km = opp_data.get('nearest_pharmacy_km', 0) or 0
        print(f"  {i:3d}. [{mopp.score:5.1f}] {region} | {address}")
        print(f"       Categories: {mopp.category_string} | Nearest pharmacy: {format_distance(nearest_km)}")
        if mopp.near_miss_rule:
            print(f"       Near-miss: {mopp.near_miss_rule}")
        print()

    # Generate markdown report
    output_path = os.path.join(config.OUTPUT_DIR, 'ministerial_opportunities.md')
    if results:
        report_path = generate_markdown_report(results, scan_stats, output_path)
        print(f"\nReport saved to: {report_path}")
    else:
        print("\nNo ministerial opportunities found — no report generated.")

    # TAS spotlight
    tas_results = [r for r in results if r[0].get('region') == 'TAS']
    if tas_results:
        print(f"\n{'='*70}")
        print(f"  TASMANIA SPOTLIGHT: {len(tas_results)} ministerial opportunities")
        print(f"{'='*70}")
        for i, (opp_data, mopp) in enumerate(tas_results[:10], 1):
            address = opp_data.get('address', 'Unknown')[:55]
            print(f"  {i}. [{mopp.score:.1f}] {address}")
            for reason in mopp.sub_reasons[:2]:
                print(f"     > {reason}")
            print()

    db.close()
    print("Done.")


if __name__ == '__main__':
    main()
