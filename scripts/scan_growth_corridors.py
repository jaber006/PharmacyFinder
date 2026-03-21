#!/usr/bin/env python3
"""
Growth Corridor Scanner CLI.

Finds fast-growing suburbs with no pharmacy that are about to get retail
infrastructure — the NEXT Beveridge.

Usage:
    py -3.12 scripts/scan_growth_corridors.py --national
    py -3.12 scripts/scan_growth_corridors.py --state VIC
    py -3.12 scripts/scan_growth_corridors.py --top 20
    py -3.12 scripts/scan_growth_corridors.py --state VIC --top 10
"""

import argparse
import logging
import os
import sys

# Fix Windows console encoding for emoji
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from candidates.growth_corridors import scan_growth_corridors, AUSTRALIAN_STATES


def main():
    parser = argparse.ArgumentParser(
        description="Growth Corridor Scanner — find the next Beveridge",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  py -3.12 scripts/scan_growth_corridors.py --national          Scan all states
  py -3.12 scripts/scan_growth_corridors.py --state VIC         Scan Victoria only
  py -3.12 scripts/scan_growth_corridors.py --top 20            Show top 20 results
  py -3.12 scripts/scan_growth_corridors.py --state NSW --top 5 Top 5 in NSW
        """,
    )
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--national", action="store_true",
        help="Scan all states nationally",
    )
    group.add_argument(
        "--state", type=str, choices=AUSTRALIAN_STATES,
        help="Scan a specific state (e.g. VIC, NSW, QLD)",
    )
    
    parser.add_argument(
        "--top", type=int, default=20,
        help="Number of top results to display (default: 20)",
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Path to database file (default: pharmacy_finder.db)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose logging output",
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Minimal output — just the results",
    )
    
    args = parser.parse_args()
    
    # Default to national if nothing specified
    if not args.national and not args.state:
        args.national = True
    
    # Setup logging
    if args.quiet:
        log_level = logging.WARNING
    elif args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    
    # Run scan
    state_filter = args.state if args.state else None
    scope = args.state or "NATIONAL"
    
    print(f"\n{'='*70}")
    print(f"  GROWTH CORRIDOR SCANNER — {scope}")
    print(f"  Finding the next Beveridge...")
    print(f"{'='*70}\n")
    
    corridors = scan_growth_corridors(
        state_filter=state_filter,
        top_n=args.top,
        db_path=args.db,
    )
    
    if not corridors:
        print("No growth corridors found.")
        return
    
    # Display results
    hot = [c for c in corridors if c.classification == "HOT"]
    warm = [c for c in corridors if c.classification == "WARM"]
    watch = [c for c in corridors if c.classification == "WATCH"]
    
    print(f"\n{'='*70}")
    print(f"  RESULTS: {len(corridors)} corridors analysed")
    print(f"  🔥 HOT: {len(hot)}  |  🟡 WARM: {len(warm)}  |  👀 WATCH: {len(watch)}")
    print(f"{'='*70}\n")
    
    # Top N table
    top = corridors[:args.top]
    print(f"  TOP {min(args.top, len(corridors))} OPPORTUNITIES")
    print(f"  {'─'*66}")
    print(f"  {'#':>3}  {'Score':>5}  {'Class':>5}  {'Suburb':<25} {'State':<5} {'Pop':>7} {'Proj':>7} {'Growth':>6} {'Rx/10km':>7}")
    print(f"  {'─'*66}")
    
    for i, gc in enumerate(top, 1):
        emoji = "🔥" if gc.classification == "HOT" else "🟡" if gc.classification == "WARM" else "👀"
        pop_str = f"{gc.population_current:>7,}" if gc.population_current else "   n/a"
        proj_str = f"{gc.population_projected:>7,}" if gc.population_projected else "   n/a"
        growth_str = f"{gc.growth_rate_3yr:>5.0%}" if gc.growth_rate_3yr else "  n/a"
        rx_str = f"{gc.pharmacies_10km:>7}" if gc.pharmacies_10km < 999 else "      0"
        
        print(
            f"  {i:>3}  {gc.growth_score:>5.1f}  {emoji:>1}{gc.classification:>4}  "
            f"{gc.sa2_name:<25} {gc.state:<5} {pop_str} {proj_str} {growth_str} {rx_str}"
        )
    
    print(f"  {'─'*66}")
    
    # Show detail for top 5
    print(f"\n  TOP 5 DETAIL")
    for i, gc in enumerate(top[:5], 1):
        emoji = "🔥" if gc.classification == "HOT" else "🟡" if gc.classification == "WARM" else "👀"
        print(f"\n  {i}. {gc.sa2_name}, {gc.state} — {gc.growth_score}/100 {emoji}")
        print(f"     Pop: {gc.population_current:,} → {gc.population_projected:,} ({gc.growth_rate_3yr:.0%} in 3yr)")
        print(f"     Pharmacies: {gc.pharmacies_5km} within 5km, {gc.pharmacies_10km} within 10km")
        print(f"     Nearest pharmacy: {gc.nearest_pharmacy_km:.1f} km")
        print(f"     People/pharmacy (projected): {gc.people_per_pharmacy_projected:,.0f}")
        if gc.psp_name:
            print(f"     PSP: {gc.psp_name}")
        if gc.planned_dwellings:
            print(f"     Planned dwellings: {gc.planned_dwellings:,}")
        if gc.has_planned_retail:
            print(f"     Planned retail: {gc.planned_retail_count} development(s)")
        print(f"     Score: {gc.score_breakdown}")
    
    print(f"\n  Output files:")
    print(f"    📊 output/growth_corridors.json")
    print(f"    📋 output/growth_corridors.csv")
    print(f"    📝 output/growth_corridor_report.md")
    print()


if __name__ == "__main__":
    main()
