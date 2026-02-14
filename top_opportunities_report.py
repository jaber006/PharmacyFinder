#!/usr/bin/env python3
"""
Top Opportunities Report Generator for PharmacyFinder.

Generates clean reports from ranked_opportunities.csv:
- Top 50 opportunities nationally
- Top 10 per state
- Best Item 136 (medical centre) opportunities
- Best greenfield (Item 130/131) opportunities
- Quick wins (high confidence + low competition)

Outputs both CSV extracts and a comprehensive markdown report.

Usage:
    python top_opportunities_report.py
"""

import csv
import os
import sys
from datetime import datetime
from typing import Dict, List

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']


def load_ranked() -> List[Dict]:
    """Load ranked opportunities."""
    path = os.path.join(OUTPUT_DIR, 'ranked_opportunities.csv')
    if not os.path.exists(path):
        print(f"ERROR: {path} not found. Run rank_opportunities.py first.")
        sys.exit(1)
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def fmt_money(val) -> str:
    """Format a number as currency."""
    try:
        v = float(val)
        if v < 0:
            return f"-${abs(v):,.0f}"
        return f"${v:,.0f}"
    except (ValueError, TypeError):
        return str(val)


def fmt_pct(val) -> str:
    """Format a decimal as percentage."""
    try:
        return f"{float(val):.1%}"
    except (ValueError, TypeError):
        return str(val)


def fmt_loc(row: Dict, max_len: int = 55) -> str:
    """Format location string."""
    addr = row.get('address', '').strip()
    poi = row.get('poi_name', '').strip()
    lat = row.get('latitude', '')
    lon = row.get('longitude', '')

    if addr:
        loc = addr
    elif poi:
        loc = poi
    else:
        loc = f"{lat}, {lon}"

    if len(loc) > max_len:
        loc = loc[:max_len - 3] + '...'
    return loc


def opportunity_row_md(row: Dict, rank_field: str = 'national_rank') -> str:
    """Format a single opportunity as a markdown table row."""
    rank = row.get(rank_field, '')
    loc = fmt_loc(row, 40)
    state = row.get('region', '')
    score = row.get('composite_score', '')
    tier = row.get('tier', '')
    rules = row.get('qualifying_rules', '')
    revenue = fmt_money(row.get('est_annual_revenue', 0))
    profit = fmt_money(row.get('est_annual_profit', 0))
    roi = fmt_pct(row.get('roi', 0))
    payback = row.get('payback_years', '')
    npv = fmt_money(row.get('npv_10yr', 0))
    pop = row.get('catchment_pop', '')
    pharm5 = row.get('pharmacy_count_5km', '')
    conf = row.get('confidence', '')
    nearest_km = row.get('nearest_pharmacy_km', '')

    return (f"| {rank} | {loc} | {state} | {score} | {tier} | {rules} | "
            f"{revenue} | {profit} | {roi} | {payback} | {npv} | "
            f"{pop} | {pharm5} | {conf} | {nearest_km} |")


def table_header() -> str:
    """Return markdown table header."""
    header = ("| Rank | Location | State | Score | Tier | Rules | "
              "Revenue | Profit | ROI | Payback | NPV 10yr | "
              "Pop | Pharm 5km | Conf | Nearest (km) |")
    sep = ("|------|----------|-------|-------|------|-------|"
           "---------|--------|-----|---------|----------|"
           "-----|-----------|------|--------------|")
    return f"{header}\n{sep}"


def write_csv_extract(rows: List[Dict], filename: str):
    """Write a subset of ranked data to CSV."""
    if not rows:
        return
    path = os.path.join(OUTPUT_DIR, filename)
    fieldnames = list(rows[0].keys())
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows)} rows to {filename}")


def generate_report(data: List[Dict]) -> str:
    """Generate the full markdown report."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    total = len(data)
    profitable = [r for r in data if float(r.get('est_annual_profit', 0)) > 0]
    tier_a = [r for r in data if r.get('tier', '').startswith('A')]
    tier_b = [r for r in data if r.get('tier', '').startswith('B')]

    report = []
    report.append(f"# PharmacyFinder — Top Opportunities Report")
    report.append(f"")
    report.append(f"**Generated:** {now}")
    report.append(f"**Total opportunities analysed:** {total}")
    report.append(f"**Profitable (base case):** {len(profitable)}")
    report.append(f"**Tier A (Premium):** {len(tier_a)}")
    report.append(f"**Tier B (Strong):** {len(tier_b)}")
    report.append(f"")

    # Executive Summary
    report.append(f"---")
    report.append(f"")
    report.append(f"## Executive Summary")
    report.append(f"")

    if profitable:
        avg_profit = sum(float(r['est_annual_profit']) for r in profitable) / len(profitable)
        avg_roi = sum(float(r['roi']) for r in profitable) / len(profitable)
        total_revenue_opportunity = sum(float(r['est_annual_revenue']) for r in data)
        report.append(f"- **{len(profitable)}** of {total} opportunities are profitable in the base case")
        report.append(f"- Average profit (profitable): **{fmt_money(avg_profit)}**/year")
        report.append(f"- Average ROI (profitable): **{fmt_pct(avg_roi)}**")
        report.append(f"- Total addressable revenue: **{fmt_money(total_revenue_opportunity)}**")
    report.append(f"")

    # Tier distribution
    tiers = {}
    for r in data:
        t = r.get('tier', 'Unknown')
        tiers[t] = tiers.get(t, 0) + 1
    report.append(f"### Tier Distribution")
    report.append(f"")
    for tier in sorted(tiers.keys()):
        bar = '█' * (tiers[tier] // 5) if tiers[tier] >= 5 else '▏'
        report.append(f"- **{tier}**: {tiers[tier]} {bar}")
    report.append(f"")

    # =============================================
    # Section 1: Top 50 National
    # =============================================
    report.append(f"---")
    report.append(f"")
    report.append(f"## 1. Top 50 Opportunities — National")
    report.append(f"")
    report.append(table_header())
    for row in data[:50]:
        report.append(opportunity_row_md(row))
    report.append(f"")

    # =============================================
    # Section 2: Top 10 per State
    # =============================================
    report.append(f"---")
    report.append(f"")
    report.append(f"## 2. Top 10 per State")
    report.append(f"")

    for state in STATES:
        state_rows = [r for r in data if r.get('region') == state]
        if not state_rows:
            continue
        report.append(f"### {state} ({len(state_rows)} total opportunities)")
        report.append(f"")
        report.append(table_header())
        for row in state_rows[:10]:
            report.append(opportunity_row_md(row, 'state_rank'))
        report.append(f"")

    # =============================================
    # Section 3: Best Item 136 (Medical Centre)
    # =============================================
    report.append(f"---")
    report.append(f"")
    report.append(f"## 3. Best Item 136 Opportunities (Medical Centre Co-location)")
    report.append(f"")
    item136 = [r for r in data if 'Item 136' in r.get('qualifying_rules', '')]
    report.append(f"**Total Item 136 opportunities:** {len(item136)}")
    report.append(f"")
    if item136:
        report.append(table_header())
        for row in item136[:20]:
            report.append(opportunity_row_md(row))
    report.append(f"")

    # =============================================
    # Section 4: Best Greenfield (Item 130/131)
    # =============================================
    report.append(f"---")
    report.append(f"")
    report.append(f"## 4. Best Greenfield Opportunities (Item 130/131)")
    report.append(f"")
    greenfield = [r for r in data
                  if ('Item 130' in r.get('qualifying_rules', '') or
                      'Item 131' in r.get('qualifying_rules', ''))
                  and 'Item 132' not in r.get('qualifying_rules', '')
                  and 'Item 133' not in r.get('qualifying_rules', '')
                  and 'Item 136' not in r.get('qualifying_rules', '')]
    report.append(f"**Total greenfield (standalone 130/131):** {len(greenfield)}")
    report.append(f"")
    if greenfield:
        report.append(table_header())
        for row in greenfield[:20]:
            report.append(opportunity_row_md(row))
    report.append(f"")

    # =============================================
    # Section 5: Quick Wins
    # =============================================
    report.append(f"---")
    report.append(f"")
    report.append(f"## 5. Quick Wins (High Confidence + Low Competition)")
    report.append(f"")
    report.append(f"Criteria: Confidence ≥ 70%, composite score ≥ 50, profitable in base case, sorted by confidence then score")
    report.append(f"")
    quick_wins = sorted(
        [
            r for r in data
            if float(r.get('confidence', '0%').replace('%', '') or 0) >= 70
            and float(r.get('composite_score', 0)) >= 50
            and float(r.get('est_annual_profit', 0)) > 0
        ],
        key=lambda r: (
            float(r.get('confidence', '0%').replace('%', '') or 0),
            float(r.get('composite_score', 0))
        ),
        reverse=True,
    )
    report.append(f"**Total quick wins:** {len(quick_wins)}")
    report.append(f"")
    if quick_wins:
        report.append(table_header())
        for row in quick_wins[:30]:
            report.append(opportunity_row_md(row))
    report.append(f"")

    # =============================================
    # Section 6: State-by-State Summary
    # =============================================
    report.append(f"---")
    report.append(f"")
    report.append(f"## 6. State Summary Statistics")
    report.append(f"")
    report.append(f"| State | Total | Profitable | Tier A | Tier B | Avg Score | Avg Profit | Avg ROI |")
    report.append(f"|-------|-------|------------|--------|--------|-----------|------------|---------|")

    for state in STATES:
        state_rows = [r for r in data if r.get('region') == state]
        if not state_rows:
            continue
        s_profitable = [r for r in state_rows if float(r.get('est_annual_profit', 0)) > 0]
        s_tier_a = len([r for r in state_rows if r.get('tier', '').startswith('A')])
        s_tier_b = len([r for r in state_rows if r.get('tier', '').startswith('B')])
        s_avg_score = sum(float(r['composite_score']) for r in state_rows) / len(state_rows)
        if s_profitable:
            s_avg_profit = sum(float(r['est_annual_profit']) for r in s_profitable) / len(s_profitable)
            s_avg_roi = sum(float(r['roi']) for r in s_profitable) / len(s_profitable)
        else:
            s_avg_profit = 0
            s_avg_roi = 0
        report.append(f"| {state} | {len(state_rows)} | {len(s_profitable)} | "
                       f"{s_tier_a} | {s_tier_b} | {s_avg_score:.1f} | "
                       f"{fmt_money(s_avg_profit)} | {fmt_pct(s_avg_roi)} |")
    report.append(f"")

    # =============================================
    # Section 7: Methodology Notes
    # =============================================
    report.append(f"---")
    report.append(f"")
    report.append(f"## Methodology & Assumptions")
    report.append(f"")
    report.append(f"### Revenue Model")
    report.append(f"- **PBS scripts per capita:** 13/year (conservative; national avg 12-15)")
    report.append(f"- **Dispensing fee:** $16.50/script")
    report.append(f"- **Market share:** Sole=80%, 1 competitor=40%, 2=30%, 3+=20%")
    report.append(f"- **OTC/front-of-shop:** 35% of total revenue")
    report.append(f"- **Item 136 bonus:** +25% script volume if ≥8 GP FTE")
    report.append(f"- **Population cap:** 50,000 effective catchment per pharmacy")
    report.append(f"")
    report.append(f"### Cost Model")
    report.append(f"- **Fit-out:** $400,000")
    report.append(f"- **Initial stock:** $250,000")
    report.append(f"- **Total investment:** $657,500")
    report.append(f"- **COGS:** 70% of revenue")
    report.append(f"- **Staff (min team):** ~$212,000/year (1 pharmacist + 1 tech + 1 retail)")
    report.append(f"- **Rent:** State-dependent ($45k-$85k/year)")
    report.append(f"- **Other operating:** ~$42,000/year")
    report.append(f"")
    report.append(f"### Scoring Weights")
    report.append(f"- Financial: 40% (ROI 40%, Profit 35%, NPV 25%)")
    report.append(f"- Confidence: 20%")
    report.append(f"- Population: 15%")
    report.append(f"- Competition: 15%")
    report.append(f"- Growth corridor: 10%")
    report.append(f"")
    report.append(f"### Sensitivity Analysis")
    report.append(f"- **Worst case:** 70% revenue, 115% costs")
    report.append(f"- **Base case:** As modelled")
    report.append(f"- **Best case:** 130% revenue, 90% costs")
    report.append(f"")
    report.append(f"### Caveats")
    report.append(f"- All estimates are conservative and for screening purposes only")
    report.append(f"- Actual revenue depends on pharmacist skill, location quality, marketing")
    report.append(f"- Rent estimates use state medians — actual rents vary significantly")
    report.append(f"- Population data from OpenStreetMap may have gaps")
    report.append(f"- Competition data based on known pharmacy database — may miss recent openings")
    report.append(f"- NPV uses 10% discount rate over 10 years with 2% annual growth")
    report.append(f"")
    report.append(f"---")
    report.append(f"*Report generated by PharmacyFinder financial model v1.0*")

    return '\n'.join(report)


def main():
    print("=" * 60)
    print("TOP OPPORTUNITIES REPORT GENERATOR")
    print("=" * 60)
    print()

    data = load_ranked()
    print(f"Loaded {len(data)} ranked opportunities.")

    # Generate CSV extracts
    print(f"\nGenerating CSV extracts...")
    write_csv_extract(data[:50], 'top50_national.csv')

    for state in STATES:
        state_rows = [r for r in data if r.get('region') == state]
        if state_rows:
            write_csv_extract(state_rows[:10], f'top10_{state}.csv')

    item136 = [r for r in data if 'Item 136' in r.get('qualifying_rules', '')]
    write_csv_extract(item136[:20], 'top_item136_medical_centre.csv')

    greenfield = [r for r in data
                  if ('Item 130' in r.get('qualifying_rules', '') or
                      'Item 131' in r.get('qualifying_rules', ''))
                  and 'Item 132' not in r.get('qualifying_rules', '')
                  and 'Item 133' not in r.get('qualifying_rules', '')
                  and 'Item 136' not in r.get('qualifying_rules', '')]
    write_csv_extract(greenfield[:20], 'top_greenfield.csv')

    quick_wins = sorted(
        [
            r for r in data
            if float(r.get('confidence', '0%').replace('%', '') or 0) >= 70
            and float(r.get('composite_score', 0)) >= 50
            and float(r.get('est_annual_profit', 0)) > 0
        ],
        key=lambda r: (
            float(r.get('confidence', '0%').replace('%', '') or 0),
            float(r.get('composite_score', 0))
        ),
        reverse=True,
    )
    write_csv_extract(quick_wins[:30], 'quick_wins.csv')

    # Generate markdown report
    print(f"\nGenerating markdown report...")
    report = generate_report(data)
    report_path = os.path.join(OUTPUT_DIR, 'top_opportunities_report.md')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"  Wrote report to top_opportunities_report.md")

    # Also write a plain text summary
    print(f"\nDone! Reports generated in output/")
    print(f"  - top_opportunities_report.md (comprehensive)")
    print(f"  - top50_national.csv")
    print(f"  - top10_<STATE>.csv (per state)")
    print(f"  - top_item136_medical_centre.csv")
    print(f"  - top_greenfield.csv")
    print(f"  - quick_wins.csv")


if __name__ == '__main__':
    main()
