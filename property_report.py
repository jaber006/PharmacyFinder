"""
Property Report Generator
=========================

Generates a nicely formatted summary report of commercial properties
matched to pharmacy opportunity zones.

Usage:
    python property_report.py                    # Full report
    python property_report.py --state TAS        # Single state
    python property_report.py --html             # HTML report
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import datetime
from typing import Dict, List

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
STATE_PRIORITY = ['TAS', 'VIC', 'NSW', 'QLD', 'SA', 'WA', 'NT', 'ACT']


def load_summary() -> Dict:
    """Load the properties summary JSON."""
    path = os.path.join(OUTPUT_DIR, 'properties_summary.json')
    if not os.path.exists(path):
        print("No properties_summary.json found. Run scrape_properties.py first.")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def load_listings(suitable_only: bool = True) -> List[Dict]:
    """Load property listings from CSV."""
    filename = 'property_listings.csv' if suitable_only else 'property_listings_all.csv'
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def format_rent(rent_display: str, rent_pa: float) -> str:
    """Format rent for display."""
    if rent_display and rent_display not in ('0', '', '0.0'):
        return rent_display
    if rent_pa and float(rent_pa) > 0:
        return f"${float(rent_pa):,.0f} pa"
    return "Contact Agent"


def print_text_report(summary: Dict, listings: List[Dict], state_filter: str = None):
    """Print a formatted text report."""
    
    print()
    print("=" * 78)
    print("  PHARMACY OPPORTUNITY ZONES - COMMERCIAL PROPERTY REPORT")
    print("=" * 78)
    print(f"  Generated: {summary.get('generated', 'N/A')[:19]}")
    print(f"  Suburbs Searched: {summary.get('suburbs_scraped', 'N/A')}")
    print(f"  Total Listings Found: {summary.get('total_raw_listings', 'N/A')}")
    print(f"  Pharmacy-Suitable: {summary.get('suitable_listings', 'N/A')}")
    print(f"  Unique Properties: {summary.get('unique_suitable', 'N/A')}")
    print()
    
    by_state = summary.get('by_state', {})
    
    for state in STATE_PRIORITY:
        if state_filter and state != state_filter:
            continue
        if state not in by_state:
            continue
        
        sd = by_state[state]
        print("-" * 78)
        print(f"  {state}")
        print("-" * 78)
        print(f"  Opportunities with listings: {sd.get('opportunities_with_listings', 0)}")
        print(f"  Unique listings found:       {sd.get('unique_listings', 0)}")
        print(f"  Pharmacy-suitable:           {sd.get('suitable_listings', 0)}")
        print(f"  Rent range:                  {sd.get('rent_range', 'N/A')}")
        print(f"  Rent median:                 {sd.get('rent_median', 'N/A')}")
        print(f"  Size range:                  {sd.get('size_range', 'N/A')}")
        print()
        
        top = sd.get('top_properties', [])
        if top:
            print(f"  TOP PROPERTIES:")
            print()
            for i, p in enumerate(top[:10], 1):
                score = p.get('score', 0)
                stars = '*' * (score // 20) if score else ''
                print(f"  {i:2d}. {p.get('address', 'Unknown')}")
                print(f"      Rent: {p.get('rent', 'Contact Agent')}")
                size = p.get('size', 0)
                if size:
                    print(f"      Size: {size} sqm")
                ptype = p.get('type', '')
                if ptype:
                    print(f"      Type: {ptype}")
                print(f"      Score: {score}/100 {stars}")
                opp = p.get('opportunity', '')
                if opp:
                    print(f"      Near: {opp}")
                url = p.get('url', '')
                if url:
                    print(f"      URL:  {url}")
                print()
    
    # Overall top 20 across all states
    if not state_filter:
        print("=" * 78)
        print("  TOP 20 PHARMACY-SUITABLE PROPERTIES (ALL STATES)")
        print("=" * 78)
        
        # Sort by suitability score
        sorted_listings = sorted(listings, 
            key=lambda x: (int(x.get('suitability_score', 0)), float(x.get('composite_score', 0))),
            reverse=True)
        
        # Deduplicate by URL
        seen_urls = set()
        unique = []
        for l in sorted_listings:
            url = l.get('listing_url', '')
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            unique.append(l)
        
        for i, l in enumerate(unique[:20], 1):
            state = l.get('state', '')
            addr = l.get('property_address', '')
            rent = format_rent(l.get('rent_display', ''), l.get('rent_pa', 0))
            size = l.get('floor_area_sqm', 0)
            ptype = l.get('property_type', '')
            score = l.get('suitability_score', 0)
            poi = l.get('poi_name', '')
            
            print(f"\n  {i:2d}. [{state}] {addr}")
            print(f"      Rent: {rent}", end='')
            if size and str(size) != '0':
                print(f"  |  Size: {size} sqm", end='')
            print()
            if ptype:
                print(f"      Type: {ptype}")
            print(f"      Score: {score}/100  |  Opportunity: {poi}")
    
    print()
    print("=" * 78)
    print("  Report complete.")
    print("=" * 78)


def generate_html_report(summary: Dict, listings: List[Dict], state_filter: str = None):
    """Generate an HTML property report."""
    by_state = summary.get('by_state', {})
    
    # Sort and deduplicate
    sorted_listings = sorted(listings,
        key=lambda x: (int(x.get('suitability_score', 0)), float(x.get('composite_score', 0))),
        reverse=True)
    
    seen_urls = set()
    unique = []
    for l in sorted_listings:
        url = l.get('listing_url', '')
        if url and url in seen_urls:
            continue
        if url:
            seen_urls.add(url)
        unique.append(l)
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pharmacy Opportunity - Property Report</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        .header {{ background: linear-gradient(135deg, #1a5276, #2e86c1); color: white; padding: 40px; border-radius: 12px; margin-bottom: 30px; }}
        .header h1 {{ font-size: 28px; margin-bottom: 10px; }}
        .header .stats {{ display: flex; gap: 40px; margin-top: 20px; flex-wrap: wrap; }}
        .header .stat {{ text-align: center; }}
        .header .stat .num {{ font-size: 36px; font-weight: 700; }}
        .header .stat .label {{ font-size: 14px; opacity: 0.8; }}
        .state-section {{ background: white; border-radius: 12px; padding: 30px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .state-section h2 {{ font-size: 22px; margin-bottom: 15px; color: #1a5276; }}
        .state-stats {{ display: flex; gap: 30px; margin-bottom: 20px; flex-wrap: wrap; }}
        .state-stats .stat {{ background: #eaf2f8; padding: 12px 20px; border-radius: 8px; }}
        .state-stats .stat .num {{ font-size: 24px; font-weight: 600; color: #2e86c1; }}
        .state-stats .stat .label {{ font-size: 12px; color: #666; }}
        .property-card {{ border: 1px solid #e0e0e0; border-radius: 8px; padding: 16px; margin-bottom: 12px; transition: box-shadow 0.2s; }}
        .property-card:hover {{ box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
        .property-card .address {{ font-size: 16px; font-weight: 600; color: #1a5276; }}
        .property-card .details {{ display: flex; gap: 20px; margin-top: 8px; flex-wrap: wrap; }}
        .property-card .detail {{ font-size: 14px; color: #555; }}
        .property-card .detail strong {{ color: #333; }}
        .property-card .score {{ display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }}
        .score-high {{ background: #d4efdf; color: #1e8449; }}
        .score-med {{ background: #fef9e7; color: #b7950b; }}
        .score-low {{ background: #fdedec; color: #c0392b; }}
        .property-card .opp {{ font-size: 13px; color: #888; margin-top: 6px; }}
        a {{ color: #2e86c1; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .top-section {{ margin-top: 40px; }}
        .top-section h2 {{ font-size: 24px; color: #1a5276; margin-bottom: 20px; }}
        .footer {{ text-align: center; padding: 30px; color: #888; font-size: 13px; }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>Pharmacy Opportunity Zones - Property Report</h1>
        <p>Commercial properties suitable for pharmacy leases near identified opportunity zones</p>
        <div class="stats">
            <div class="stat">
                <div class="num">{summary.get('suburbs_scraped', 0)}</div>
                <div class="label">Suburbs Searched</div>
            </div>
            <div class="stat">
                <div class="num">{summary.get('total_raw_listings', 0)}</div>
                <div class="label">Total Listings</div>
            </div>
            <div class="stat">
                <div class="num">{summary.get('suitable_listings', 0)}</div>
                <div class="label">Pharmacy Suitable</div>
            </div>
            <div class="stat">
                <div class="num">{summary.get('unique_suitable', 0)}</div>
                <div class="label">Unique Properties</div>
            </div>
        </div>
    </div>
"""
    
    # State sections
    for state in STATE_PRIORITY:
        if state_filter and state != state_filter:
            continue
        if state not in by_state:
            continue
        
        sd = by_state[state]
        state_name = {
            'TAS': 'Tasmania', 'VIC': 'Victoria', 'NSW': 'New South Wales',
            'QLD': 'Queensland', 'SA': 'South Australia', 'WA': 'Western Australia',
            'NT': 'Northern Territory', 'ACT': 'Australian Capital Territory',
        }.get(state, state)
        
        html += f"""
    <div class="state-section">
        <h2>{state_name} ({state})</h2>
        <div class="state-stats">
            <div class="stat"><div class="num">{sd.get('suitable_listings', 0)}</div><div class="label">Suitable Listings</div></div>
            <div class="stat"><div class="num">{sd.get('rent_range', 'N/A')}</div><div class="label">Rent Range (p.a.)</div></div>
            <div class="stat"><div class="num">{sd.get('size_range', 'N/A')}</div><div class="label">Size Range</div></div>
        </div>
"""
        for p in sd.get('top_properties', [])[:8]:
            score = p.get('score', 0)
            score_class = 'score-high' if score >= 80 else ('score-med' if score >= 60 else 'score-low')
            url = p.get('url', '')
            addr = p.get('address', 'Unknown')
            if url:
                addr_html = f'<a href="{url}" target="_blank">{addr}</a>'
            else:
                addr_html = addr
            
            html += f"""
        <div class="property-card">
            <div class="address">{addr_html}</div>
            <div class="details">
                <div class="detail"><strong>Rent:</strong> {p.get('rent', 'Contact Agent')}</div>
                <div class="detail"><strong>Size:</strong> {p.get('size', 'N/A')} sqm</div>
                <div class="detail"><strong>Type:</strong> {p.get('type', 'N/A')}</div>
                <span class="score {score_class}">{score}/100</span>
            </div>
            <div class="opp">Near opportunity: {p.get('opportunity', 'N/A')}</div>
        </div>
"""
        html += "    </div>\n"
    
    # Top 30 overall
    html += """
    <div class="top-section">
        <h2>Top 30 Properties Across All States</h2>
"""
    
    for i, l in enumerate(unique[:30], 1):
        score = int(l.get('suitability_score', 0))
        score_class = 'score-high' if score >= 80 else ('score-med' if score >= 60 else 'score-low')
        url = l.get('listing_url', '')
        addr = l.get('property_address', 'Unknown')
        if url:
            addr_html = f'<a href="{url}" target="_blank">{addr}</a>'
        else:
            addr_html = addr
        
        rent = format_rent(l.get('rent_display', ''), l.get('rent_pa', 0))
        size = l.get('floor_area_sqm', 0)
        
        html += f"""
        <div class="property-card">
            <div class="address">{i}. [{l.get('state', '')}] {addr_html}</div>
            <div class="details">
                <div class="detail"><strong>Rent:</strong> {rent}</div>
                <div class="detail"><strong>Size:</strong> {size} sqm</div>
                <div class="detail"><strong>Type:</strong> {l.get('property_type', 'N/A')}</div>
                <span class="score {score_class}">{score}/100</span>
            </div>
            <div class="opp">Near: {l.get('poi_name', 'N/A')} | Rule: {l.get('rule', '')} | Confidence: {l.get('confidence', '')}</div>
        </div>
"""
    
    html += f"""
    </div>
    <div class="footer">
        <p>Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data from realcommercial.com.au</p>
        <p>PharmacyFinder - Commercial Property Report</p>
    </div>
</div>
</body>
</html>"""
    
    output_path = os.path.join(OUTPUT_DIR, 'property_report.html')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"HTML report: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description='Generate property report')
    parser.add_argument('--state', help='Filter to single state')
    parser.add_argument('--html', action='store_true', help='Generate HTML report')
    parser.add_argument('--both', action='store_true', help='Generate both text and HTML')
    args = parser.parse_args()
    
    summary = load_summary()
    listings = load_listings()
    
    state_filter = args.state.upper() if args.state else None
    
    if args.html or args.both:
        generate_html_report(summary, listings, state_filter)
    
    if not args.html or args.both:
        print_text_report(summary, listings, state_filter)


if __name__ == '__main__':
    main()
