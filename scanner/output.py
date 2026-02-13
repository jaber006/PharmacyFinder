"""
Output generators for opportunity zone scan results.

Produces:
  - Interactive HTML map (Folium) with layered markers
  - CSV export for spreadsheet analysis
  - Console summary
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List

import folium
import pandas as pd

from utils.database import Database
import config


# ── colour palette per rule ───────────────────────────────────────

RULE_COLOURS = {
    'Item 130': '#3388ff',   # blue
    'Item 131': '#2ca02c',   # green
    'Item 132': '#9467bd',   # purple
    'Item 133': '#ff7f0e',   # orange
    'Item 134': '#e377c2',   # pink
    'Item 134A': '#1f77b4',  # dark blue
    'Item 135': '#d62728',   # red
    'Item 136': '#17becf',   # teal
}

RULE_ICONS = {
    'Item 130': ('star',      'blue'),
    'Item 131': ('tree',      'green'),
    'Item 132': ('shopping-cart', 'purple'),
    'Item 133': ('shopping-basket', 'orange'),
    'Item 134': ('shopping-cart', 'pink'),
    'Item 134A': ('globe',    'darkblue'),
    'Item 135': ('plus',      'red'),
    'Item 136': ('stethoscope', 'cadetblue'),
}


# ── CSV ───────────────────────────────────────────────────────────

def generate_csv(opportunities: List[Dict], output_path: str) -> str:
    """Write opportunity zones to CSV."""
    rows = []
    for opp in opportunities:
        rows.append({
            'Latitude': opp['latitude'],
            'Longitude': opp['longitude'],
            'Address': opp.get('address', ''),
            'Qualifying Rules': opp['qualifying_rules'],
            'Evidence': opp['evidence'],
            'Confidence': f"{opp.get('confidence', 0):.0%}",
            'Nearest Pharmacy (km)': f"{opp.get('nearest_pharmacy_km', 0):.2f}",
            'Nearest Pharmacy Name': opp.get('nearest_pharmacy_name', ''),
            'POI Name': opp.get('poi_name', ''),
            'POI Type': opp.get('poi_type', ''),
            'Region': opp.get('region', ''),
            'Date Scanned': opp.get('date_scanned', datetime.now().strftime('%Y-%m-%d')),
        })

    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path


# ── Interactive Map ───────────────────────────────────────────────

def generate_map(
    opportunities: List[Dict],
    db: Database,
    output_path: str,
    region: str = '',
) -> str:
    """Build an interactive Folium HTML map."""

    # --- determine map centre ---
    if opportunities:
        clat = sum(o['latitude'] for o in opportunities) / len(opportunities)
        clon = sum(o['longitude'] for o in opportunities) / len(opportunities)
        zoom = 8
    else:
        clat, clon, zoom = -42.0, 146.5, 7  # Tasmania fallback

    m = folium.Map(location=[clat, clon], zoom_start=zoom, tiles='OpenStreetMap')

    # --- feature groups ---
    opp_group = folium.FeatureGroup(name='🎯 Opportunity Zones', show=True)
    pharm_group = folium.FeatureGroup(name='💊 Existing Pharmacies', show=True)
    sm_group = folium.FeatureGroup(name='🛒 Supermarkets', show=False)
    gp_group = folium.FeatureGroup(name='🩺 GP Practices', show=False)
    hosp_group = folium.FeatureGroup(name='🏥 Hospitals', show=False)

    # --- opportunity markers ---
    for opp in opportunities:
        rules = opp['qualifying_rules'].split(', ')
        primary_rule = rules[0] if rules else 'Item 130'
        icon_name, icon_colour = RULE_ICONS.get(primary_rule, ('star', 'blue'))
        confidence_pct = f"{opp.get('confidence', 0):.0%}"

        popup_html = f"""
        <div style="min-width:280px;font-family:sans-serif">
          <h4 style="margin:0 0 8px">🎯 Opportunity Zone</h4>
          <b>Rules:</b> {opp['qualifying_rules']}<br>
          <b>Confidence:</b> {confidence_pct}<br>
          <b>Nearest pharmacy:</b> {opp.get('nearest_pharmacy_name','?')}
            ({opp.get('nearest_pharmacy_km',0):.1f} km)<br>
          <b>POI:</b> {opp.get('poi_name','')} ({opp.get('poi_type','')})<br>
          <hr style="margin:6px 0">
          <b>Evidence:</b><br>
          <small>{opp['evidence'][:400]}</small>
        </div>
        """

        folium.Marker(
            location=[opp['latitude'], opp['longitude']],
            popup=folium.Popup(popup_html, max_width=380),
            icon=folium.Icon(color=icon_colour, icon=icon_name, prefix='fa'),
            tooltip=f"[{primary_rule}] {opp.get('poi_name','Opportunity')} ({confidence_pct})",
        ).add_to(opp_group)

    # --- reference layers ---
    pharmacies = db.get_all_pharmacies()
    for p in pharmacies:
        folium.CircleMarker(
            location=[p['latitude'], p['longitude']],
            radius=5, color='red', fill=True, fill_color='red', fill_opacity=0.7,
            popup=f"💊 {p.get('name','Pharmacy')}",
            tooltip=f"Pharmacy: {p.get('name','')[:40]}",
        ).add_to(pharm_group)

    supermarkets = db.get_all_supermarkets()
    for s in supermarkets:
        folium.CircleMarker(
            location=[s['latitude'], s['longitude']],
            radius=4, color='blue', fill=True, fill_color='blue', fill_opacity=0.5,
            popup=f"🛒 {s.get('name','Supermarket')} ({s.get('floor_area_sqm',0):.0f} sqm)",
            tooltip=f"Supermarket: {s.get('name','')[:40]}",
        ).add_to(sm_group)

    gps = db.get_all_gps()
    for g in gps:
        folium.CircleMarker(
            location=[g['latitude'], g['longitude']],
            radius=3, color='green', fill=True, fill_color='green', fill_opacity=0.5,
            popup=f"🩺 {g.get('name','GP')} ({g.get('fte',0):.1f} FTE)",
            tooltip=f"GP: {g.get('name','')[:40]}",
        ).add_to(gp_group)

    hospitals = db.get_all_hospitals()
    for h in hospitals:
        beds = h.get('bed_count') or 0
        folium.Marker(
            location=[h['latitude'], h['longitude']],
            popup=f"🏥 {h.get('name','Hospital')} ({beds} beds)",
            icon=folium.Icon(color='red', icon='plus', prefix='fa'),
            tooltip=f"Hospital: {h.get('name','')[:40]} ({beds} beds)",
        ).add_to(hosp_group)

    # --- add groups + controls ---
    for grp in [opp_group, pharm_group, sm_group, gp_group, hosp_group]:
        grp.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    # --- legend ---
    legend_items = ''.join(
        f'<li><span style="color:{RULE_COLOURS.get(r,"gray")}">■</span> {r}</li>'
        for r in RULE_COLOURS
    )
    legend_html = f"""
    <div style="position:fixed;bottom:30px;left:30px;z-index:9999;
                background:white;padding:12px 16px;border-radius:6px;
                border:1px solid #ccc;font:13px/1.5 sans-serif;max-width:260px">
      <b>Pharmacy Opportunity Zones</b>
      <div style="margin-top:4px"><small>{len(opportunities)} opportunities found
        {f' -- {region}' if region else ''}</small></div>
      <hr style="margin:6px 0">
      <ul style="list-style:none;padding:0;margin:0">{legend_items}</ul>
      <hr style="margin:6px 0">
      <small>
        <span style="color:red">●</span> Existing pharmacies&nbsp;&nbsp;
        <span style="color:blue">●</span> Supermarkets<br>
        <span style="color:green">●</span> GPs&nbsp;&nbsp;
        <span style="color:red">+</span> Hospitals
      </small>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    m.save(output_path)
    return output_path


# ── Console summary ───────────────────────────────────────────────

def print_summary(opportunities: List[Dict], region: str = ''):
    """Pretty-print a summary to stdout."""
    print(f"\n{'='*60}")
    print(f"  OPPORTUNITY ZONE SCAN RESULTS{f' - {region}' if region else ''}")
    print(f"{'='*60}")
    print(f"  Total opportunity zones: {len(opportunities)}\n")

    if not opportunities:
        print("  No opportunities found.")
        return

    # Group by rule
    rule_counts: Dict[str, int] = {}
    for opp in opportunities:
        for rule in opp['qualifying_rules'].split(', '):
            rule = rule.strip()
            rule_counts[rule] = rule_counts.get(rule, 0) + 1

    print("  By rule:")
    for rule, count in sorted(rule_counts.items()):
        print(f"    {rule}: {count}")

    # Top 10
    print(f"\n  Top opportunities (by confidence):")
    print(f"  {'-'*56}")
    for i, opp in enumerate(opportunities[:15], 1):
        conf = f"{opp.get('confidence', 0):.0%}"
        dist = opp.get('nearest_pharmacy_km', 0)
        dist_str = f"{dist:.1f} km" if dist and dist > 0 else "?"
        poi = opp.get('poi_name', '')[:35]
        rules = opp['qualifying_rules']
        print(f"  {i:>3}. [{conf}] {poi}")
        print(f"       Rules: {rules}")
        print(f"       Nearest pharmacy: {dist_str}")

    print(f"\n{'='*60}\n")
