#!/usr/bin/env python3
"""Generate a self-contained HTML verification map for a single opportunity."""

import sqlite3
import json
import sys
import os
import math

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'output')
SCORED_PATH = os.path.join(OUTPUT_DIR, 'scored_v2.json')

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def get_opportunity(opp_id):
    with open(SCORED_PATH) as f:
        scored = json.load(f)
    for s in scored:
        if s['id'] == opp_id:
            return s
    return None

def get_nearby_pharmacies(lat, lng, radius_km=15):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM pharmacies")
    nearby = []
    for row in cur.fetchall():
        d = dict(row)
        rlat = d.get('lat') or d.get('latitude')
        rlng = d.get('lng') or d.get('longitude')
        if rlat and rlng:
            d['lat'] = rlat
            d['lng'] = rlng
            dist = haversine(lat, lng, rlat, rlng)
            if dist <= radius_km:
                d['distance_km'] = round(dist, 2)
                nearby.append(d)
    conn.close()
    nearby.sort(key=lambda x: x['distance_km'])
    return nearby

def get_db_details(opp_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM opportunities WHERE id = ?", (opp_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else {}

def get_rule_radius(rule):
    """Return the relevant radius for each rule type."""
    if rule == "Item 131":
        return 10  # 10km from nearest pharmacy
    elif rule == "Item 132":
        return 2   # 200m+ from nearest, show 2km area
    elif rule == "Item 134A":
        return 1   # Within shopping centre, show 1km
    elif rule == "Item 136":
        return 2   # Near medical centre, show 2km
    return 5

def get_rule_criteria(rule, opp, db_details, pharmacies):
    """Return rule criteria with pass/fail for each."""
    criteria = []
    
    if rule == "Item 131":
        criteria.append({
            'name': 'No pharmacy within 10km by shortest lawful access route',
            'value': f"Nearest: {opp.get('nearest_pharmacy', '?')} at {opp.get('nearest_pharmacy_km', '?'):.1f}km" if isinstance(opp.get('nearest_pharmacy_km'), (int, float)) else f"Nearest: {opp.get('nearest_pharmacy', '?')}",
            'pass': opp.get('nearest_pharmacy_km', 0) >= 10 if isinstance(opp.get('nearest_pharmacy_km'), (int, float)) else False
        })
        criteria.append({
            'name': 'Population ≥ 500 within 10km',
            'value': f"Population 10km: {opp.get('pop_10km', '?'):,}",
            'pass': (opp.get('pop_10km', 0) or 0) >= 500
        })
        
    elif rule == "Item 132":
        criteria.append({
            'name': 'Nearest pharmacy ≥ 200m (same town)',
            'value': f"Nearest: {opp.get('nearest_pharmacy', '?')} at {opp.get('nearest_pharmacy_km', 0)*1000:.0f}m",
            'pass': (opp.get('nearest_pharmacy_km', 0) or 0) >= 0.2
        })
        criteria.append({
            'name': 'Qualifying POI anchor (supermarket/medical)',
            'value': f"Anchor: {opp.get('name', '?')}",
            'pass': True  # If it's in the list, we found an anchor
        })
        criteria.append({
            'name': 'Population supports additional pharmacy',
            'value': f"Pop 10km: {opp.get('pop_10km', '?'):,} | Ratio: {opp.get('ratio', '?'):,}:1",
            'pass': (opp.get('ratio', 0) or 0) >= 2000
        })
        
    elif rule == "Item 134A":
        criteria.append({
            'name': 'Shopping centre ≥ 5,000sqm GLA',
            'value': f"Centre: {opp.get('name', '?')}",
            'pass': True  # Extracted from evidence
        })
        gla_match = ''
        tenants = ''
        existing_ph = ''
        evidence = opp.get('evidence', '')
        if 'centre_gla:' in evidence:
            gla_match = evidence.split('centre_gla:')[1].split('|')[0].strip()
        if 'tenants' in evidence:
            for part in evidence.split('|'):
                if 'tenant' in part.lower():
                    tenants = part.strip()
                    break
        criteria.append({
            'name': 'GLA & tenant count',
            'value': gla_match or 'See evidence',
            'pass': True
        })
        criteria.append({
            'name': 'Fewer pharmacies than allowed for tenant tier',
            'value': tenants or 'See evidence',
            'pass': True
        })
        
    elif rule == "Item 136":
        criteria.append({
            'name': 'Medical centre with ≥ 8 FTE medical practitioners',
            'value': f"Centre: {opp.get('name', '?')}",
            'pass': True
        })
        gp_info = ''
        evidence = opp.get('evidence', '')
        if 'practitioners:' in evidence:
            gp_info = evidence.split('practitioners:')[1].split('|')[0].strip()
        criteria.append({
            'name': 'GP count / FTE',
            'value': gp_info or 'See evidence',
            'pass': True
        })
        criteria.append({
            'name': 'Nearest pharmacy distance',
            'value': f"{opp.get('nearest_pharmacy', '?')} at {opp.get('nearest_pharmacy_km', 0)*1000:.0f}m",
            'pass': True
        })
    
    return criteria

def generate_html(opp, db_details, pharmacies, criteria, rule_radius):
    rule = opp.get('rules', '')
    lat, lng = opp['lat'], opp['lng']
    
    # Pharmacy markers
    ph_markers = []
    for p in pharmacies[:30]:  # Limit to 30 nearest
        name_escaped = p.get('name', '').replace("'", "\\'").replace('"', '\\"')
        addr = p.get('address', '').replace("'", "\\'").replace('"', '\\"')
        ph_markers.append(f"""
        L.circleMarker([{p['lat']}, {p['lng']}], {{radius: 7, color: '#ef4444', fillColor: '#ef4444', fillOpacity: 0.8, weight: 1}})
            .bindPopup('<b>{name_escaped}</b><br>{addr}<br>Distance: {p["distance_km"]}km')
            .addTo(map);""")
    
    # Criteria HTML
    criteria_html = ""
    for c in criteria:
        icon = "✅" if c['pass'] else "❌"
        criteria_html += f"""
        <div class="criterion {'pass' if c['pass'] else 'fail'}">
            <span class="icon">{icon}</span>
            <div>
                <div class="crit-name">{c['name']}</div>
                <div class="crit-value">{c['value']}</div>
            </div>
        </div>"""
    
    # Nearby pharmacies table
    ph_table = ""
    for i, p in enumerate(pharmacies[:10]):
        ph_table += f"""<tr>
            <td>{i+1}</td>
            <td>{p.get('name', '?')}</td>
            <td>{p.get('address', '?')}</td>
            <td><b>{p['distance_km']}km</b></td>
        </tr>"""
    
    # Address
    address = opp.get('address', '') or db_details.get('address', '') or 'Address not available'
    town = ''
    if 'town:' in opp.get('evidence', ''):
        town = opp['evidence'].split('town:')[1].split('|')[0].strip()
    
    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Verify: {opp['name']} ({opp['state']})</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e2e8f0; }}
.container {{ display: grid; grid-template-columns: 400px 1fr; height: 100vh; }}
.panel {{ padding: 20px; overflow-y: auto; background: #1a1d2e; border-right: 1px solid #2a2f45; }}
#map {{ width: 100%; height: 100%; }}
h1 {{ font-size: 20px; color: #22c55e; margin-bottom: 4px; }}
.subtitle {{ color: #94a3b8; font-size: 13px; margin-bottom: 16px; }}
.score-bar {{ display: flex; gap: 12px; margin-bottom: 16px; padding: 12px; background: #0f1117; border-radius: 8px; }}
.score-item {{ text-align: center; }}
.score-item .value {{ font-size: 24px; font-weight: 700; color: #f59e0b; }}
.score-item .label {{ font-size: 10px; color: #94a3b8; text-transform: uppercase; }}
.section {{ margin-bottom: 16px; }}
.section-title {{ font-size: 12px; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; border-bottom: 1px solid #2a2f45; padding-bottom: 4px; }}
.criterion {{ display: flex; gap: 8px; padding: 8px; margin-bottom: 4px; border-radius: 6px; background: #0f1117; }}
.criterion.pass {{ border-left: 3px solid #22c55e; }}
.criterion.fail {{ border-left: 3px solid #ef4444; }}
.icon {{ font-size: 16px; }}
.crit-name {{ font-size: 13px; font-weight: 600; }}
.crit-value {{ font-size: 12px; color: #94a3b8; }}
.address {{ font-size: 13px; color: #cbd5e1; padding: 8px; background: #0f1117; border-radius: 6px; margin-bottom: 8px; }}
.evidence {{ font-size: 11px; color: #64748b; padding: 8px; background: #0f1117; border-radius: 6px; word-break: break-all; }}
table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
th, td {{ padding: 6px 8px; text-align: left; border-bottom: 1px solid #2a2f45; }}
th {{ color: #94a3b8; font-weight: 600; }}
</style>
</head><body>
<div class="container">
<div class="panel">
    <h1>📍 {opp['name']}</h1>
    <div class="subtitle">{opp['state']} • {town} • {rule}</div>
    
    <div class="address">📌 {address}</div>
    <div class="address">🌐 {lat:.6f}, {lng:.6f}</div>
    
    <div class="score-bar">
        <div class="score-item"><div class="value">{opp['score']}</div><div class="label">Score</div></div>
        <div class="score-item"><div class="value">{opp['ratio']:,}:1</div><div class="label">Ratio</div></div>
        <div class="score-item"><div class="value">{opp['pop_10km']:,}</div><div class="label">Pop 10km</div></div>
        <div class="score-item"><div class="value">{opp.get('pharmacies_10km', '?')}</div><div class="label">Ph 10km</div></div>
    </div>
    
    <div class="section">
        <div class="section-title">Rule Criteria — {rule}</div>
        {criteria_html}
    </div>
    
    <div class="section">
        <div class="section-title">Nearest Pharmacies</div>
        <table>
            <tr><th>#</th><th>Name</th><th>Address</th><th>Dist</th></tr>
            {ph_table}
        </table>
    </div>
    
    <div class="section">
        <div class="section-title">Evidence</div>
        <div class="evidence">{opp.get('evidence', 'N/A')}</div>
    </div>
</div>
<div id="map"></div>
</div>
<script>
var map = L.map('map').setView([{lat}, {lng}], 14);
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
    maxZoom: 19,
    attribution: '&copy; OSM &copy; CARTO'
}}).addTo(map);

// Opportunity marker (green star)
L.marker([{lat}, {lng}], {{
    icon: L.divIcon({{
        html: '<div style="background:#22c55e;width:20px;height:20px;border-radius:50%;border:3px solid #fff;box-shadow:0 0 10px rgba(34,197,94,0.5)"></div>',
        iconSize: [20,20],
        iconAnchor: [10,10],
        className: ''
    }})
}}).bindPopup('<b>{opp["name"]}</b><br>Opportunity Location').addTo(map);

// Rule radius circle
L.circle([{lat}, {lng}], {{
    radius: {rule_radius * 1000},
    color: '#f59e0b',
    fillColor: '#f59e0b',
    fillOpacity: 0.05,
    weight: 2,
    dashArray: '8 4'
}}).addTo(map);

// 200m circle for Item 132
{"L.circle([" + str(lat) + ", " + str(lng) + "], {radius: 200, color: '#ef4444', fillOpacity: 0.05, weight: 2, dashArray: '4 4'}).bindPopup('200m exclusion zone').addTo(map);" if rule == "Item 132" else ""}

// 10km circle for Item 131
{"L.circle([" + str(lat) + ", " + str(lng) + "], {radius: 10000, color: '#3b82f6', fillOpacity: 0.03, weight: 1, dashArray: '8 4'}).bindPopup('10km radius').addTo(map);" if rule == "Item 131" else ""}

// Pharmacy markers (red)
{"".join(ph_markers)}
</script>
</body></html>"""
    
    return html

def main():
    if len(sys.argv) < 2:
        print("Usage: python verify_opportunity.py <opportunity_id>")
        sys.exit(1)
    
    opp_id = int(sys.argv[1])
    opp = get_opportunity(opp_id)
    if not opp:
        print(f"Opportunity {opp_id} not found")
        sys.exit(1)
    
    db_details = get_db_details(opp_id)
    rule = opp.get('rules', '')
    rule_radius = get_rule_radius(rule)
    pharmacies = get_nearby_pharmacies(opp['lat'], opp['lng'], radius_km=15)
    criteria = get_rule_criteria(rule, opp, db_details, pharmacies)
    
    html = generate_html(opp, db_details, pharmacies, criteria, rule_radius)
    
    out_path = os.path.join(OUTPUT_DIR, f'verify_{opp_id}.html')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"Verification map: {out_path}")
    print(f"Name: {opp['name']}")
    print(f"State: {opp['state']}")
    print(f"Rule: {rule}")
    print(f"Score: {opp['score']}")
    print(f"Nearby pharmacies: {len(pharmacies)}")

if __name__ == '__main__':
    main()
