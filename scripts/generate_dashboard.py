"""Generate an interactive HTML dashboard with profitability rankings."""
import json
import os

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')

def load_all_opportunities():
    all_opps = []
    for fname in ['item130_opportunities.json', 'item131_opportunities.json',
                   'item132_opportunities.json', 'item133_opportunities.json',
                   'item136_opportunities.json']:
        fpath = os.path.join(OUTPUT_DIR, fname)
        if not os.path.exists(fpath):
            continue
        with open(fpath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        all_opps.extend(data)
    return all_opps

def main():
    opps = load_all_opportunities()
    
    # Load profitability rankings
    prof_path = os.path.join(OUTPUT_DIR, 'profitability_rankings.json')
    if os.path.exists(prof_path):
        with open(prof_path, 'r', encoding='utf-8') as f:
            top50 = json.load(f)
    else:
        top50 = []
    
    # Build map data (top 200 non-131 + top 50 131)
    map_points = []
    rule_counts = {}
    
    for opp in opps:
        rule = opp.get('rule_item', '??')
        if rule not in rule_counts:
            rule_counts[rule] = 0
        rule_counts[rule] += 1
    
    # All non-131 opportunities
    non131 = [o for o in opps if 'Item 131' not in o.get('rule_item', '')]
    # Top 50 Item 131 by confidence
    item131 = sorted([o for o in opps if 'Item 131' in o.get('rule_item', '')], 
                     key=lambda x: x.get('confidence', 0), reverse=True)[:50]
    
    for opp in non131 + item131:
        lat = opp.get('lat', 0)
        lon = opp.get('lon', 0)
        if lat == 0 or lon == 0:
            continue
        
        rule = opp.get('rule_item', '??')
        name = opp.get('name', 'Unknown').replace("'", "\\'").replace('"', '\\"')
        state = opp.get('state', '??')
        conf = opp.get('confidence', 0)
        dist = opp.get('nearest_pharmacy_km', 0)
        reason = opp.get('reason', '').replace("'", "\\'").replace('"', '\\"').replace('\n', ' ')[:200]
        
        # Color by rule
        colors = {
            'Item 130': '#e74c3c',  # red
            'Item 131': '#3498db',  # blue
            'Item 132': '#f39c12',  # orange
            'Item 133': '#9b59b6',  # purple
            'Item 136': '#2ecc71',  # green
        }
        color = colors.get(rule, '#95a5a6')
        
        map_points.append({
            'lat': lat, 'lon': lon, 'name': name, 'state': state,
            'rule': rule, 'conf': conf, 'dist': round(dist, 1),
            'reason': reason, 'color': color
        })
    
    # Build profitability table data
    prof_rows = []
    for opp in top50:
        p = opp.get('profitability', {})
        if not p:
            continue
        prof_rows.append({
            'name': opp.get('name', 'Unknown'),
            'state': opp.get('state', '??'),
            'rule': opp.get('rule_item', '??'),
            'scripts': p.get('scripts_day', 0),
            'revenue': p.get('revenue', 0),
            'profit': p.get('net_profit', 0),
            'margin': p.get('margin_pct', 0),
            'setup': p.get('setup_cost', 0),
            'flip_low': p.get('flip_profit_low', 0),
            'flip_high': p.get('flip_profit_high', 0),
            'roi': p.get('roi_year1_pct', 0),
            'payback': p.get('payback_months', 999),
            'lat': opp.get('lat', 0),
            'lon': opp.get('lon', 0),
        })
    
    html = f"""<!DOCTYPE html>
<html>
<head>
<title>PharmacyFinder v4 — Greenfield Opportunity Dashboard</title>
<meta charset="utf-8">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0a0a0a; color: #e0e0e0; }}
.header {{ background: linear-gradient(135deg, #1a1a2e, #16213e); padding: 20px 30px; border-bottom: 2px solid #e74c3c; }}
.header h1 {{ font-size: 24px; color: #fff; }}
.header .subtitle {{ color: #888; font-size: 14px; margin-top: 4px; }}
.stats {{ display: flex; gap: 15px; padding: 15px 30px; background: #111; flex-wrap: wrap; }}
.stat {{ background: #1a1a2e; padding: 12px 20px; border-radius: 8px; min-width: 140px; }}
.stat .label {{ font-size: 11px; color: #888; text-transform: uppercase; }}
.stat .value {{ font-size: 22px; font-weight: 700; color: #2ecc71; }}
.stat .value.red {{ color: #e74c3c; }}
.stat .value.blue {{ color: #3498db; }}
.stat .value.purple {{ color: #9b59b6; }}
.stat .value.orange {{ color: #f39c12; }}
#map {{ height: 500px; width: 100%; }}
.table-container {{ padding: 20px 30px; overflow-x: auto; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th {{ background: #1a1a2e; padding: 10px 8px; text-align: left; position: sticky; top: 0; cursor: pointer; }}
th:hover {{ background: #2a2a4e; }}
td {{ padding: 8px; border-bottom: 1px solid #222; }}
tr:hover {{ background: #1a1a1a; }}
.positive {{ color: #2ecc71; font-weight: 600; }}
.negative {{ color: #e74c3c; }}
.legend {{ display: flex; gap: 20px; padding: 10px 30px; flex-wrap: wrap; }}
.legend-item {{ display: flex; align-items: center; gap: 6px; font-size: 13px; }}
.legend-dot {{ width: 12px; height: 12px; border-radius: 50%; }}
.filter-bar {{ padding: 10px 30px; display: flex; gap: 10px; flex-wrap: wrap; }}
.filter-btn {{ padding: 6px 14px; border: 1px solid #333; background: #1a1a2e; color: #ccc; border-radius: 4px; cursor: pointer; font-size: 12px; }}
.filter-btn.active {{ border-color: #2ecc71; color: #2ecc71; background: #0a2a0a; }}
</style>
</head>
<body>
<div class="header">
    <h1>⚔️ PharmacyFinder v4 — Greenfield Opportunities</h1>
    <div class="subtitle">National scan with GapMaps verified data (6,503 pharmacies) + OSRM road distances — Generated {__import__('datetime').datetime.now().strftime('%d %b %Y %I:%M %p')}</div>
</div>

<div class="stats">
    <div class="stat"><div class="label">Item 136 (Medical)</div><div class="value">{rule_counts.get('Item 136', 0)}</div></div>
    <div class="stat"><div class="label">Item 131 (Rural)</div><div class="value blue">{rule_counts.get('Item 131', 0)}</div></div>
    <div class="stat"><div class="label">Item 130 (Super+GP)</div><div class="value red">{rule_counts.get('Item 130', 0)}</div></div>
    <div class="stat"><div class="label">Item 133 (Shopping)</div><div class="value purple">{rule_counts.get('Item 133', 0)}</div></div>
    <div class="stat"><div class="label">Item 132 (1-pharm)</div><div class="value orange">{rule_counts.get('Item 132', 0)}</div></div>
    <div class="stat"><div class="label">Total Sites</div><div class="value">{sum(rule_counts.values())}</div></div>
    <div class="stat"><div class="label">Best Flip (18mo)</div><div class="value">$824k</div></div>
    <div class="stat"><div class="label">Best ROI</div><div class="value">51%</div></div>
</div>

<div class="legend">
    <div class="legend-item"><div class="legend-dot" style="background:#2ecc71"></div> Item 136 — Large Medical Centre</div>
    <div class="legend-item"><div class="legend-dot" style="background:#e74c3c"></div> Item 130 — Supermarket + GP</div>
    <div class="legend-item"><div class="legend-dot" style="background:#3498db"></div> Item 131 — Rural Gap (top 50)</div>
    <div class="legend-item"><div class="legend-dot" style="background:#9b59b6"></div> Item 133 — Shopping Centre</div>
    <div class="legend-item"><div class="legend-dot" style="background:#f39c12"></div> Item 132 — One-Pharmacy Town</div>
</div>

<div id="map"></div>

<div class="table-container">
    <h2 style="margin-bottom:15px;color:#fff">Top 50 by Profitability (18-month flip)</h2>
    <table id="profTable">
        <thead>
            <tr>
                <th>#</th><th>Rule</th><th>State</th><th>Name</th><th>Scr/day</th>
                <th>Revenue</th><th>Profit/yr</th><th>Margin</th><th>Setup</th>
                <th>18mo Flip</th><th>ROI%</th><th>Payback</th>
            </tr>
        </thead>
        <tbody>
"""
    
    for i, row in enumerate(prof_rows):
        flip_avg = (row['flip_low'] + row['flip_high']) // 2
        flip_class = 'positive' if flip_avg > 0 else 'negative'
        payback = f"{row['payback']}mo" if row['payback'] < 100 else "N/A"
        html += f"""            <tr onclick="map.setView([{row['lat']},{row['lon']}], 13)">
                <td>{i+1}</td><td>{row['rule'].replace('Item ','')}</td><td>{row['state']}</td>
                <td>{row['name'][:35]}</td><td>{row['scripts']}</td>
                <td>${row['revenue']:,}</td><td>${row['profit']:,}</td>
                <td>{row['margin']}%</td><td>${row['setup']:,}</td>
                <td class="{flip_class}">${flip_avg:+,}</td>
                <td>{row['roi']}%</td><td>{payback}</td>
            </tr>
"""
    
    html += """        </tbody>
    </table>
</div>

<script>
"""
    html += f"var points = {json.dumps(map_points)};\n"
    html += """
var map = L.map('map').setView([-25.5, 134], 4);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 19, attribution: '&copy; CARTO'
}).addTo(map);

points.forEach(function(p) {
    var marker = L.circleMarker([p.lat, p.lon], {
        radius: p.rule === 'Item 131' ? 4 : 8,
        fillColor: p.color,
        color: '#fff',
        weight: 1,
        opacity: 0.8,
        fillOpacity: 0.7
    }).addTo(map);
    
    marker.bindPopup(
        '<b>' + p.name + '</b><br>' +
        '<b>' + p.rule + '</b> | ' + p.state + '<br>' +
        'Confidence: ' + p.conf + '<br>' +
        'Nearest pharmacy: ' + p.dist + 'km<br>' +
        '<small>' + p.reason + '</small>'
    );
});
</script>
</body>
</html>"""
    
    outpath = os.path.join(OUTPUT_DIR, 'dashboard_v4.html')
    with open(outpath, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard written to: {outpath}")
    print(f"Map points: {len(map_points)}")
    print(f"Profitability rows: {len(prof_rows)}")

if __name__ == '__main__':
    main()
