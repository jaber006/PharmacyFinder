#!/usr/bin/env python3
"""
PharmacyFinder Dashboard v4 - Leaderboard
Generates a self-contained HTML dashboard at output/dashboard.html
"""

import sqlite3
import os
import json
import html

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'dashboard.html')

EXCLUDE_VERIFICATIONS = [
    'false_positive', 'false positive', 'pharmacy exists',
    'below_threshold', 'fail_pharmacy_exists'
]

MIN_POP_10KM = 2000


def query_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get opportunities
    cur = conn.cursor()
    cur.execute("SELECT * FROM opportunities")
    all_opps = [dict(row) for row in cur.fetchall()]

    # Filter
    opps = []
    for o in all_opps:
        ver = (o.get('verification') or '').strip().lower()
        if any(ex in ver for ex in EXCLUDE_VERIFICATIONS):
            continue
        pop = o.get('pop_10km') or 0
        if pop < MIN_POP_10KM:
            continue
        opps.append(o)

    # Calculate people_per_pharmacy and sort
    for o in opps:
        pop = o.get('pop_10km') or 0
        pharm = o.get('pharmacy_10km') or 0
        if pharm > 0:
            o['people_per_pharmacy'] = round(pop / pharm, 1)
        else:
            o['people_per_pharmacy'] = pop
        # Town fallback
        o['town'] = o.get('nearest_town') or o.get('address') or 'Unknown'
        o['state'] = o.get('region') or ''

    opps.sort(key=lambda x: x['people_per_pharmacy'], reverse=True)

    # Get pharmacies
    cur.execute("SELECT id, name, latitude, longitude, state, suburb FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    pharmacies = [dict(row) for row in cur.fetchall()]

    conn.close()
    return opps, pharmacies


def status_badge(verification):
    v = (verification or '').strip().upper()
    if 'VERIFIED' in v and 'UN' not in v:
        return '<span class="badge badge-green">VERIFIED</span>'
    elif 'REVIEW' in v or 'NEEDS' in v:
        return '<span class="badge badge-yellow">NEEDS REVIEW</span>'
    else:
        return '<span class="badge badge-grey">UNVERIFIED</span>'


def build_html(opps, pharmacies):
    # Prepare JSON data for JS
    opp_json = []
    for i, o in enumerate(opps):
        opp_json.append({
            'rank': i + 1,
            'id': o.get('id'),
            'town': o['town'],
            'state': o['state'],
            'pop_10km': o.get('pop_10km') or 0,
            'pharmacy_10km': o.get('pharmacy_10km') or 0,
            'people_per_pharmacy': o['people_per_pharmacy'],
            'nearest_km': round(o.get('nearest_pharmacy_km') or 0, 1),
            'rules': o.get('qualifying_rules') or '',
            'verification': o.get('verification') or '',
            'score': round(o.get('composite_score') or 0, 1),
            'lat': o.get('latitude'),
            'lng': o.get('longitude'),
        })

    pharm_json = []
    for p in pharmacies:
        pharm_json.append({
            'id': p['id'],
            'name': p['name'],
            'lat': p['latitude'],
            'lng': p['longitude'],
            'state': p.get('state') or '',
            'suburb': p.get('suburb') or '',
        })

    # Compute stats
    total_opps = len(opp_json)
    total_pharmacies = len(pharm_json)
    if total_opps > 0:
        avg_ppp = round(sum(o['people_per_pharmacy'] for o in opp_json) / total_opps, 0)
    else:
        avg_ppp = 0

    # Collect unique states
    states = sorted(set(o['state'] for o in opp_json if o['state']))

    states_options = ''.join(f'<option value="{html.escape(s)}">{html.escape(s)}</option>' for s in states)

    opp_data_js = json.dumps(opp_json)
    pharm_data_js = json.dumps(pharm_json)

    page_html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PharmacyFinder Leaderboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
html, body {{ height:100%; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background:#0f0f1a; color:#e0e0e0; overflow:hidden; }}

.container {{ display:flex; height:100vh; }}

/* LEFT SIDEBAR */
.sidebar {{ width:40%; min-width:480px; background:#1a1a2e; display:flex; flex-direction:column; border-right:1px solid #2a2a4a; }}

.header {{ padding:16px 20px 12px; border-bottom:1px solid #2a2a4a; flex-shrink:0; }}
.header h1 {{ font-size:20px; font-weight:700; color:#fff; margin-bottom:8px; letter-spacing:-0.3px; }}
.header h1 span {{ color:#4ade80; }}

.stats {{ display:flex; gap:20px; margin-bottom:12px; }}
.stat {{ text-align:center; }}
.stat-val {{ font-size:22px; font-weight:700; color:#fff; }}
.stat-label {{ font-size:11px; color:#888; text-transform:uppercase; letter-spacing:0.5px; }}

.controls {{ display:flex; gap:8px; align-items:center; }}
.controls input, .controls select {{
    background:#16162b; border:1px solid #333; color:#e0e0e0;
    padding:7px 10px; border-radius:6px; font-size:13px; outline:none;
}}
.controls input:focus, .controls select:focus {{ border-color:#4ade80; }}
.controls input {{ flex:1; min-width:120px; }}
.controls select {{ min-width:70px; }}
.btn-export {{
    background:#4ade80; color:#1a1a2e; border:none; padding:7px 14px;
    border-radius:6px; font-size:13px; font-weight:600; cursor:pointer; white-space:nowrap;
}}
.btn-export:hover {{ background:#22c55e; }}

/* TABLE */
.table-wrap {{ flex:1; overflow-y:auto; overflow-x:auto; }}
.table-wrap::-webkit-scrollbar {{ width:6px; }}
.table-wrap::-webkit-scrollbar-thumb {{ background:#333; border-radius:3px; }}
.table-wrap::-webkit-scrollbar-track {{ background:#1a1a2e; }}

table {{ width:100%; border-collapse:collapse; font-size:12px; }}
thead {{ position:sticky; top:0; z-index:2; }}
thead th {{
    background:#16162b; color:#999; font-weight:600; text-transform:uppercase;
    font-size:10px; letter-spacing:0.5px; padding:8px 6px; text-align:left;
    cursor:pointer; user-select:none; white-space:nowrap; border-bottom:2px solid #2a2a4a;
}}
thead th:hover {{ color:#4ade80; }}
thead th.sort-asc::after {{ content:" ▲"; color:#4ade80; }}
thead th.sort-desc::after {{ content:" ▼"; color:#4ade80; }}

tbody tr {{
    border-bottom:1px solid #222; cursor:pointer; transition: background 0.15s;
}}
tbody tr:hover {{ background:#252545; }}
tbody tr.active {{ background:#2a2a5a !important; }}
tbody tr.gold {{ background: rgba(255, 215, 0, 0.07); }}
tbody tr.gold:hover {{ background: rgba(255, 215, 0, 0.14); }}
tbody tr.gold.active {{ background: rgba(255, 215, 0, 0.2) !important; }}

td {{ padding:6px 6px; white-space:nowrap; }}
td.rank {{ color:#666; font-size:11px; text-align:center; width:30px; }}
td.town {{ color:#fff; font-weight:500; max-width:160px; overflow:hidden; text-overflow:ellipsis; }}
td.ppp {{ color:#4ade80; font-weight:700; font-size:13px; }}
td.rules {{ max-width:100px; overflow:hidden; text-overflow:ellipsis; color:#888; font-size:11px; }}

.badge {{
    display:inline-block; padding:2px 7px; border-radius:10px;
    font-size:10px; font-weight:600; text-transform:uppercase; letter-spacing:0.3px;
}}
.badge-green {{ background:rgba(74,222,128,0.2); color:#4ade80; }}
.badge-yellow {{ background:rgba(250,204,21,0.2); color:#facc15; }}
.badge-grey {{ background:rgba(150,150,150,0.15); color:#888; }}

/* MAP */
.map-wrap {{ width:60%; position:relative; }}
#map {{ width:100%; height:100%; }}

/* Leaflet popup */
.leaflet-popup-content-wrapper {{ background:#1a1a2e; color:#e0e0e0; border-radius:8px; }}
.leaflet-popup-tip {{ background:#1a1a2e; }}
.leaflet-popup-content {{ font-size:13px; line-height:1.5; }}
.leaflet-popup-content b {{ color:#4ade80; }}

/* Cluster overrides */
.marker-cluster-small {{ background-color: rgba(220, 38, 38, 0.3); }}
.marker-cluster-small div {{ background-color: rgba(220, 38, 38, 0.6); color:#fff; }}
.marker-cluster-medium {{ background-color: rgba(220, 38, 38, 0.35); }}
.marker-cluster-medium div {{ background-color: rgba(220, 38, 38, 0.65); color:#fff; }}
.marker-cluster-large {{ background-color: rgba(220, 38, 38, 0.4); }}
.marker-cluster-large div {{ background-color: rgba(220, 38, 38, 0.7); color:#fff; }}

.count-badge {{
    position:absolute; bottom:8px; right:8px; background:rgba(0,0,0,0.7);
    color:#999; font-size:11px; padding:3px 8px; border-radius:4px; z-index:1000;
}}
</style>
</head>
<body>
<div class="container">
  <div class="sidebar">
    <div class="header">
      <h1>🏥 PharmacyFinder <span>Leaderboard</span></h1>
      <div class="stats">
        <div class="stat"><div class="stat-val" id="statOpps">{total_opps}</div><div class="stat-label">Opportunities</div></div>
        <div class="stat"><div class="stat-val">{total_pharmacies:,}</div><div class="stat-label">Pharmacies</div></div>
        <div class="stat"><div class="stat-val" id="statAvg">{int(avg_ppp):,}</div><div class="stat-label">Avg People/Pharmacy</div></div>
      </div>
      <div class="controls">
        <input type="text" id="searchBox" placeholder="Search town...">
        <select id="stateFilter">
          <option value="">All States</option>
          {states_options}
        </select>
        <button class="btn-export" onclick="exportCSV()">⬇ CSV</button>
      </div>
    </div>
    <div class="table-wrap" id="tableWrap">
      <table>
        <thead>
          <tr>
            <th data-col="rank" data-type="num">#</th>
            <th data-col="town" data-type="str">Town</th>
            <th data-col="state" data-type="str">State</th>
            <th data-col="pop_10km" data-type="num">Pop 10km</th>
            <th data-col="pharmacy_10km" data-type="num">Pharm</th>
            <th data-col="people_per_pharmacy" data-type="num">People/Pharm</th>
            <th data-col="nearest_km" data-type="num">Nearest km</th>
            <th data-col="rules" data-type="str">Rules</th>
            <th data-col="verification" data-type="str">Status</th>
            <th data-col="score" data-type="num">Score</th>
          </tr>
        </thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>
  <div class="map-wrap">
    <div id="map"></div>
    <div class="count-badge" id="countBadge"></div>
  </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<script>
// ── DATA ──
const OPPS = {opp_data_js};
const PHARMS = {pharm_data_js};

// ── STATE ──
let filteredOpps = [...OPPS];
let sortCol = 'people_per_pharmacy';
let sortDir = 'desc';
let activeOppId = null;
let oppMarkers = {{}};
let highlightCircle = null;

// ── MAP INIT ──
const map = L.map('map', {{ zoomControl: true }}).setView([-25.5, 134], 5);
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
    attribution: '&copy; OSM &copy; CARTO', maxZoom: 19
}}).addTo(map);

// Pharmacy clusters (red)
const pharmCluster = L.markerClusterGroup({{
    maxClusterRadius: 50,
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
    iconCreateFunction: function(cluster) {{
        const count = cluster.getChildCount();
        let size = 'small';
        if (count > 50) size = 'large';
        else if (count > 20) size = 'medium';
        return L.divIcon({{
            html: '<div>' + count + '</div>',
            className: 'marker-cluster marker-cluster-' + size,
            iconSize: L.point(40, 40)
        }});
    }}
}});

PHARMS.forEach(p => {{
    const m = L.circleMarker([p.lat, p.lng], {{
        radius: 3, color: '#dc2626', fillColor: '#dc2626', fillOpacity: 0.6, weight: 1
    }});
    m.bindPopup(`<b style="color:#dc2626">${{p.name}}</b><br>${{p.suburb}}, ${{p.state}}`);
    pharmCluster.addLayer(m);
}});
map.addLayer(pharmCluster);

// Opportunity markers (green circles)
function addOppMarkers() {{
    // Clear old
    Object.values(oppMarkers).forEach(m => map.removeLayer(m));
    oppMarkers = {{}};

    const maxPPP = Math.max(...filteredOpps.map(o => o.people_per_pharmacy), 1);

    filteredOpps.forEach(o => {{
        if (!o.lat || !o.lng) return;
        const radius = 6 + (o.people_per_pharmacy / maxPPP) * 18;
        const m = L.circleMarker([o.lat, o.lng], {{
            radius: radius,
            color: '#4ade80',
            fillColor: '#4ade80',
            fillOpacity: 0.35,
            weight: 1.5,
        }});
        m.bindPopup(
            `<b>${{o.town}}</b> (${{o.state}})<br>` +
            `People/Pharmacy: <b>${{o.people_per_pharmacy.toLocaleString()}}</b><br>` +
            `Pop 10km: ${{o.pop_10km.toLocaleString()}}<br>` +
            `Pharmacies: ${{o.pharmacy_10km}}<br>` +
            `Nearest: ${{o.nearest_km}} km<br>` +
            `Score: ${{o.score}}`
        );
        m.on('click', () => selectOpp(o.id, 'map'));
        m.addTo(map);
        oppMarkers[o.id] = m;
    }});
}}

// ── TABLE RENDER ──
function statusBadge(v) {{
    const u = (v || '').toUpperCase();
    if (u.includes('VERIFIED') && !u.includes('UN'))
        return '<span class="badge badge-green">VERIFIED</span>';
    if (u.includes('REVIEW') || u.includes('NEEDS'))
        return '<span class="badge badge-yellow">NEEDS REVIEW</span>';
    return '<span class="badge badge-grey">UNVERIFIED</span>';
}}

function renderTable() {{
    const tbody = document.getElementById('tbody');
    let html = '';
    filteredOpps.forEach((o, i) => {{
        const idx = i + 1;
        const goldClass = idx <= 10 ? ' gold' : '';
        html += `<tr class="opp-row${{goldClass}}" data-id="${{o.id}}" onclick="selectOpp(${{o.id}}, 'table')">` +
            `<td class="rank">${{idx}}</td>` +
            `<td class="town" title="${{o.town}}">${{o.town}}</td>` +
            `<td>${{o.state}}</td>` +
            `<td>${{o.pop_10km.toLocaleString()}}</td>` +
            `<td>${{o.pharmacy_10km}}</td>` +
            `<td class="ppp">${{o.people_per_pharmacy.toLocaleString()}}</td>` +
            `<td>${{o.nearest_km}}</td>` +
            `<td class="rules" title="${{o.rules}}">${{o.rules}}</td>` +
            `<td>${{statusBadge(o.verification)}}</td>` +
            `<td>${{o.score}}</td>` +
            `</tr>`;
    }});
    tbody.innerHTML = html;
    document.getElementById('countBadge').textContent = filteredOpps.length + ' opportunities shown';
    document.getElementById('statOpps').textContent = filteredOpps.length;

    // Update avg
    if (filteredOpps.length > 0) {{
        const avg = Math.round(filteredOpps.reduce((s,o) => s + o.people_per_pharmacy, 0) / filteredOpps.length);
        document.getElementById('statAvg').textContent = avg.toLocaleString();
    }}
}}

// ── SELECTION ──
function selectOpp(id, source) {{
    activeOppId = id;
    // Highlight table row
    document.querySelectorAll('.opp-row').forEach(r => r.classList.remove('active'));
    const row = document.querySelector(`tr[data-id="${{id}}"]`);
    if (row) {{
        row.classList.add('active');
        if (source === 'map') {{
            row.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
        }}
    }}
    // Highlight map marker
    if (highlightCircle) {{ map.removeLayer(highlightCircle); highlightCircle = null; }}
    const opp = OPPS.find(o => o.id === id);
    if (opp && opp.lat && opp.lng) {{
        if (source === 'table') {{
            map.flyTo([opp.lat, opp.lng], 11, {{ duration: 0.8 }});
        }}
        highlightCircle = L.circleMarker([opp.lat, opp.lng], {{
            radius: 22, color: '#fff', fillColor: '#4ade80', fillOpacity: 0.3, weight: 2, dashArray: '4,4'
        }}).addTo(map);

        // Open popup
        const m = oppMarkers[id];
        if (m) m.openPopup();
    }}
}}

// ── SORT ──
function doSort(col, type) {{
    if (sortCol === col) {{
        sortDir = sortDir === 'desc' ? 'asc' : 'desc';
    }} else {{
        sortCol = col;
        sortDir = type === 'num' ? 'desc' : 'asc';
    }}
    // Update header classes
    document.querySelectorAll('thead th').forEach(th => {{
        th.classList.remove('sort-asc', 'sort-desc');
        if (th.dataset.col === sortCol) {{
            th.classList.add(sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
        }}
    }});
    filteredOpps.sort((a, b) => {{
        let va = a[col], vb = b[col];
        if (type === 'num') {{
            va = Number(va) || 0; vb = Number(vb) || 0;
            return sortDir === 'asc' ? va - vb : vb - va;
        }}
        va = String(va || '').toLowerCase(); vb = String(vb || '').toLowerCase();
        return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
    }});
    renderTable();
}}

document.querySelectorAll('thead th').forEach(th => {{
    th.addEventListener('click', () => doSort(th.dataset.col, th.dataset.type));
}});

// ── FILTER ──
function applyFilters() {{
    const search = document.getElementById('searchBox').value.toLowerCase().trim();
    const state = document.getElementById('stateFilter').value;

    filteredOpps = OPPS.filter(o => {{
        if (search && !o.town.toLowerCase().includes(search)) return false;
        if (state && o.state !== state) return false;
        return true;
    }});

    // Re-apply current sort
    const th = document.querySelector(`thead th[data-col="${{sortCol}}"]`);
    const type = th ? th.dataset.type : 'num';
    filteredOpps.sort((a, b) => {{
        let va = a[sortCol], vb = b[sortCol];
        if (type === 'num') {{
            va = Number(va) || 0; vb = Number(vb) || 0;
            return sortDir === 'asc' ? va - vb : vb - va;
        }}
        va = String(va || '').toLowerCase(); vb = String(vb || '').toLowerCase();
        return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
    }});

    renderTable();
    addOppMarkers();
}}

document.getElementById('searchBox').addEventListener('input', applyFilters);
document.getElementById('stateFilter').addEventListener('change', applyFilters);

// ── CSV EXPORT ──
function exportCSV() {{
    const headers = ['Rank','Town','State','Pop_10km','Pharmacies','People_Per_Pharmacy','Nearest_km','Rules','Status','Score','Latitude','Longitude'];
    const rows = filteredOpps.map((o, i) => [
        i+1, '"'+o.town.replace(/"/g,'""')+'"', o.state, o.pop_10km, o.pharmacy_10km,
        o.people_per_pharmacy, o.nearest_km, '"'+o.rules.replace(/"/g,'""')+'"',
        '"'+(o.verification||'').replace(/"/g,'""')+'"', o.score, o.lat, o.lng
    ]);
    let csv = headers.join(',') + '\\n' + rows.map(r => r.join(',')).join('\\n');
    const blob = new Blob([csv], {{ type: 'text/csv' }});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'pharmacy_opportunities.csv';
    a.click();
}}

// ── INIT ──
// Set initial sort indicator
document.querySelector('thead th[data-col="people_per_pharmacy"]').classList.add('sort-desc');

renderTable();
addOppMarkers();

// Fit map to opportunity bounds
if (filteredOpps.length > 0) {{
    const bounds = filteredOpps
        .filter(o => o.lat && o.lng)
        .map(o => [o.lat, o.lng]);
    if (bounds.length > 0) map.fitBounds(bounds, {{ padding: [30, 30] }});
}}
</script>
</body>
</html>'''

    return page_html


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Querying database...")
    opps, pharmacies = query_data()
    print(f"  {len(opps)} opportunities (after filtering)")
    print(f"  {len(pharmacies)} pharmacies")

    print("Building HTML...")
    page_html = build_html(opps, pharmacies)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(page_html)

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"Dashboard written to {OUTPUT_FILE}")
    print(f"  Size: {size_kb:.0f} KB")
    print("Done.")


if __name__ == '__main__':
    main()
