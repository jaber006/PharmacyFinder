#!/usr/bin/env python3
"""
Build an interactive web dashboard for PharmacyFinder opportunities.

Generates a self-contained HTML file with:
- Leaflet.js map with RED markers for existing pharmacies (clustered)
- GREEN markers for potential new pharmacy sites (opportunities)
- Interactive click on GREEN markers draws rule-specific radii, lines to POIs
- Sidebar filters (state, population, distance, competition)
- Sortable table of all opportunities
- Competition data overlay
- Growth corridor indicators
"""

import csv
import json
import os
import sqlite3

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']


def load_opportunities():
    """Load opportunities directly from the database (single source of truth)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""SELECT * FROM opportunities 
                   WHERE latitude IS NOT NULL AND longitude IS NOT NULL 
                   ORDER BY composite_score DESC""")
    
    all_opportunities = []
    for row in cur.fetchall():
        try:
            opp = {
                'lat': float(row['latitude'] or 0),
                'lng': float(row['longitude'] or 0),
                'name': row['poi_name'] or 'Unknown',
                'address': row['address'] or '',
                'rules': row['qualifying_rules'] or '',
                'evidence': (row['evidence'] or '')[:300],
                'confidence': row['confidence'] or '0%',
                'nearest_pharmacy_km': float(row['nearest_pharmacy_km'] or 0),
                'nearest_pharmacy': row['nearest_pharmacy_name'] or '',
                'poi_type': row['poi_type'] or '',
                'state': row['region'] or '',
                'verification': row['verification'] if row['verification'] in ('VERIFIED', 'FALSE POSITIVE', 'NEEDS REVIEW', 'UNVERIFIED') else 'UNVERIFIED',
                'pop_5km': int(row['pop_5km'] or 0),
                'pop_10km': int(row['pop_10km'] or 0),
                'pop_15km': int(row['pop_15km'] or 0),
                'nearest_town': row['nearest_town'] or '',
                'opp_score': float(row['opp_score'] or 0),
                'pharmacy_5km': int(row['pharmacy_5km'] or 0),
                'pharmacy_10km': int(row['pharmacy_10km'] or 0),
                'pharmacy_15km': int(row['pharmacy_15km'] or 0),
                'chain_count': int(row['chain_count'] or 0),
                'independent_count': int(row['independent_count'] or 0),
                'competition_score': float(row['competition_score'] or 0),
                'composite_score': float(row['composite_score'] or 0),
                'nearest_competitors': row['nearest_competitors'] or '',
                'growth_indicator': row['growth_indicator'] or '',
                'growth_details': row['growth_details'] or '',
            }
            all_opportunities.append(opp)
        except (ValueError, TypeError, IndexError):
            continue

    conn.close()

    print(f"  Loaded {len(all_opportunities)} total opportunities (from DB)")
    for state in STATES:
        count = sum(1 for o in all_opportunities if o['state'] == state)
        if count > 0:
            print(f"    {state}: {count}")

    return all_opportunities


def load_pharmacies():
    """Load all existing pharmacies from the database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT id, name, address, latitude, longitude, state, suburb, postcode FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    pharmacies = []
    for row in cur.fetchall():
        pharmacies.append({
            'id': row['id'],
            'name': row['name'] or 'Unknown Pharmacy',
            'address': row['address'] or '',
            'lat': row['latitude'],
            'lng': row['longitude'],
            'state': row['state'] or '',
            'suburb': row['suburb'] or '',
            'postcode': row['postcode'] or '',
        })
    conn.close()
    print(f"  Loaded {len(pharmacies)} pharmacies")
    return pharmacies


def load_pois():
    """Load all POIs from database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    pois = {'supermarkets': [], 'hospitals': [], 'shopping_centres': [], 'medical_centres': [], 'gps': []}

    cur.execute("SELECT id, name, address, latitude, longitude, floor_area_sqm, brand FROM supermarkets WHERE latitude IS NOT NULL")
    for row in cur.fetchall():
        pois['supermarkets'].append({
            'id': row['id'], 'name': row['name'] or '', 'address': row['address'] or '',
            'lat': row['latitude'], 'lng': row['longitude'],
            'floor_area': row['floor_area_sqm'] or 0, 'brand': row['brand'] or '',
        })

    cur.execute("SELECT id, name, address, latitude, longitude, bed_count, hospital_type FROM hospitals WHERE latitude IS NOT NULL")
    for row in cur.fetchall():
        pois['hospitals'].append({
            'id': row['id'], 'name': row['name'] or '', 'address': row['address'] or '',
            'lat': row['latitude'], 'lng': row['longitude'],
            'beds': row['bed_count'] or 0, 'type': row['hospital_type'] or '',
        })

    cur.execute("SELECT id, name, address, latitude, longitude, gla_sqm, major_supermarkets, centre_class FROM shopping_centres WHERE latitude IS NOT NULL")
    for row in cur.fetchall():
        pois['shopping_centres'].append({
            'id': row['id'], 'name': row['name'] or '', 'address': row['address'] or '',
            'lat': row['latitude'], 'lng': row['longitude'],
            'gla': row['gla_sqm'] or 0, 'supermarkets': row['major_supermarkets'] or '',
            'cls': row['centre_class'] or '',
        })

    cur.execute("SELECT id, name, address, latitude, longitude, num_gps, total_fte, hours_per_week, state FROM medical_centres WHERE latitude IS NOT NULL")
    for row in cur.fetchall():
        pois['medical_centres'].append({
            'id': row['id'], 'name': row['name'] or '', 'address': row['address'] or '',
            'lat': row['latitude'], 'lng': row['longitude'],
            'gps': row['num_gps'] or 0, 'fte': row['total_fte'] or 0,
            'hours': row['hours_per_week'] or 0, 'state': row['state'] or '',
        })

    cur.execute("SELECT id, name, address, latitude, longitude, fte, hours_per_week FROM gps WHERE latitude IS NOT NULL")
    for row in cur.fetchall():
        pois['gps'].append({
            'id': row['id'], 'name': row['name'] or '', 'address': row['address'] or '',
            'lat': row['latitude'], 'lng': row['longitude'],
            'fte': row['fte'] or 0, 'hours': row['hours_per_week'] or 0,
        })

    conn.close()
    for k, v in pois.items():
        print(f"  Loaded {len(v)} {k}")
    return pois


def generate_html(opportunities, pharmacies, pois):
    """Generate the dashboard HTML using template replacement to avoid f-string brace issues."""
    opp_json = json.dumps(opportunities, ensure_ascii=False)
    pharm_json = json.dumps(pharmacies, ensure_ascii=False)
    pois_json = json.dumps(pois, ensure_ascii=False)

    # Use placeholder replacement instead of f-strings to keep JS braces clean
    template = _get_html_template()
    html_content = template.replace('__OPP_DATA__', opp_json)
    html_content = html_content.replace('__PHARM_DATA__', pharm_json)
    html_content = html_content.replace('__POI_DATA__', pois_json)

    return html_content


def _get_html_template():
    return r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PharmacyFinder - Greenfield Opportunity Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, sans-serif; background: #f0f2f5; color: #1a1a2e; }

.header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    color: white; padding: 0 24px; display: flex; align-items: center;
    justify-content: space-between; box-shadow: 0 2px 10px rgba(0,0,0,0.3);
    position: fixed; top: 0; left: 0; right: 0; z-index: 1000; height: 56px;
}
.header h1 { font-size: 18px; font-weight: 700; letter-spacing: -0.5px; }
.header h1 span { color: #53c587; }
.header .stats { font-size: 12px; color: #94a3b8; display: flex; gap: 16px; }
.header .stats .stat-val { color: #53c587; font-weight: 600; }

.main { display: flex; margin-top: 56px; height: calc(100vh - 56px); }

.sidebar {
    width: 280px; min-width: 280px; background: white; padding: 12px;
    overflow-y: auto; border-right: 1px solid #e2e8f0;
    box-shadow: 2px 0 10px rgba(0,0,0,0.05);
}
.sidebar h3 { font-size: 11px; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px; margin-bottom: 8px; font-weight: 600; }
.filter-section { margin-bottom: 14px; padding-bottom: 12px; border-bottom: 1px solid #f1f5f9; }
.filter-section:last-child { border-bottom: none; }

.state-filters { display: grid; grid-template-columns: 1fr 1fr; gap: 2px; }
.state-cb { display: flex; align-items: center; gap: 5px; padding: 3px 6px; border-radius: 6px; cursor: pointer; transition: background 0.15s; font-size: 12px; }
.state-cb:hover { background: #f8fafc; }
.state-cb input { accent-color: var(--state-color); }
.state-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.state-count { color: #94a3b8; font-size: 10px; margin-left: auto; }

.slider-group { margin-bottom: 10px; }
.slider-label { display: flex; justify-content: space-between; font-size: 11px; color: #475569; margin-bottom: 3px; }
.slider-val { font-weight: 600; color: #1a1a2e; }
input[type="range"] { width: 100%; height: 5px; -webkit-appearance: none; background: #e2e8f0; border-radius: 3px; outline: none; }
input[type="range"]::-webkit-slider-thumb { -webkit-appearance: none; width: 14px; height: 14px; border-radius: 50%; background: #0f3460; cursor: pointer; }

.quick-filters { display: flex; flex-wrap: wrap; gap: 3px; }
.quick-btn { padding: 3px 8px; border: 1px solid #e2e8f0; border-radius: 14px; font-size: 10px; cursor: pointer; background: white; color: #475569; transition: all 0.15s; }
.quick-btn:hover, .quick-btn.active { background: #0f3460; color: white; border-color: #0f3460; }

.badge { display: inline-block; padding: 1px 7px; border-radius: 10px; font-size: 9px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.3px; }
.badge-verified { background: #d1fae5; color: #065f46; }
.badge-false { background: #fee2e2; color: #991b1b; }
.badge-review { background: #fef3c7; color: #92400e; }
.badge-unverified { background: #f1f5f9; color: #64748b; }
.badge-growth { background: #dbeafe; color: #1e40af; }

.map-container { flex: 1; position: relative; }
#map { width: 100%; height: 100%; }

.pharmacy-cluster-small { background-color: rgba(255,150,150,0.6); }
.pharmacy-cluster-small div { background-color: rgba(220,50,50,0.7); color: #fff; font-weight: 700; }
.pharmacy-cluster-medium { background-color: rgba(255,120,120,0.6); }
.pharmacy-cluster-medium div { background-color: rgba(200,40,40,0.7); color: #fff; font-weight: 700; }
.pharmacy-cluster-large { background-color: rgba(255,90,90,0.6); }
.pharmacy-cluster-large div { background-color: rgba(180,30,30,0.7); color: #fff; font-weight: 700; }

.marker-cluster-small { background-color: rgba(140,226,140,0.6); }
.marker-cluster-small div { background-color: rgba(57,180,57,0.7); color: #fff; font-weight: 700; }
.marker-cluster-medium { background-color: rgba(100,210,100,0.6); }
.marker-cluster-medium div { background-color: rgba(40,160,40,0.7); color: #fff; font-weight: 700; }
.marker-cluster-large { background-color: rgba(70,190,70,0.6); }
.marker-cluster-large div { background-color: rgba(30,140,30,0.7); color: #fff; font-weight: 700; }

.table-panel {
    position: fixed; bottom: 0; left: 280px; right: 0; height: 0;
    background: white; box-shadow: 0 -4px 20px rgba(0,0,0,0.15);
    transition: height 0.3s ease; z-index: 900; overflow: hidden;
    border-top: 3px solid #0f3460;
}
.table-panel.open { height: 42vh; }
.table-toggle {
    position: absolute; top: -32px; left: 50%; transform: translateX(-50%);
    background: #0f3460; color: white; border: none; padding: 6px 18px;
    border-radius: 8px 8px 0 0; cursor: pointer; font-size: 12px; font-weight: 600;
    box-shadow: 0 -2px 10px rgba(0,0,0,0.15);
}
.table-toggle:hover { background: #16213e; }
.table-wrap { height: 100%; overflow: auto; }
table { width: 100%; border-collapse: collapse; font-size: 11px; }
thead { position: sticky; top: 0; z-index: 10; }
th { background: #1a1a2e; color: white; padding: 6px 8px; text-align: left; cursor: pointer; white-space: nowrap; font-weight: 600; font-size: 10px; text-transform: uppercase; letter-spacing: 0.3px; }
th:hover { background: #0f3460; }
th.sorted-asc::after { content: " \25B2"; font-size: 8px; }
th.sorted-desc::after { content: " \25BC"; font-size: 8px; }
td { padding: 5px 8px; border-bottom: 1px solid #f1f5f9; white-space: nowrap; }
tr:hover td { background: #f8fafc; }
.td-name { max-width: 180px; overflow: hidden; text-overflow: ellipsis; font-weight: 500; }
.td-score { font-weight: 700; }
.comp-low { color: #059669; }
.comp-med { color: #d97706; }
.comp-high { color: #dc2626; }

.leaflet-popup-content-wrapper { border-radius: 10px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); }
.leaflet-popup-content { margin: 10px; font-size: 12px; line-height: 1.5; min-width: 260px; }
.popup-title { font-size: 14px; font-weight: 700; color: #1a1a2e; margin-bottom: 6px; border-bottom: 2px solid #53c587; padding-bottom: 5px; }
.popup-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 3px 10px; }
.popup-label { color: #64748b; font-size: 10px; }
.popup-value { font-weight: 600; font-size: 12px; }
.popup-section { margin-top: 6px; padding-top: 6px; border-top: 1px solid #f1f5f9; }

@media (max-width: 768px) {
    .sidebar { display: none; }
    .table-panel { left: 0; }
}

.loading { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(26,26,46,0.9); z-index: 9999; display: flex; align-items: center; justify-content: center; flex-direction: column; gap: 12px; }
.loading-text { color: white; font-size: 16px; }
.loading-bar { width: 200px; height: 4px; background: #334155; border-radius: 2px; overflow: hidden; }
.loading-bar-inner { width: 30%; height: 100%; background: #53c587; border-radius: 2px; animation: load 1.2s ease-in-out infinite; }
@keyframes load { 0%{transform:translateX(-100%)} 50%{transform:translateX(200%)} 100%{transform:translateX(-100%)} }
.loading.hidden { display: none; }

.search-box { width: 100%; padding: 7px 10px; border: 1px solid #e2e8f0; border-radius: 8px; font-size: 12px; outline: none; margin-bottom: 10px; }
.search-box:focus { border-color: #0f3460; box-shadow: 0 0 0 3px rgba(15,52,96,0.1); }

.export-btn {
    width: 100%; padding: 8px; background: #0f3460; color: white; border: none;
    border-radius: 8px; cursor: pointer; font-size: 12px; font-weight: 600;
    transition: background 0.15s; margin-top: 8px;
}
.export-btn:hover { background: #16213e; }

.rule-panel {
    position: absolute; top: 66px; right: 12px; width: 340px; max-height: calc(100vh - 160px);
    background: white; border-radius: 12px; box-shadow: 0 8px 32px rgba(0,0,0,0.18);
    z-index: 800; overflow-y: auto; display: none; border: 2px solid #22c55e;
}
.rule-panel.visible { display: block; }
.rule-panel-header {
    background: linear-gradient(135deg, #166534, #22c55e); color: white;
    padding: 12px 16px; border-radius: 10px 10px 0 0; display: flex;
    justify-content: space-between; align-items: center;
}
.rule-panel-header h3 { font-size: 14px; font-weight: 700; }
.rule-panel-close { background: none; border: none; color: white; font-size: 20px; cursor: pointer; line-height: 1; padding: 0 4px; }
.rule-panel-body { padding: 14px 16px; }
.rule-card {
    background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px;
    padding: 10px 12px; margin-bottom: 10px;
}
.rule-card h4 { font-size: 13px; color: #166534; margin-bottom: 5px; display: flex; align-items: center; gap: 6px; }
.rule-card p { font-size: 11px; color: #475569; line-height: 1.5; margin: 0; }
.rule-card .rule-distance { font-size: 12px; font-weight: 700; color: #059669; }
.evidence-block {
    background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px;
    padding: 10px 12px; margin-top: 8px; font-size: 11px; color: #475569; line-height: 1.6;
}
.evidence-block strong { color: #1a1a2e; }
.poi-list { margin-top: 8px; }
.poi-item {
    display: flex; align-items: center; gap: 8px; padding: 5px 0;
    border-bottom: 1px solid #f1f5f9; font-size: 11px;
}
.poi-item:last-child { border-bottom: none; }
.poi-icon { width: 20px; height: 20px; border-radius: 50%; display: flex;
    align-items: center; justify-content: center; font-size: 10px; flex-shrink: 0; }
.poi-icon.pharmacy { background: #fee2e2; }
.poi-icon.supermarket { background: #fef3c7; }
.poi-icon.hospital { background: #dbeafe; }
.poi-icon.shopping { background: #f3e8ff; }
.poi-icon.medical { background: #d1fae5; }
.poi-icon.gp { background: #e0e7ff; }

.map-legend {
    position: absolute; bottom: 20px; left: 12px; background: white;
    border-radius: 10px; padding: 10px 14px; box-shadow: 0 2px 12px rgba(0,0,0,0.12);
    z-index: 800; font-size: 11px;
}
.map-legend h4 { font-size: 11px; margin-bottom: 6px; color: #1a1a2e; }
.legend-item { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
.legend-dot { width: 12px; height: 12px; border-radius: 50%; border: 2px solid #fff; box-shadow: 0 0 3px rgba(0,0,0,0.3); }

.layer-toggle {
    position: absolute; top: 66px; left: 12px; background: white;
    border-radius: 10px; padding: 8px 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.12);
    z-index: 800; font-size: 11px;
}
.layer-toggle label { display: flex; align-items: center; gap: 6px; cursor: pointer; padding: 2px 0; }
</style>
</head>
<body>

<div class="loading" id="loading">
    <div class="loading-text">Loading PharmacyFinder Dashboard...</div>
    <div class="loading-bar"><div class="loading-bar-inner"></div></div>
</div>

<div class="header">
    <h1>&#x1f48a; <span>Pharmacy</span>Finder Dashboard</h1>
    <div class="stats">
        <div>Pharmacies: <span class="stat-val" id="stat-pharmacies">0</span></div>
        <div>Opportunities: <span class="stat-val" id="stat-total">0</span></div>
        <div>Showing: <span class="stat-val" id="stat-showing">0</span></div>
        <div>States: <span class="stat-val" id="stat-states">8</span></div>
        <div>Growth: <span class="stat-val" id="stat-growth">0</span></div>
    </div>
</div>

<div class="main">
    <div class="sidebar">
        <input type="text" class="search-box" id="searchBox" placeholder="&#x1f50d; Search locations, towns, states..." />

        <div class="filter-section">
            <h3>States</h3>
            <div class="state-filters" id="stateFilters"></div>
        </div>

        <div class="filter-section">
            <h3>Quick Filters</h3>
            <div class="quick-filters">
                <button class="quick-btn" onclick="applyQuickFilter('verified')">&#x2705; Verified</button>
                <button class="quick-btn" onclick="applyQuickFilter('low-comp')">&#x1f3af; Low Competition</button>
                <button class="quick-btn" onclick="applyQuickFilter('high-pop')">&#x1f465; High Pop</button>
                <button class="quick-btn" onclick="applyQuickFilter('rural')">&#x1f33e; Rural</button>
                <button class="quick-btn" onclick="applyQuickFilter('growth')">&#x1f4c8; Growth</button>
                <button class="quick-btn" onclick="applyQuickFilter('top50')">&#x1f3c6; Top 50</button>
                <button class="quick-btn" onclick="applyQuickFilter('reset')">&#x1f504; Reset</button>
            </div>
        </div>

        <div class="filter-section">
            <h3>Population (10km radius)</h3>
            <div class="slider-group">
                <div class="slider-label"><span>Minimum</span><span class="slider-val" id="popVal">0</span></div>
                <input type="range" id="popSlider" min="0" max="200000" step="1000" value="0" />
            </div>
        </div>

        <div class="filter-section">
            <h3>Distance to Nearest Pharmacy</h3>
            <div class="slider-group">
                <div class="slider-label"><span>Minimum (km)</span><span class="slider-val" id="distVal">0</span></div>
                <input type="range" id="distSlider" min="0" max="50" step="0.5" value="0" />
            </div>
        </div>

        <div class="filter-section">
            <h3>Competition Score</h3>
            <div class="slider-group">
                <div class="slider-label"><span>Maximum (lower = less competition)</span><span class="slider-val" id="compVal">200</span></div>
                <input type="range" id="compSlider" min="0" max="200" step="1" value="200" />
            </div>
        </div>

        <div class="filter-section">
            <h3>Verification Status</h3>
            <div style="display: flex; flex-direction: column; gap: 3px;">
                <label class="state-cb"><input type="checkbox" class="verif-cb" value="VERIFIED" checked> <span class="badge badge-verified">Verified</span></label>
                <label class="state-cb"><input type="checkbox" class="verif-cb" value="FALSE POSITIVE"> <span class="badge badge-false">False Positive</span></label>
                <label class="state-cb"><input type="checkbox" class="verif-cb" value="NEEDS REVIEW" checked> <span class="badge badge-review">Needs Review</span></label>
                <label class="state-cb"><input type="checkbox" class="verif-cb" value="UNVERIFIED" checked> <span class="badge badge-unverified">Unverified</span></label>
            </div>
        </div>

        <div class="filter-section">
            <h3>Qualifying Rules</h3>
            <div id="ruleFilters" style="display: flex; flex-direction: column; gap: 3px;"></div>
        </div>

        <div class="filter-section">
            <h3>Summary</h3>
            <div id="summaryPanel" style="font-size: 11px; color: #475569;"></div>
        </div>

        <button class="export-btn" onclick="exportCSV()">&#x1f4e5; Export Filtered Results (CSV)</button>
    </div>

    <div class="map-container">
        <div id="map"></div>

        <div class="layer-toggle">
            <label><input type="checkbox" id="togglePharmacies" checked> &#x1f534; Existing Pharmacies</label>
            <label><input type="checkbox" id="toggleOpportunities" checked> &#x1f7e2; Opportunities</label>
        </div>

        <div class="map-legend">
            <h4>Legend</h4>
            <div class="legend-item"><div class="legend-dot" style="background:#dc2626;"></div> Existing Pharmacy</div>
            <div class="legend-item"><div class="legend-dot" style="background:#22c55e;"></div> Opportunity Site</div>
            <div class="legend-item"><div class="legend-dot" style="background:#22c55e; border-color: #f59e0b; border-width:3px;"></div> Growth Area</div>
            <div class="legend-item"><div class="legend-dot" style="background:#3b82f6;"></div> Hospital</div>
            <div class="legend-item"><div class="legend-dot" style="background:#f59e0b;"></div> Supermarket</div>
            <div class="legend-item"><div class="legend-dot" style="background:#8b5cf6;"></div> Shopping Centre</div>
            <div class="legend-item"><div class="legend-dot" style="background:#06b6d4;"></div> Medical Centre / GP</div>
        </div>

        <div class="rule-panel" id="rulePanel">
            <div class="rule-panel-header">
                <h3>&#x1f4cb; Rule Analysis</h3>
                <button class="rule-panel-close" onclick="closeRulePanel()">&times;</button>
            </div>
            <div class="rule-panel-body" id="rulePanelBody"></div>
        </div>

        <div class="table-panel" id="tablePanel">
            <button class="table-toggle" id="tableToggle" onclick="toggleTable()">&#x25B2; Show Table (0 results)</button>
            <div class="table-wrap">
                <table>
                    <thead id="tableHead"></thead>
                    <tbody id="tableBody"></tbody>
                </table>
            </div>
        </div>
    </div>
</div>

<script>
// ============================================================
// EMBEDDED DATA
// ============================================================
const OPP_DATA = __OPP_DATA__;
const PHARM_DATA = __PHARM_DATA__;
const POI_DATA = __POI_DATA__;

// ============================================================
// RULE DEFINITIONS
// ============================================================
const RULE_DEFS = {
    'Item 130': {
        name: 'New Pharmacy (1.5km + Supermarket/GP)',
        radius: 1500,
        radiusLabel: '1.5 km from nearest pharmacy',
        secondaryRadius: 500,
        secondaryLabel: '500m GP/Supermarket zone',
        color: '#22c55e',
        secondaryColor: '#3b82f6',
        description: 'Proposed premises at least 1.5km from nearest pharmacy. Within 500m must have either: (a) 1 FTE GP + supermarket \u22651,000sqm, or (b) supermarket \u22652,500sqm.',
        poiTypes: ['supermarkets', 'gps'],
        pharmacyRadius: 2000,
    },
    'Item 131': {
        name: 'Rural/Remote Pharmacy (10km route)',
        radius: 10000,
        radiusLabel: '10 km from nearest pharmacy (by road)',
        color: '#f97316',
        description: 'Proposed premises at least 10km by shortest lawful access route from nearest pharmacy. No supermarket/GP requirements. Suitable for rural/remote areas.',
        poiTypes: [],
        pharmacyRadius: 15000,
    },
    'Item 132': {
        name: 'Major Shopping Centre (15,000+ sqm)',
        radius: 200,
        radiusLabel: '200m centre proximity zone',
        color: '#8b5cf6',
        description: 'Located within a shopping centre with GLA \u226515,000sqm and at least one major supermarket (Woolworths, Coles, ALDI).',
        poiTypes: ['shopping_centres'],
        pharmacyRadius: 2000,
    },
    'Item 133': {
        name: 'Supermarket (1,000+ sqm)',
        radius: 100,
        radiusLabel: '100m adjacency zone',
        color: '#eab308',
        description: 'Adjoining or within a supermarket premises with floor area \u22651,000sqm. Must be a major chain (Woolworths, Coles, ALDI).',
        poiTypes: ['supermarkets'],
        pharmacyRadius: 2000,
    },
    'Item 134': {
        name: 'Small Shopping Centre (5,000-15,000 sqm)',
        radius: 200,
        radiusLabel: '200m centre proximity zone',
        color: '#a855f7',
        description: 'Located within a shopping centre with GLA 5,000-15,000sqm and at least one major supermarket.',
        poiTypes: ['shopping_centres'],
        pharmacyRadius: 2000,
    },
    'Item 134A': {
        name: 'Very Remote Location (90km)',
        radius: 90000,
        radiusLabel: '90 km from nearest pharmacy',
        color: '#ef4444',
        description: 'Proposed premises at least 90km straight-line from nearest existing pharmacy. For very remote areas.',
        poiTypes: [],
        pharmacyRadius: 100000,
    },
    'Item 135': {
        name: 'Hospital (100+ beds)',
        radius: 300,
        radiusLabel: '300m hospital adjacency zone',
        color: '#3b82f6',
        description: 'Located within or adjacent to a public hospital with \u2265100 beds (acute care facility).',
        poiTypes: ['hospitals'],
        pharmacyRadius: 2000,
    },
    'Item 136': {
        name: 'Large Medical Centre (8+ FTE GPs)',
        radius: 300,
        radiusLabel: '300m from nearest pharmacy (min required)',
        color: '#06b6d4',
        description: 'Located in a large medical centre with \u22658 FTE prescribers, operating \u226570hrs/week. Nearest pharmacy must be \u2265300m away.',
        poiTypes: ['medical_centres', 'gps'],
        pharmacyRadius: 2000,
    },
};

// ============================================================
// STATE
// ============================================================
const STATE_COLORS = {
    'ACT': '#e74c3c', 'NSW': '#3498db', 'NT': '#e67e22', 'QLD': '#9b59b6',
    'SA': '#1abc9c', 'TAS': '#2ecc71', 'VIC': '#34495e', 'WA': '#f39c12'
};

let map, pharmacyCluster, opportunityCluster;
let filteredData = [], tableOpen = false;
let sortCol = 'composite_score', sortDir = 'desc';
let quickFilterActive = null;
let ruleVisualizationGroup = null;
let activeOppMarker = null;

// ============================================================
// HAVERSINE
// ============================================================
function haversine(lat1, lon1, lat2, lon2) {
    const R = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon/2) * Math.sin(dLon/2);
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

// ============================================================
// MAP INIT
// ============================================================
function initMap() {
    map = L.map('map', { zoomControl: true }).setView([-25.5, 134], 5);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
        maxZoom: 19
    }).addTo(map);

    pharmacyCluster = L.markerClusterGroup({
        maxClusterRadius: 50,
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false,
        zoomToBoundsOnClick: true,
        disableClusteringAtZoom: 14,
        iconCreateFunction: function(cluster) {
            var count = cluster.getChildCount();
            var size = 'small';
            if (count > 100) size = 'large';
            else if (count > 30) size = 'medium';
            return L.divIcon({
                html: '<div>' + count + '</div>',
                className: 'marker-cluster pharmacy-cluster-' + size,
                iconSize: L.point(40, 40)
            });
        }
    });

    opportunityCluster = L.markerClusterGroup({
        maxClusterRadius: 40,
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false,
        zoomToBoundsOnClick: true,
        disableClusteringAtZoom: 12,
    });

    map.addLayer(pharmacyCluster);
    map.addLayer(opportunityCluster);

    ruleVisualizationGroup = L.layerGroup().addTo(map);

    map.on('click', function(e) {
        if (!e.originalEvent._markerClicked) {
            clearRuleVisualization();
        }
    });
}

// ============================================================
// PHARMACY MARKERS (RED)
// ============================================================
function loadPharmacyMarkers() {
    pharmacyCluster.clearLayers();
    PHARM_DATA.forEach(function(p) {
        if (!p.lat || !p.lng) return;
        var marker = L.circleMarker([p.lat, p.lng], {
            radius: 4,
            fillColor: '#dc2626',
            color: '#991b1b',
            weight: 1,
            opacity: 0.8,
            fillOpacity: 0.7
        });
        marker.bindPopup(
            '<div style="font-size:12px;">' +
            '<div style="font-weight:700; color:#dc2626; border-bottom:2px solid #dc2626; padding-bottom:4px; margin-bottom:6px;">' +
            '\ud83c\udfe5 ' + p.name +
            '</div>' +
            '<div style="color:#475569; font-size:11px;">' + p.address + '</div>' +
            '<div style="margin-top:4px; font-size:11px; color:#94a3b8;">' +
            p.suburb + ' ' + p.state + ' ' + p.postcode +
            '</div></div>',
            { maxWidth: 280 }
        );
        pharmacyCluster.addLayer(marker);
    });
}

// ============================================================
// OPPORTUNITY MARKERS (GREEN)
// ============================================================
function createOpportunityMarker(d) {
    var isGrowth = d.growth_indicator === 'YES';
    var score = Math.max(d.composite_score, 1);
    var radius = Math.min(Math.max(Math.log10(score) * 2.5, 5), 14);

    var marker = L.circleMarker([d.lat, d.lng], {
        radius: radius,
        fillColor: '#22c55e',
        color: isGrowth ? '#f59e0b' : '#166534',
        weight: isGrowth ? 2.5 : 1.5,
        opacity: 1,
        fillOpacity: 0.85
    });

    var verifBadge = d.verification === 'VERIFIED' ? '<span class="badge badge-verified">Verified</span>' :
        d.verification === 'FALSE POSITIVE' ? '<span class="badge badge-false">False Positive</span>' :
        d.verification === 'NEEDS REVIEW' ? '<span class="badge badge-review">Needs Review</span>' :
        '<span class="badge badge-unverified">Unverified</span>';

    var growthHtml = isGrowth ?
        '<div class="popup-section"><span class="badge badge-growth">\ud83d\udcc8 Growth Area</span> ' + (d.growth_details || '') + '</div>' : '';

    var compClass = d.competition_score <= 5 ? 'comp-low' : d.competition_score <= 30 ? 'comp-med' : 'comp-high';

    marker.bindPopup(
        '<div class="popup-title" style="border-bottom-color: #22c55e;">\ud83d\udfe2 ' + d.name + '</div>' +
        '<div style="margin-bottom:4px">' + verifBadge + ' <span style="color:#64748b;font-size:11px">' + d.rules + '</span></div>' +
        '<div class="popup-grid popup-section">' +
            '<div><div class="popup-label">Population 5km</div><div class="popup-value">' + d.pop_5km.toLocaleString() + '</div></div>' +
            '<div><div class="popup-label">Population 10km</div><div class="popup-value">' + d.pop_10km.toLocaleString() + '</div></div>' +
            '<div><div class="popup-label">Population 15km</div><div class="popup-value">' + d.pop_15km.toLocaleString() + '</div></div>' +
            '<div><div class="popup-label">Nearest Town</div><div class="popup-value">' + d.nearest_town + '</div></div>' +
        '</div>' +
        '<div class="popup-grid popup-section">' +
            '<div><div class="popup-label">Nearest Pharmacy</div><div class="popup-value">' + d.nearest_pharmacy_km.toFixed(1) + ' km</div></div>' +
            '<div><div class="popup-label">Pharmacy Name</div><div class="popup-value" style="white-space:normal;font-size:10px">' + d.nearest_pharmacy + '</div></div>' +
            '<div><div class="popup-label">Pharmacies in 5km</div><div class="popup-value">' + d.pharmacy_5km + '</div></div>' +
            '<div><div class="popup-label">Pharmacies in 10km</div><div class="popup-value">' + d.pharmacy_10km + '</div></div>' +
        '</div>' +
        '<div class="popup-grid popup-section">' +
            '<div><div class="popup-label">Competition</div><div class="popup-value ' + compClass + '">' + d.competition_score + '</div></div>' +
            '<div><div class="popup-label">Composite Score</div><div class="popup-value">' + Math.round(d.composite_score).toLocaleString() + '</div></div>' +
        '</div>' +
        '<div style="margin-top:8px; text-align:center;">' +
            '<button onclick="showRuleVisualization(' + d.lat + ',' + d.lng + ')" style="' +
                'background: linear-gradient(135deg, #166534, #22c55e); color: white; border: none;' +
                'padding: 6px 16px; border-radius: 16px; cursor: pointer; font-size: 11px; font-weight: 600;' +
            '">\ud83d\udd0d Show Rule Analysis</button>' +
        '</div>' +
        growthHtml,
        { maxWidth: 320 }
    );

    marker.on('click', function(e) {
        e.originalEvent._markerClicked = true;
        activeOppMarker = d;
    });

    return marker;
}

// ============================================================
// RULE VISUALIZATION
// ============================================================
function showRuleVisualization(lat, lng) {
    var opp = OPP_DATA.find(function(d) {
        return Math.abs(d.lat - lat) < 0.0001 && Math.abs(d.lng - lng) < 0.0001;
    });
    if (!opp) return;

    clearRuleVisualization();

    var rules = opp.rules.split(',').map(function(r) { return r.trim(); }).filter(function(r) { return r; });
    var panelHtml = '';
    panelHtml += '<div style="font-size:13px; font-weight:600; margin-bottom:8px;">\ud83d\udccd ' + opp.name + '</div>';
    panelHtml += '<div style="font-size:11px; color:#64748b; margin-bottom:12px;">' + (opp.address || 'No address') + '</div>';

    // Find nearby pharmacies
    var nearbyPharmacies = [];
    PHARM_DATA.forEach(function(p) {
        var dist = haversine(lat, lng, p.lat, p.lng);
        nearbyPharmacies.push(Object.assign({}, p, { dist: dist }));
    });
    nearbyPharmacies.sort(function(a, b) { return a.dist - b.dist; });

    // Process each qualifying rule
    rules.forEach(function(ruleKey) {
        var ruleDef = RULE_DEFS[ruleKey];
        if (!ruleDef) {
            panelHtml += '<div class="rule-card"><h4>\u26a0\ufe0f ' + ruleKey + '</h4><p>Unknown rule</p></div>';
            return;
        }

        // Draw primary radius circle
        var circle = L.circle([lat, lng], {
            radius: ruleDef.radius,
            color: ruleDef.color,
            fillColor: ruleDef.color,
            fillOpacity: 0.08,
            weight: 2,
            dashArray: '8,6',
        });
        ruleVisualizationGroup.addLayer(circle);

        // Add label for radius
        var labelIcon = L.divIcon({
            html: '<div style="background:' + ruleDef.color + '; color:white; padding:2px 8px; border-radius:10px; font-size:10px; font-weight:600; white-space:nowrap; box-shadow:0 1px 4px rgba(0,0,0,0.2);">' + ruleDef.radiusLabel + '</div>',
            className: '',
            iconAnchor: [0, 0],
        });
        var labelLat = lat + (ruleDef.radius / 111320);
        var labelMarker = L.marker([labelLat, lng], { icon: labelIcon, interactive: false });
        ruleVisualizationGroup.addLayer(labelMarker);

        // Secondary radius (Item 130)
        if (ruleDef.secondaryRadius) {
            var circle2 = L.circle([lat, lng], {
                radius: ruleDef.secondaryRadius,
                color: ruleDef.secondaryColor,
                fillColor: ruleDef.secondaryColor,
                fillOpacity: 0.06,
                weight: 2,
                dashArray: '4,4',
            });
            ruleVisualizationGroup.addLayer(circle2);

            // Secondary label
            var secLabelIcon = L.divIcon({
                html: '<div style="background:' + ruleDef.secondaryColor + '; color:white; padding:2px 8px; border-radius:10px; font-size:10px; font-weight:600; white-space:nowrap; box-shadow:0 1px 4px rgba(0,0,0,0.2);">' + ruleDef.secondaryLabel + '</div>',
                className: '',
                iconAnchor: [0, 0],
            });
            var secLabelLat = lat + (ruleDef.secondaryRadius / 111320);
            ruleVisualizationGroup.addLayer(L.marker([secLabelLat, lng], { icon: secLabelIcon, interactive: false }));
        }

        // Opportunity marker highlight (green pulsing)
        var oppHighlight = L.circleMarker([lat, lng], {
            radius: 10,
            fillColor: '#22c55e',
            color: '#166534',
            weight: 3,
            fillOpacity: 0.9,
        });
        ruleVisualizationGroup.addLayer(oppHighlight);

        // Draw nearby pharmacies
        var showRadius = Math.max(ruleDef.pharmacyRadius, ruleDef.radius) / 1000;
        var pharmInRange = nearbyPharmacies.filter(function(p) { return p.dist <= showRadius; }).slice(0, 20);
        var poiListHtml = [];

        pharmInRange.forEach(function(p, i) {
            var pharmMarker = L.circleMarker([p.lat, p.lng], {
                radius: 6,
                fillColor: '#dc2626',
                color: '#7f1d1d',
                weight: 2,
                fillOpacity: 0.9,
            });
            pharmMarker.bindPopup(
                '<b style="color:#dc2626;">\ud83c\udfe5 ' + p.name + '</b><br>' +
                '<span style="font-size:11px;">' + p.address + '</span><br>' +
                '<span style="font-size:11px; color:#64748b;">' + p.dist.toFixed(2) + ' km away</span>'
            );
            ruleVisualizationGroup.addLayer(pharmMarker);

            if (i < 3) {
                var line = L.polyline([[lat, lng], [p.lat, p.lng]], {
                    color: '#dc2626',
                    weight: 1.5,
                    dashArray: '6,4',
                    opacity: 0.6,
                });
                ruleVisualizationGroup.addLayer(line);

                var midLat = (lat + p.lat) / 2;
                var midLng = (lng + p.lng) / 2;
                var distLabel = L.divIcon({
                    html: '<div style="background:rgba(220,38,38,0.9); color:white; padding:1px 6px; border-radius:8px; font-size:9px; font-weight:600;">' + p.dist.toFixed(1) + ' km</div>',
                    className: '',
                    iconAnchor: [20, 8],
                });
                ruleVisualizationGroup.addLayer(L.marker([midLat, midLng], { icon: distLabel, interactive: false }));
            }

            if (i < 5) {
                poiListHtml.push('<div class="poi-item"><div class="poi-icon pharmacy">\ud83c\udfe5</div><div><b>' + p.name + '</b><br><span style="color:#64748b;">' + p.dist.toFixed(2) + ' km</span></div></div>');
            }
        });

        // Draw relevant POIs
        ruleDef.poiTypes.forEach(function(poiType) {
            var pois = POI_DATA[poiType] || [];
            pois.forEach(function(poi) {
                var poiDist = haversine(lat, lng, poi.lat, poi.lng);
                var maxShowDist = 2;
                if (ruleKey === 'Item 130') maxShowDist = 1;
                if (ruleKey === 'Item 131') maxShowDist = 15;
                if (ruleKey === 'Item 134A') maxShowDist = 100;

                if (poiDist <= maxShowDist) {
                    var poiColor, poiIconStr, poiClass, poiLabel;
                    if (poiType === 'supermarkets') {
                        poiColor = '#f59e0b'; poiIconStr = '\ud83d\uded2'; poiClass = 'supermarket';
                        poiLabel = poi.name + ' (' + Math.round(poi.floor_area) + 'sqm)';
                    } else if (poiType === 'hospitals') {
                        poiColor = '#3b82f6'; poiIconStr = '\ud83c\udfe5'; poiClass = 'hospital';
                        poiLabel = poi.name + ' (' + poi.beds + ' beds)';
                    } else if (poiType === 'shopping_centres') {
                        poiColor = '#8b5cf6'; poiIconStr = '\ud83c\udfec'; poiClass = 'shopping';
                        poiLabel = poi.name + ' (' + Math.round(poi.gla) + 'sqm GLA)';
                    } else if (poiType === 'medical_centres') {
                        poiColor = '#06b6d4'; poiIconStr = '\u2695\ufe0f'; poiClass = 'medical';
                        poiLabel = poi.name + ' (' + poi.gps + ' GPs, ' + poi.fte.toFixed(1) + ' FTE)';
                    } else if (poiType === 'gps') {
                        poiColor = '#6366f1'; poiIconStr = '\ud83d\udc68\u200d\u2695\ufe0f'; poiClass = 'gp';
                        poiLabel = poi.name + ' (' + poi.fte + ' FTE)';
                    }

                    var poiMarker = L.circleMarker([poi.lat, poi.lng], {
                        radius: 7,
                        fillColor: poiColor,
                        color: '#fff',
                        weight: 2,
                        fillOpacity: 0.9,
                    });
                    poiMarker.bindPopup(
                        '<b style="color:' + poiColor + ';">' + poiIconStr + ' ' + poiLabel + '</b><br>' +
                        '<span style="font-size:11px;">' + poi.address + '</span><br>' +
                        '<span style="font-size:11px; color:#64748b;">' + poiDist.toFixed(2) + ' km from site</span>'
                    );
                    ruleVisualizationGroup.addLayer(poiMarker);

                    var poiLine = L.polyline([[lat, lng], [poi.lat, poi.lng]], {
                        color: poiColor,
                        weight: 2,
                        dashArray: '4,4',
                        opacity: 0.7,
                    });
                    ruleVisualizationGroup.addLayer(poiLine);

                    poiListHtml.push('<div class="poi-item"><div class="poi-icon ' + poiClass + '">' + poiIconStr + '</div><div><b>' + poiLabel + '</b><br><span style="color:#64748b;">' + poiDist.toFixed(2) + ' km</span></div></div>');
                }
            });
        });

        // Build rule card HTML
        panelHtml += '<div class="rule-card">' +
            '<h4><span style="color:' + ruleDef.color + ';">\u25cf</span> ' + ruleKey + ': ' + ruleDef.name + '</h4>' +
            '<p>' + ruleDef.description + '</p>' +
            '<div class="rule-distance" style="margin-top:4px;">\ud83d\udccf ' + ruleDef.radiusLabel + '</div>' +
        '</div>';

        if (poiListHtml.length > 0) {
            panelHtml += '<div class="poi-list">' + poiListHtml.join('') + '</div>';
        }
    });

    // Evidence section
    if (opp.evidence) {
        panelHtml += '<div class="evidence-block"><strong>Evidence:</strong><br>' + opp.evidence + '</div>';
    }

    // Key metrics
    panelHtml += '<div class="evidence-block" style="margin-top:8px;">' +
        '<strong>Key Metrics:</strong><br>' +
        'Nearest Pharmacy: <b>' + opp.nearest_pharmacy_km.toFixed(2) + ' km</b> (' + opp.nearest_pharmacy + ')<br>' +
        'Population (5/10/15km): <b>' + opp.pop_5km.toLocaleString() + ' / ' + opp.pop_10km.toLocaleString() + ' / ' + opp.pop_15km.toLocaleString() + '</b><br>' +
        'Competition Score: <b>' + opp.competition_score + '</b> | Composite: <b>' + Math.round(opp.composite_score).toLocaleString() + '</b><br>' +
        'Pharmacies within 5/10/15km: <b>' + opp.pharmacy_5km + ' / ' + opp.pharmacy_10km + ' / ' + opp.pharmacy_15km + '</b>' +
    '</div>';

    document.getElementById('rulePanelBody').innerHTML = panelHtml;
    document.getElementById('rulePanel').classList.add('visible');

    // Fit map to show visualization
    if (ruleVisualizationGroup.getLayers().length > 0) {
        try {
            var bounds = ruleVisualizationGroup.getBounds();
            map.fitBounds(bounds.pad(0.1), { maxZoom: 15 });
        } catch(e) { }
    }
}

function clearRuleVisualization() {
    if (ruleVisualizationGroup) {
        ruleVisualizationGroup.clearLayers();
    }
    document.getElementById('rulePanel').classList.remove('visible');
    activeOppMarker = null;
}

function closeRulePanel() {
    clearRuleVisualization();
}

// ============================================================
// STATE FILTERS
// ============================================================
function buildStateFilters() {
    var container = document.getElementById('stateFilters');
    var stateSet = {};
    OPP_DATA.forEach(function(d) { stateSet[d.state] = (stateSet[d.state] || 0) + 1; });
    var states = Object.keys(stateSet).sort();
    states.forEach(function(state) {
        var count = stateSet[state];
        var color = STATE_COLORS[state] || '#999';
        var label = document.createElement('label');
        label.className = 'state-cb';
        label.style.setProperty('--state-color', color);
        label.innerHTML = '<input type="checkbox" class="state-cb-input" value="' + state + '" checked>' +
            '<span class="state-dot" style="background:' + color + '"></span>' +
            state + ' <span class="state-count">' + count + '</span>';
        container.appendChild(label);
    });
}

function buildRuleFilters() {
    var container = document.getElementById('ruleFilters');
    var rules = {};
    OPP_DATA.forEach(function(d) {
        d.rules.split(',').forEach(function(r) { r = r.trim(); if (r) rules[r] = true; });
    });
    Object.keys(rules).sort().forEach(function(rule) {
        var label = document.createElement('label');
        label.className = 'state-cb';
        label.innerHTML = '<input type="checkbox" class="rule-cb" value="' + rule + '" checked> <span style="font-size:11px">' + rule + '</span>';
        container.appendChild(label);
    });
}

// ============================================================
// FILTERS
// ============================================================
function applyFilters() {
    var checkedStates = {};
    document.querySelectorAll('.state-cb-input:checked').forEach(function(c) { checkedStates[c.value] = true; });
    var checkedVerifs = {};
    document.querySelectorAll('.verif-cb:checked').forEach(function(c) { checkedVerifs[c.value] = true; });
    var checkedRules = {};
    document.querySelectorAll('.rule-cb:checked').forEach(function(c) { checkedRules[c.value] = true; });
    var minPop = parseInt(document.getElementById('popSlider').value);
    var minDist = parseFloat(document.getElementById('distSlider').value);
    var maxComp = parseInt(document.getElementById('compSlider').value);
    var search = document.getElementById('searchBox').value.toLowerCase();

    filteredData = OPP_DATA.filter(function(d) {
        if (!checkedStates[d.state]) return false;
        if (!checkedVerifs[d.verification]) return false;
        var dRules = d.rules.split(',').map(function(r) { return r.trim(); });
        if (!dRules.some(function(r) { return checkedRules[r]; })) return false;
        if (d.pop_10km < minPop) return false;
        if (d.nearest_pharmacy_km < minDist) return false;
        if (d.competition_score > maxComp) return false;
        if (search && d.name.toLowerCase().indexOf(search) < 0 &&
            d.nearest_town.toLowerCase().indexOf(search) < 0 &&
            d.address.toLowerCase().indexOf(search) < 0 &&
            d.state.toLowerCase().indexOf(search) < 0 &&
            d.rules.toLowerCase().indexOf(search) < 0) return false;
        return true;
    });

    if (quickFilterActive === 'growth') {
        filteredData = filteredData.filter(function(d) { return d.growth_indicator === 'YES'; });
    }
    if (quickFilterActive === 'top50') {
        filteredData = filteredData.slice(0, 50);
    }

    updateOpportunityMarkers();
    updateTable();
    updateStats();
}

function updateOpportunityMarkers() {
    opportunityCluster.clearLayers();
    filteredData.forEach(function(d) {
        var m = createOpportunityMarker(d);
        opportunityCluster.addLayer(m);
    });
}

// ============================================================
// TABLE
// ============================================================
function updateTable() {
    var sorted = filteredData.slice().sort(function(a, b) {
        var av = a[sortCol], bv = b[sortCol];
        if (typeof av === 'string') { av = av.toLowerCase(); bv = (bv||'').toLowerCase(); }
        if (sortDir === 'asc') return av > bv ? 1 : av < bv ? -1 : 0;
        return av < bv ? 1 : av > bv ? -1 : 0;
    });

    var cols = [
        ['#', 'rank', 'num'],
        ['Name', 'name', 'text'],
        ['State', 'state', 'text'],
        ['Pop 10km', 'pop_10km', 'num'],
        ['Dist (km)', 'nearest_pharmacy_km', 'num'],
        ['Pharma 5km', 'pharmacy_5km', 'num'],
        ['Chains', 'chain_count', 'num'],
        ['Comp', 'competition_score', 'num'],
        ['Score', 'composite_score', 'num'],
        ['Rules', 'rules', 'text'],
        ['Status', 'verification', 'text'],
        ['Town', 'nearest_town', 'text'],
        ['Growth', 'growth_indicator', 'text'],
    ];

    var thead = document.getElementById('tableHead');
    thead.innerHTML = '<tr>' + cols.map(function(col) {
        var label = col[0], key = col[1];
        var cls = key === sortCol ? (sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc') : '';
        return '<th class="' + cls + '" onclick="sortTable(\'' + key + '\')">' + label + '</th>';
    }).join('') + '</tr>';

    var tbody = document.getElementById('tableBody');
    tbody.innerHTML = sorted.slice(0, 500).map(function(d, i) {
        var compClass = d.competition_score <= 5 ? 'comp-low' : d.competition_score <= 30 ? 'comp-med' : 'comp-high';
        var verifClass = d.verification === 'VERIFIED' ? 'badge-verified' :
            d.verification === 'FALSE POSITIVE' ? 'badge-false' :
            d.verification === 'NEEDS REVIEW' ? 'badge-review' : 'badge-unverified';
        var growth = d.growth_indicator === 'YES' ? ' <span class="badge badge-growth">\ud83d\udcc8</span>' : '';
        return '<tr onclick="flyToOpp(' + d.lat + ',' + d.lng + ')" style="cursor:pointer">' +
            '<td>' + (i + 1) + '</td>' +
            '<td class="td-name">' + d.name + growth + '</td>' +
            '<td><span class="state-dot" style="background:' + (STATE_COLORS[d.state] || '#999') + '"></span> ' + d.state + '</td>' +
            '<td>' + d.pop_10km.toLocaleString() + '</td>' +
            '<td>' + d.nearest_pharmacy_km.toFixed(1) + '</td>' +
            '<td>' + d.pharmacy_5km + '</td>' +
            '<td>' + d.chain_count + '</td>' +
            '<td class="td-score ' + compClass + '">' + d.competition_score + '</td>' +
            '<td class="td-score">' + Math.round(d.composite_score).toLocaleString() + '</td>' +
            '<td style="font-size:10px">' + d.rules + '</td>' +
            '<td><span class="badge ' + verifClass + '">' + d.verification.substring(0,8) + '</span></td>' +
            '<td>' + d.nearest_town + '</td>' +
            '<td>' + (d.growth_indicator === 'YES' ? '\ud83d\udcc8' : '') + '</td>' +
        '</tr>';
    }).join('');

    var icon = tableOpen ? '\u25BC' : '\u25B2';
    document.getElementById('tableToggle').textContent =
        icon + ' ' + (tableOpen ? 'Hide' : 'Show') + ' Table (' + filteredData.length + ' results)';
}

function sortTable(col) {
    if (sortCol === col) { sortDir = sortDir === 'asc' ? 'desc' : 'asc'; }
    else { sortCol = col; sortDir = 'desc'; }
    updateTable();
}

function flyToOpp(lat, lng) {
    map.flyTo([lat, lng], 13, { duration: 0.8 });
    setTimeout(function() {
        opportunityCluster.eachLayer(function(m) {
            var ll = m.getLatLng();
            if (Math.abs(ll.lat - lat) < 0.001 && Math.abs(ll.lng - lng) < 0.001) {
                m.openPopup();
            }
        });
    }, 900);
}

function toggleTable() {
    tableOpen = !tableOpen;
    document.getElementById('tablePanel').classList.toggle('open', tableOpen);
    var icon = tableOpen ? '\u25BC' : '\u25B2';
    document.getElementById('tableToggle').textContent =
        icon + ' ' + (tableOpen ? 'Hide' : 'Show') + ' Table (' + filteredData.length + ' results)';
    setTimeout(function() { map.invalidateSize(); }, 350);
}

function updateStats() {
    document.getElementById('stat-pharmacies').textContent = PHARM_DATA.length.toLocaleString();
    document.getElementById('stat-total').textContent = OPP_DATA.length.toLocaleString();
    document.getElementById('stat-showing').textContent = filteredData.length.toLocaleString();
    var stateSet = {};
    filteredData.forEach(function(d) { stateSet[d.state] = true; });
    document.getElementById('stat-states').textContent = Object.keys(stateSet).length;
    var growthCount = filteredData.filter(function(d) { return d.growth_indicator === 'YES'; }).length;
    document.getElementById('stat-growth').textContent = growthCount;

    var panel = document.getElementById('summaryPanel');
    if (filteredData.length === 0) { panel.innerHTML = '<em>No results match filters.</em>'; return; }
    var avgPop = Math.round(filteredData.reduce(function(s, d) { return s + d.pop_10km; }, 0) / filteredData.length);
    var avgComp = (filteredData.reduce(function(s, d) { return s + d.competition_score; }, 0) / filteredData.length).toFixed(1);
    var avgDist = (filteredData.reduce(function(s, d) { return s + d.nearest_pharmacy_km; }, 0) / filteredData.length).toFixed(1);
    var verified = filteredData.filter(function(d) { return d.verification === 'VERIFIED'; }).length;
    var topScore = Math.round(filteredData[0] ? filteredData[0].composite_score : 0);
    panel.innerHTML =
        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:4px">' +
            '<div>Avg pop (10km): <b>' + avgPop.toLocaleString() + '</b></div>' +
            '<div>Avg competition: <b>' + avgComp + '</b></div>' +
            '<div>Avg distance: <b>' + avgDist + ' km</b></div>' +
            '<div>Verified: <b>' + verified + '</b></div>' +
            '<div>Growth areas: <b>' + growthCount + '</b></div>' +
            '<div>Top score: <b>' + topScore.toLocaleString() + '</b></div>' +
        '</div>';
}

// ============================================================
// QUICK FILTERS
// ============================================================
function applyQuickFilter(type) {
    document.querySelectorAll('.quick-btn').forEach(function(b) { b.classList.remove('active'); });
    if (event && event.target) event.target.classList.add('active');
    quickFilterActive = null;

    if (type === 'reset') {
        document.querySelectorAll('.state-cb-input, .rule-cb').forEach(function(c) { c.checked = true; });
        document.querySelectorAll('.verif-cb').forEach(function(c) {
            c.checked = c.value !== 'FALSE POSITIVE';
        });
        document.getElementById('popSlider').value = 0;
        document.getElementById('distSlider').value = 0;
        document.getElementById('compSlider').value = 200;
        document.getElementById('searchBox').value = '';
        updateSliderLabels();
        applyFilters();
        return;
    }
    if (type === 'rural') {
        document.getElementById('distSlider').value = 10;
        updateSliderLabels();
    }
    if (type === 'verified') {
        document.querySelectorAll('.verif-cb').forEach(function(c) {
            c.checked = c.value === 'VERIFIED';
        });
    }
    if (type === 'low-comp') {
        document.getElementById('compSlider').value = 10;
        updateSliderLabels();
    }
    if (type === 'high-pop') {
        document.getElementById('popSlider').value = 10000;
        updateSliderLabels();
    }
    if (type === 'growth') {
        quickFilterActive = 'growth';
    }
    if (type === 'top50') {
        quickFilterActive = 'top50';
    }
    applyFilters();
}

function updateSliderLabels() {
    document.getElementById('popVal').textContent = parseInt(document.getElementById('popSlider').value).toLocaleString();
    document.getElementById('distVal').textContent = document.getElementById('distSlider').value;
    document.getElementById('compVal').textContent = document.getElementById('compSlider').value;
}

// ============================================================
// EXPORT
// ============================================================
function exportCSV() {
    if (filteredData.length === 0) { alert('No data to export'); return; }
    var cols = ['name','state','nearest_town','lat','lng','pop_5km','pop_10km','pop_15km',
        'nearest_pharmacy_km','nearest_pharmacy','pharmacy_5km','pharmacy_10km','pharmacy_15km',
        'chain_count','independent_count','competition_score','composite_score',
        'rules','verification','confidence','growth_indicator','growth_details'];
    var header = cols.join(',');
    var rows = filteredData.map(function(d) {
        return cols.map(function(c) {
            var v = d[c]; if (v == null) v = '';
            v = String(v);
            if (v.indexOf(',') >= 0 || v.indexOf('"') >= 0 || v.indexOf('\n') >= 0) v = '"' + v.replace(/"/g, '""') + '"';
            return v;
        }).join(',');
    });
    var csv = header + '\n' + rows.join('\n');
    var blob = new Blob([csv], {type: 'text/csv'});
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'pharmacy_opportunities_filtered_' + new Date().toISOString().slice(0,10) + '.csv';
    a.click();
}

// ============================================================
// LAYER TOGGLES
// ============================================================
function bindLayerToggles() {
    document.getElementById('togglePharmacies').addEventListener('change', function() {
        if (this.checked) { map.addLayer(pharmacyCluster); }
        else { map.removeLayer(pharmacyCluster); }
    });
    document.getElementById('toggleOpportunities').addEventListener('change', function() {
        if (this.checked) { map.addLayer(opportunityCluster); }
        else { map.removeLayer(opportunityCluster); }
    });
}

// ============================================================
// EVENT LISTENERS
// ============================================================
function bindEvents() {
    document.querySelectorAll('.state-cb-input, .verif-cb, .rule-cb').forEach(function(cb) {
        cb.addEventListener('change', function() {
            quickFilterActive = null;
            document.querySelectorAll('.quick-btn').forEach(function(b) { b.classList.remove('active'); });
            applyFilters();
        });
    });
    ['popSlider', 'distSlider', 'compSlider'].forEach(function(id) {
        document.getElementById(id).addEventListener('input', function() {
            updateSliderLabels();
            applyFilters();
        });
    });
    document.getElementById('searchBox').addEventListener('input', applyFilters);
}

// ============================================================
// BOOT
// ============================================================
window.addEventListener('load', function() {
    initMap();
    loadPharmacyMarkers();
    buildStateFilters();
    buildRuleFilters();
    bindEvents();
    bindLayerToggles();
    document.querySelectorAll('.verif-cb').forEach(function(c) {
        if (c.value === 'FALSE POSITIVE') c.checked = false;
    });
    applyFilters();
    document.getElementById('loading').classList.add('hidden');
});
</script>
</body>
</html>'''


def main():
    print("=" * 60)
    print("BUILDING PHARMACY FINDER DASHBOARD v2")
    print("=" * 60)

    print("\nLoading opportunities from population_ranked CSVs...")
    opportunities = load_opportunities()

    print("\nLoading existing pharmacies from database...")
    pharmacies = load_pharmacies()

    print("\nLoading POI data from database...")
    pois = load_pois()

    print(f"\nGenerating dashboard HTML...")
    html_content = generate_html(opportunities, pharmacies, pois)

    output_path = os.path.join(OUTPUT_DIR, 'dashboard.html')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    size_kb = os.path.getsize(output_path) / 1024
    size_mb = size_kb / 1024
    print(f"\nDashboard written to: {output_path}")
    print(f"File size: {size_kb:.0f} KB ({size_mb:.1f} MB)")
    print(f"Total opportunities: {len(opportunities)}")
    print(f"Total pharmacies: {len(pharmacies)}")
    print(f"POIs: {sum(len(v) for v in pois.values())}")
    print("=" * 60)


if __name__ == '__main__':
    main()
