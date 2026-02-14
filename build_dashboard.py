#!/usr/bin/env python3
"""
Build an interactive web dashboard for PharmacyFinder opportunities.

Generates a self-contained HTML file with:
- Leaflet.js map with color-coded markers by state
- Sidebar filters (state, population, distance, competition)
- Sortable table of all opportunities
- Competition data overlay
- Growth corridor indicators
"""

import csv
import json
import os
import html

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

STATE_COLORS = {
    'ACT': '#e74c3c',
    'NSW': '#3498db',
    'NT': '#e67e22',
    'QLD': '#9b59b6',
    'SA': '#1abc9c',
    'TAS': '#2ecc71',
    'VIC': '#34495e',
    'WA': '#f39c12',
}


def load_all_data():
    """Load population_ranked CSVs from all states."""
    all_opportunities = []

    for state in STATES:
        filepath = os.path.join(OUTPUT_DIR, f'population_ranked_{state}.csv')
        if not os.path.exists(filepath):
            print(f"  Warning: {filepath} not found, skipping {state}")
            continue

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    opp = {
                        'lat': float(row.get('Latitude', 0)),
                        'lng': float(row.get('Longitude', 0)),
                        'name': row.get('POI Name', 'Unknown'),
                        'address': row.get('Address', ''),
                        'rules': row.get('Qualifying Rules', ''),
                        'evidence': row.get('Evidence', '')[:200],  # Truncate
                        'confidence': row.get('Confidence', '0%'),
                        'nearest_pharmacy_km': float(row.get('Nearest Pharmacy (km)', 0) or 0),
                        'nearest_pharmacy': row.get('Nearest Pharmacy Name', ''),
                        'poi_type': row.get('POI Type', ''),
                        'state': row.get('Region', state),
                        'verification': row.get('Verification', 'UNVERIFIED'),
                        'pop_5km': int(float(row.get('Pop 5km', 0) or 0)),
                        'pop_10km': int(float(row.get('Pop 10km', 0) or 0)),
                        'pop_15km': int(float(row.get('Pop 15km', 0) or 0)),
                        'nearest_town': row.get('Nearest Town', ''),
                        'opp_score': float(row.get('Opportunity Score', 0) or 0),
                        'pharmacy_5km': int(float(row.get('Pharmacies 5km', 0) or row.get('Pharmacy Count 5km', 0) or 0)),
                        'pharmacy_10km': int(float(row.get('Pharmacies 10km', 0) or row.get('Pharmacy Count 10km', 0) or 0)),
                        'pharmacy_15km': int(float(row.get('Pharmacies 15km', 0) or row.get('Pharmacy Count 15km', 0) or 0)),
                        'chain_count': int(float(row.get('Chains 5km', 0) or row.get('Chain Count', 0) or 0)),
                        'independent_count': int(float(row.get('Independents 5km', 0) or row.get('Independent Count', 0) or 0)),
                        'chain_names': row.get('Chain Names 5km', ''),
                        'competition_score': float(row.get('Competition Score', 0) or 0),
                        'composite_score': float(row.get('Composite Score', 0) or row.get('Opportunity Score', 0) or 0),
                        'nearest_competitors': row.get('Nearest Competitors', '') or row.get('Chain Names 10km', ''),
                        'growth_indicator': row.get('Growth Corridor', '') or row.get('Growth Indicator', ''),
                        'growth_details': row.get('Growth Notes', '') or row.get('Growth Details', ''),
                        'growth_rating': int(float(row.get('Growth Rating', 0) or 0)),
                        'growth_type': row.get('Growth Type', ''),
                    }
                    all_opportunities.append(opp)
                except (ValueError, TypeError) as e:
                    continue

    # Sort by composite score descending
    all_opportunities.sort(key=lambda x: x['composite_score'], reverse=True)

    print(f"  Loaded {len(all_opportunities)} total opportunities")
    for state in STATES:
        count = sum(1 for o in all_opportunities if o['state'] == state)
        if count > 0:
            print(f"    {state}: {count}")

    return all_opportunities


def generate_html(opportunities):
    """Generate the dashboard HTML."""
    data_json = json.dumps(opportunities, ensure_ascii=False)

    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PharmacyFinder - Greenfield Opportunity Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, sans-serif; background: #f0f2f5; color: #1a1a2e; }}

/* Header */
.header {{
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    color: white; padding: 16px 24px; display: flex; align-items: center;
    justify-content: space-between; box-shadow: 0 2px 10px rgba(0,0,0,0.3);
    position: fixed; top: 0; left: 0; right: 0; z-index: 1000; height: 64px;
}}
.header h1 {{ font-size: 20px; font-weight: 700; letter-spacing: -0.5px; }}
.header h1 span {{ color: #53c587; }}
.header .stats {{ font-size: 13px; color: #94a3b8; display: flex; gap: 20px; }}
.header .stats .stat-val {{ color: #53c587; font-weight: 600; }}

/* Layout */
.main {{ display: flex; margin-top: 64px; height: calc(100vh - 64px); }}

/* Sidebar */
.sidebar {{
    width: 300px; min-width: 300px; background: white; padding: 16px;
    overflow-y: auto; border-right: 1px solid #e2e8f0;
    box-shadow: 2px 0 10px rgba(0,0,0,0.05);
}}
.sidebar h3 {{ font-size: 13px; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px; margin-bottom: 10px; font-weight: 600; }}
.filter-section {{ margin-bottom: 20px; padding-bottom: 16px; border-bottom: 1px solid #f1f5f9; }}
.filter-section:last-child {{ border-bottom: none; }}

/* State checkboxes */
.state-filters {{ display: grid; grid-template-columns: 1fr 1fr; gap: 4px; }}
.state-cb {{ display: flex; align-items: center; gap: 6px; padding: 4px 8px; border-radius: 6px; cursor: pointer; transition: background 0.15s; font-size: 13px; }}
.state-cb:hover {{ background: #f8fafc; }}
.state-cb input {{ accent-color: var(--state-color); }}
.state-dot {{ width: 10px; height: 10px; border-radius: 50%; display: inline-block; }}
.state-count {{ color: #94a3b8; font-size: 11px; margin-left: auto; }}

/* Sliders */
.slider-group {{ margin-bottom: 12px; }}
.slider-label {{ display: flex; justify-content: space-between; font-size: 12px; color: #475569; margin-bottom: 4px; }}
.slider-val {{ font-weight: 600; color: #1a1a2e; }}
input[type="range"] {{ width: 100%; height: 6px; -webkit-appearance: none; background: #e2e8f0; border-radius: 3px; outline: none; }}
input[type="range"]::-webkit-slider-thumb {{ -webkit-appearance: none; width: 16px; height: 16px; border-radius: 50%; background: #0f3460; cursor: pointer; }}

/* Quick filters */
.quick-filters {{ display: flex; flex-wrap: wrap; gap: 4px; }}
.quick-btn {{ padding: 4px 10px; border: 1px solid #e2e8f0; border-radius: 16px; font-size: 11px; cursor: pointer; background: white; color: #475569; transition: all 0.15s; }}
.quick-btn:hover, .quick-btn.active {{ background: #0f3460; color: white; border-color: #0f3460; }}

/* Verification badges */
.badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 10px; font-weight: 600; text-transform: uppercase; }}
.badge-verified {{ background: #d1fae5; color: #065f46; }}
.badge-false {{ background: #fee2e2; color: #991b1b; }}
.badge-review {{ background: #fef3c7; color: #92400e; }}
.badge-unverified {{ background: #f1f5f9; color: #64748b; }}
.badge-growth {{ background: #dbeafe; color: #1e40af; }}

/* Map */
.map-container {{ flex: 1; position: relative; }}
#map {{ width: 100%; height: 100%; }}

/* Table panel */
.table-panel {{
    position: fixed; bottom: 0; left: 300px; right: 0; height: 0;
    background: white; box-shadow: 0 -4px 20px rgba(0,0,0,0.15);
    transition: height 0.3s ease; z-index: 900; overflow: hidden;
    border-top: 3px solid #0f3460;
}}
.table-panel.open {{ height: 45vh; }}
.table-toggle {{
    position: absolute; top: -36px; left: 50%; transform: translateX(-50%);
    background: #0f3460; color: white; border: none; padding: 8px 20px;
    border-radius: 8px 8px 0 0; cursor: pointer; font-size: 13px; font-weight: 600;
    box-shadow: 0 -2px 10px rgba(0,0,0,0.15);
}}
.table-toggle:hover {{ background: #16213e; }}
.table-wrap {{ height: 100%; overflow: auto; }}
table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
thead {{ position: sticky; top: 0; z-index: 10; }}
th {{ background: #1a1a2e; color: white; padding: 8px 10px; text-align: left; cursor: pointer; white-space: nowrap; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.3px; }}
th:hover {{ background: #0f3460; }}
th.sorted-asc::after {{ content: " \\25B2"; font-size: 9px; }}
th.sorted-desc::after {{ content: " \\25BC"; font-size: 9px; }}
td {{ padding: 7px 10px; border-bottom: 1px solid #f1f5f9; white-space: nowrap; }}
tr:hover td {{ background: #f8fafc; }}
tr.highlight td {{ background: #eff6ff; }}
.td-name {{ max-width: 200px; overflow: hidden; text-overflow: ellipsis; font-weight: 500; }}
.td-score {{ font-weight: 700; }}
.comp-low {{ color: #059669; }}
.comp-med {{ color: #d97706; }}
.comp-high {{ color: #dc2626; }}

/* Popup */
.leaflet-popup-content-wrapper {{ border-radius: 10px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); }}
.leaflet-popup-content {{ margin: 12px; font-size: 13px; line-height: 1.5; min-width: 280px; }}
.popup-title {{ font-size: 15px; font-weight: 700; color: #1a1a2e; margin-bottom: 8px; border-bottom: 2px solid #53c587; padding-bottom: 6px; }}
.popup-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 4px 12px; }}
.popup-label {{ color: #64748b; font-size: 11px; }}
.popup-value {{ font-weight: 600; font-size: 13px; }}
.popup-section {{ margin-top: 8px; padding-top: 8px; border-top: 1px solid #f1f5f9; }}

/* Responsive */
@media (max-width: 768px) {{
    .sidebar {{ display: none; }}
    .table-panel {{ left: 0; }}
}}

/* Loading */
.loading {{ position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(26,26,46,0.9); z-index: 9999; display: flex; align-items: center; justify-content: center; }}
.loading-text {{ color: white; font-size: 18px; }}
.loading.hidden {{ display: none; }}

/* Search */
.search-box {{ width: 100%; padding: 8px 12px; border: 1px solid #e2e8f0; border-radius: 8px; font-size: 13px; outline: none; margin-bottom: 12px; }}
.search-box:focus {{ border-color: #0f3460; box-shadow: 0 0 0 3px rgba(15,52,96,0.1); }}
</style>
</head>
<body>

<div class="loading" id="loading">
    <div class="loading-text">Loading PharmacyFinder Dashboard...</div>
</div>

<div class="header">
    <h1><span>Pharmacy</span>Finder Dashboard</h1>
    <div class="stats">
        <div>Opportunities: <span class="stat-val" id="stat-total">0</span></div>
        <div>Showing: <span class="stat-val" id="stat-showing">0</span></div>
        <div>States: <span class="stat-val" id="stat-states">8</span></div>
    </div>
</div>

<div class="main">
    <div class="sidebar">
        <input type="text" class="search-box" id="searchBox" placeholder="Search locations..." />

        <div class="filter-section">
            <h3>States</h3>
            <div class="state-filters" id="stateFilters"></div>
        </div>

        <div class="filter-section">
            <h3>Quick Filters</h3>
            <div class="quick-filters">
                <button class="quick-btn" onclick="applyQuickFilter('rural')">Rural Only</button>
                <button class="quick-btn" onclick="applyQuickFilter('verified')">Verified</button>
                <button class="quick-btn" onclick="applyQuickFilter('low-comp')">Low Competition</button>
                <button class="quick-btn" onclick="applyQuickFilter('high-pop')">High Population</button>
                <button class="quick-btn" onclick="applyQuickFilter('growth')">Growth Areas</button>
                <button class="quick-btn" onclick="applyQuickFilter('reset')">Reset All</button>
            </div>
        </div>

        <div class="filter-section">
            <h3>Population (10km)</h3>
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
                <div class="slider-label"><span>Maximum</span><span class="slider-val" id="compVal">999</span></div>
                <input type="range" id="compSlider" min="0" max="200" step="1" value="200" />
            </div>
        </div>

        <div class="filter-section">
            <h3>Verification Status</h3>
            <div style="display: flex; flex-direction: column; gap: 4px;">
                <label class="state-cb"><input type="checkbox" class="verif-cb" value="VERIFIED" checked> <span class="badge badge-verified">Verified</span></label>
                <label class="state-cb"><input type="checkbox" class="verif-cb" value="FALSE POSITIVE" checked> <span class="badge badge-false">False Positive</span></label>
                <label class="state-cb"><input type="checkbox" class="verif-cb" value="NEEDS REVIEW" checked> <span class="badge badge-review">Needs Review</span></label>
                <label class="state-cb"><input type="checkbox" class="verif-cb" value="UNVERIFIED" checked> <span class="badge badge-unverified">Unverified</span></label>
            </div>
        </div>

        <div class="filter-section">
            <h3>Qualifying Rules</h3>
            <div id="ruleFilters" style="display: flex; flex-direction: column; gap: 4px;"></div>
        </div>

        <div class="filter-section">
            <h3>Summary</h3>
            <div id="summaryPanel" style="font-size: 12px; color: #475569;"></div>
        </div>
    </div>

    <div class="map-container">
        <div id="map"></div>
        <div class="table-panel" id="tablePanel">
            <button class="table-toggle" id="tableToggle" onclick="toggleTable()">Show Table (0 results)</button>
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
// Embedded data
const ALL_DATA = {data_json};

const STATE_COLORS = {{
    'ACT': '#e74c3c', 'NSW': '#3498db', 'NT': '#e67e22', 'QLD': '#9b59b6',
    'SA': '#1abc9c', 'TAS': '#2ecc71', 'VIC': '#34495e', 'WA': '#f39c12'
}};

// State
let map, markers = [], filteredData = [], tableOpen = false;
let sortCol = 'composite_score', sortDir = 'desc';

// Init map
function initMap() {{
    map = L.map('map', {{ zoomControl: true }}).setView([-25.5, 134], 5);
    L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}@2x.png', {{
        attribution: '&copy; OSM contributors &copy; CARTO',
        maxZoom: 19
    }}).addTo(map);
}}

// Create circle marker
function createMarker(d) {{
    const color = STATE_COLORS[d.state] || '#999';
    const radius = Math.min(Math.max(Math.sqrt(d.pop_10km) / 15, 4), 14);

    const marker = L.circleMarker([d.lat, d.lng], {{
        radius: radius,
        fillColor: color,
        color: '#fff',
        weight: 1.5,
        opacity: 1,
        fillOpacity: 0.8
    }});

    const verifBadge = d.verification === 'VERIFIED' ? '<span class="badge badge-verified">Verified</span>' :
        d.verification === 'FALSE POSITIVE' ? '<span class="badge badge-false">False Positive</span>' :
        d.verification === 'NEEDS REVIEW' ? '<span class="badge badge-review">Needs Review</span>' :
        '<span class="badge badge-unverified">Unverified</span>';

    const growthHtml = d.growth_indicator ? `<div class="popup-section"><span class="badge badge-growth">${{d.growth_type || 'Growth'}} (${{d.growth_rating}}/10)</span> ${{d.growth_indicator}}<br><span style="font-size:11px;color:#64748b">${{d.growth_details || ''}}</span></div>` : '';

    const competitorsHtml = d.nearest_competitors ?
        `<div class="popup-section"><div class="popup-label">Nearest Competitors</div><div style="font-size:11px">${{d.nearest_competitors}}</div></div>` : '';

    const compClass = d.competition_score <= 5 ? 'comp-low' : d.competition_score <= 30 ? 'comp-med' : 'comp-high';

    marker.bindPopup(`
        <div class="popup-title">${{d.name}}</div>
        <div>${{verifBadge}} ${{d.rules}}</div>
        <div class="popup-grid popup-section">
            <div><div class="popup-label">Population 5km</div><div class="popup-value">${{d.pop_5km.toLocaleString()}}</div></div>
            <div><div class="popup-label">Population 10km</div><div class="popup-value">${{d.pop_10km.toLocaleString()}}</div></div>
            <div><div class="popup-label">Population 15km</div><div class="popup-value">${{d.pop_15km.toLocaleString()}}</div></div>
            <div><div class="popup-label">Nearest Town</div><div class="popup-value">${{d.nearest_town}}</div></div>
        </div>
        <div class="popup-grid popup-section">
            <div><div class="popup-label">Nearest Pharmacy</div><div class="popup-value">${{d.nearest_pharmacy_km.toFixed(1)}} km</div></div>
            <div><div class="popup-label">Pharmacy Name</div><div class="popup-value">${{d.nearest_pharmacy}}</div></div>
            <div><div class="popup-label">Pharmacies in 5km</div><div class="popup-value">${{d.pharmacy_5km}}</div></div>
            <div><div class="popup-label">Pharmacies in 10km</div><div class="popup-value">${{d.pharmacy_10km}}</div></div>
        </div>
        <div class="popup-grid popup-section">
            <div><div class="popup-label">Chains (15km)</div><div class="popup-value">${{d.chain_count}}</div></div>
            <div><div class="popup-label">Independent (15km)</div><div class="popup-value">${{d.independent_count}}</div></div>
            <div><div class="popup-label">Competition Score</div><div class="popup-value ${{compClass}}">${{d.competition_score}}</div></div>
            <div><div class="popup-label">Composite Score</div><div class="popup-value">${{d.composite_score.toLocaleString()}}</div></div>
        </div>
        <div class="popup-section" style="font-size:11px;color:#64748b">${{d.evidence}}</div>
        ${{competitorsHtml}}
        ${{growthHtml}}
    `, {{ maxWidth: 340 }});

    return marker;
}}

// Build state filter checkboxes
function buildStateFilters() {{
    const container = document.getElementById('stateFilters');
    const states = [...new Set(ALL_DATA.map(d => d.state))].sort();
    states.forEach(state => {{
        const count = ALL_DATA.filter(d => d.state === state).length;
        const color = STATE_COLORS[state] || '#999';
        const label = document.createElement('label');
        label.className = 'state-cb';
        label.style.setProperty('--state-color', color);
        label.innerHTML = `<input type="checkbox" class="state-cb-input" value="${{state}}" checked>
            <span class="state-dot" style="background:${{color}}"></span>
            ${{state}} <span class="state-count">${{count}}</span>`;
        container.appendChild(label);
    }});
}}

// Build rule filter checkboxes
function buildRuleFilters() {{
    const container = document.getElementById('ruleFilters');
    const rules = new Set();
    ALL_DATA.forEach(d => {{
        d.rules.split(', ').forEach(r => {{ if(r.trim()) rules.add(r.trim()); }});
    }});
    [...rules].sort().forEach(rule => {{
        const label = document.createElement('label');
        label.className = 'state-cb';
        label.innerHTML = `<input type="checkbox" class="rule-cb" value="${{rule}}" checked> ${{rule}}`;
        container.appendChild(label);
    }});
}}

// Apply filters
function applyFilters() {{
    const checkedStates = new Set([...document.querySelectorAll('.state-cb-input:checked')].map(c => c.value));
    const checkedVerifs = new Set([...document.querySelectorAll('.verif-cb:checked')].map(c => c.value));
    const checkedRules = new Set([...document.querySelectorAll('.rule-cb:checked')].map(c => c.value));
    const minPop = parseInt(document.getElementById('popSlider').value);
    const minDist = parseFloat(document.getElementById('distSlider').value);
    const maxComp = parseInt(document.getElementById('compSlider').value);
    const search = document.getElementById('searchBox').value.toLowerCase();

    filteredData = ALL_DATA.filter(d => {{
        if (!checkedStates.has(d.state)) return false;
        if (!checkedVerifs.has(d.verification)) return false;
        const dRules = d.rules.split(', ').map(r => r.trim());
        if (!dRules.some(r => checkedRules.has(r))) return false;
        if (d.pop_10km < minPop) return false;
        if (d.nearest_pharmacy_km < minDist) return false;
        if (d.competition_score > maxComp) return false;
        if (search && !d.name.toLowerCase().includes(search) &&
            !d.nearest_town.toLowerCase().includes(search) &&
            !d.address.toLowerCase().includes(search) &&
            !d.state.toLowerCase().includes(search)) return false;
        return true;
    }});

    updateMap();
    updateTable();
    updateStats();
}}

// Update map markers
function updateMap() {{
    markers.forEach(m => map.removeLayer(m));
    markers = [];
    filteredData.forEach(d => {{
        const m = createMarker(d);
        m.addTo(map);
        markers.push(m);
    }});
}}

// Update table
function updateTable() {{
    const sorted = [...filteredData].sort((a, b) => {{
        let av = a[sortCol], bv = b[sortCol];
        if (typeof av === 'string') {{ av = av.toLowerCase(); bv = bv.toLowerCase(); }}
        if (sortDir === 'asc') return av > bv ? 1 : av < bv ? -1 : 0;
        return av < bv ? 1 : av > bv ? -1 : 0;
    }});

    const cols = [
        ['#', 'rank', 'num'],
        ['Name', 'name', 'text'],
        ['State', 'state', 'text'],
        ['Pop 10km', 'pop_10km', 'num'],
        ['Dist (km)', 'nearest_pharmacy_km', 'num'],
        ['Pharmacies 5km', 'pharmacy_5km', 'num'],
        ['Chains', 'chain_count', 'num'],
        ['Comp Score', 'competition_score', 'num'],
        ['Composite', 'composite_score', 'num'],
        ['Rules', 'rules', 'text'],
        ['Verification', 'verification', 'text'],
        ['Confidence', 'confidence', 'text'],
        ['Town', 'nearest_town', 'text'],
    ];

    // Header
    const thead = document.getElementById('tableHead');
    thead.innerHTML = '<tr>' + cols.map(([label, key]) => {{
        let cls = key === sortCol ? (sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc') : '';
        return `<th class="${{cls}}" onclick="sortTable('${{key}}')">${{label}}</th>`;
    }}).join('') + '</tr>';

    // Body
    const tbody = document.getElementById('tableBody');
    tbody.innerHTML = sorted.slice(0, 500).map((d, i) => {{
        const compClass = d.competition_score <= 5 ? 'comp-low' : d.competition_score <= 30 ? 'comp-med' : 'comp-high';
        const verifClass = d.verification === 'VERIFIED' ? 'badge-verified' :
            d.verification === 'FALSE POSITIVE' ? 'badge-false' :
            d.verification === 'NEEDS REVIEW' ? 'badge-review' : 'badge-unverified';
        const growth = d.growth_indicator ? ' <span class="badge badge-growth">Growth</span>' : '';
        return `<tr onclick="flyTo(${{d.lat}},${{d.lng}})" style="cursor:pointer">
            <td>${{i + 1}}</td>
            <td class="td-name">${{d.name}}${{growth}}</td>
            <td><span class="state-dot" style="background:${{STATE_COLORS[d.state]}}"></span> ${{d.state}}</td>
            <td>${{d.pop_10km.toLocaleString()}}</td>
            <td>${{d.nearest_pharmacy_km.toFixed(1)}}</td>
            <td>${{d.pharmacy_5km}}</td>
            <td>${{d.chain_count}}</td>
            <td class="td-score ${{compClass}}">${{d.competition_score}}</td>
            <td class="td-score">${{d.composite_score.toLocaleString()}}</td>
            <td>${{d.rules}}</td>
            <td><span class="badge ${{verifClass}}">${{d.verification}}</span></td>
            <td>${{d.confidence}}</td>
            <td>${{d.nearest_town}}</td>
        </tr>`;
    }}).join('');

    document.getElementById('tableToggle').textContent =
        (tableOpen ? 'Hide' : 'Show') + ` Table (${{filteredData.length}} results)`;
}}

function sortTable(col) {{
    if (sortCol === col) {{ sortDir = sortDir === 'asc' ? 'desc' : 'asc'; }}
    else {{ sortCol = col; sortDir = 'desc'; }}
    updateTable();
}}

function flyTo(lat, lng) {{
    map.flyTo([lat, lng], 13, {{ duration: 0.8 }});
}}

function toggleTable() {{
    tableOpen = !tableOpen;
    document.getElementById('tablePanel').classList.toggle('open', tableOpen);
    document.getElementById('tableToggle').textContent =
        (tableOpen ? 'Hide' : 'Show') + ` Table (${{filteredData.length}} results)`;
}}

function updateStats() {{
    document.getElementById('stat-total').textContent = ALL_DATA.length;
    document.getElementById('stat-showing').textContent = filteredData.length;
    const states = new Set(filteredData.map(d => d.state));
    document.getElementById('stat-states').textContent = states.size;

    // Summary panel
    const panel = document.getElementById('summaryPanel');
    if (filteredData.length === 0) {{ panel.innerHTML = 'No results match filters.'; return; }}
    const avgPop = Math.round(filteredData.reduce((s, d) => s + d.pop_10km, 0) / filteredData.length);
    const avgComp = (filteredData.reduce((s, d) => s + d.competition_score, 0) / filteredData.length).toFixed(1);
    const avgDist = (filteredData.reduce((s, d) => s + d.nearest_pharmacy_km, 0) / filteredData.length).toFixed(1);
    const verified = filteredData.filter(d => d.verification === 'VERIFIED').length;
    const growth = filteredData.filter(d => d.growth_indicator).length;
    panel.innerHTML = `
        <div>Avg pop (10km): <b>${{avgPop.toLocaleString()}}</b></div>
        <div>Avg competition: <b>${{avgComp}}</b></div>
        <div>Avg distance: <b>${{avgDist}} km</b></div>
        <div>Verified: <b>${{verified}}</b> / ${{filteredData.length}}</div>
        ${{growth > 0 ? `<div>Growth areas: <b>${{growth}}</b></div>` : ''}}
    `;
}}

// Quick filters
function applyQuickFilter(type) {{
    // Reset
    document.querySelectorAll('.quick-btn').forEach(b => b.classList.remove('active'));
    event.target.classList.add('active');

    if (type === 'reset') {{
        document.querySelectorAll('.state-cb-input, .verif-cb, .rule-cb').forEach(c => c.checked = true);
        document.getElementById('popSlider').value = 0;
        document.getElementById('distSlider').value = 0;
        document.getElementById('compSlider').value = 200;
        document.getElementById('searchBox').value = '';
        updateSliderLabels();
        applyFilters();
        return;
    }}
    if (type === 'rural') {{
        document.getElementById('distSlider').value = 10;
        updateSliderLabels();
    }}
    if (type === 'verified') {{
        document.querySelectorAll('.verif-cb').forEach(c => {{
            c.checked = c.value === 'VERIFIED';
        }});
    }}
    if (type === 'low-comp') {{
        document.getElementById('compSlider').value = 10;
        updateSliderLabels();
    }}
    if (type === 'high-pop') {{
        document.getElementById('popSlider').value = 10000;
        updateSliderLabels();
    }}
    if (type === 'growth') {{
        // Will filter in applyFilters if growth data available
        document.getElementById('searchBox').value = '';
        // For now just filter to show all and highlight
    }}
    applyFilters();
}}

function updateSliderLabels() {{
    document.getElementById('popVal').textContent = parseInt(document.getElementById('popSlider').value).toLocaleString();
    document.getElementById('distVal').textContent = document.getElementById('distSlider').value;
    document.getElementById('compVal').textContent = document.getElementById('compSlider').value;
}}

// Event listeners
function bindEvents() {{
    document.querySelectorAll('.state-cb-input, .verif-cb, .rule-cb').forEach(cb => {{
        cb.addEventListener('change', applyFilters);
    }});
    ['popSlider', 'distSlider', 'compSlider'].forEach(id => {{
        document.getElementById(id).addEventListener('input', () => {{
            updateSliderLabels();
            applyFilters();
        }});
    }});
    document.getElementById('searchBox').addEventListener('input', applyFilters);
}}

// Boot
window.addEventListener('load', () => {{
    initMap();
    buildStateFilters();
    buildRuleFilters();
    bindEvents();
    applyFilters();
    document.getElementById('loading').classList.add('hidden');
}});
</script>
</body>
</html>'''

    return html_content


def main():
    print("=" * 60)
    print("BUILDING PHARMACY FINDER DASHBOARD")
    print("=" * 60)

    print("\nLoading data from population_ranked CSVs...")
    opportunities = load_all_data()

    print(f"\nGenerating dashboard HTML...")
    html_content = generate_html(opportunities)

    output_path = os.path.join(OUTPUT_DIR, 'dashboard.html')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    size_kb = os.path.getsize(output_path) / 1024
    print(f"\nDashboard written to: {output_path}")
    print(f"File size: {size_kb:.0f} KB")
    print(f"Total opportunities: {len(opportunities)}")
    print("=" * 60)


if __name__ == '__main__':
    main()
