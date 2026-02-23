#!/usr/bin/env python3
"""
PharmacyFinder Dashboard v3 - Clean rebuild.

Generates a self-contained output/dashboard.html with:
- Top bar with stats and filters
- Left panel: ranked opportunity cards
- Centre: Leaflet.js map (scored opportunities + clustered pharmacy dots)
- Right panel: detailed view on click
- Export CSV button

Run: python build_dashboard_v3.py
"""

import sqlite3
import json
import os
import math
from score_v2 import score_opportunities, update_db

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'output')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'dashboard.html')

AUS_AVG_RATIO = 4500


def load_scored_opportunities():
    """Run scorer and return scored opportunities with full DB data."""
    scored = score_opportunities()
    update_db(scored)
    
    # Enrich with full DB columns
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    enriched = []
    for s in scored:
        cur.execute("SELECT * FROM opportunities WHERE id = ?", (s['id'],))
        row = cur.fetchone()
        if not row:
            continue
        d = dict(row)
        enriched.append({
            'id': s['id'],
            'name': s['name'],
            'state': s['state'],
            'lat': s['lat'],
            'lng': s['lng'],
            'address': s['address'],
            'score': s['score'],
            'ratio': s['ratio'],
            'pop_5km': d['pop_5km'] or 0,
            'pop_10km': s['pop_10km'],
            'pop_15km': d['pop_15km'] or 0,
            'pharmacy_5km': d['pharmacy_5km'] or 0,
            'pharmacy_10km': s['pharmacies_10km'],
            'pharmacy_15km': d['pharmacy_15km'] or 0,
            'nearest_pharmacy_km': round(s['nearest_pharmacy_km'], 2),
            'nearest_pharmacy': s['nearest_pharmacy'],
            'rules': s['rules'],
            'evidence': s['evidence'],
            'growth': s['growth'],
            'growth_details': d.get('growth_details') or '',
            'verification': d.get('verification') or 'UNVERIFIED',
            'nearest_town': d.get('nearest_town') or '',
        })
    
    conn.close()
    return enriched


def load_pharmacies():
    """Load all pharmacies for background map dots."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""SELECT name, latitude as lat, longitude as lng, state, suburb 
                   FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL""")
    pharmacies = [dict(r) for r in cur.fetchall()]
    conn.close()
    return pharmacies


def load_pois_for_opportunities(opp_ids):
    """Load nearby POIs for each opportunity. Returns dict keyed by opp ID."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Load all scored opportunities' coords
    cur.execute("SELECT id, latitude, longitude FROM opportunities WHERE id IN ({})".format(
        ','.join('?' * len(opp_ids))), opp_ids)
    opp_coords = {r['id']: (r['latitude'], r['longitude']) for r in cur.fetchall()}
    
    # Load all POI tables
    poi_tables = {
        'supermarkets': "SELECT name, latitude, longitude, brand FROM supermarkets WHERE latitude IS NOT NULL",
        'medical_centres': "SELECT name, latitude, longitude, num_gps, total_fte FROM medical_centres WHERE latitude IS NOT NULL",
        'shopping_centres': "SELECT name, latitude, longitude, gla_sqm, centre_class FROM shopping_centres WHERE latitude IS NOT NULL",
        'gps': "SELECT name, latitude, longitude, fte FROM gps WHERE latitude IS NOT NULL",
        'hospitals': "SELECT name, latitude, longitude, bed_count, hospital_type FROM hospitals WHERE latitude IS NOT NULL",
    }
    
    all_pois = {}
    for table, query in poi_tables.items():
        cur.execute(query)
        all_pois[table] = [dict(r) for r in cur.fetchall()]
    
    conn.close()
    
    # For each opportunity, find POIs within 10km
    poi_lookup = {}
    for oid in opp_ids:
        if oid not in opp_coords:
            continue
        olat, olng = opp_coords[oid]
        nearby = {}
        for table, pois in all_pois.items():
            items = []
            for p in pois:
                dist = haversine(olat, olng, p['latitude'], p['longitude'])
                if dist <= 10:
                    item = {'name': p['name'], 'dist_km': round(dist, 1)}
                    if table == 'supermarkets':
                        item['brand'] = p.get('brand') or ''
                    elif table == 'medical_centres':
                        item['gps'] = p.get('num_gps') or 0
                        item['fte'] = p.get('total_fte') or 0
                    elif table == 'shopping_centres':
                        item['gla'] = p.get('gla_sqm') or 0
                        item['cls'] = p.get('centre_class') or ''
                    elif table == 'gps':
                        item['fte'] = p.get('fte') or 0
                    elif table == 'hospitals':
                        item['beds'] = p.get('bed_count') or 0
                        item['type'] = p.get('hospital_type') or ''
                    items.append(item)
            items.sort(key=lambda x: x['dist_km'])
            nearby[table] = items[:8]  # Top 8 nearest
        poi_lookup[oid] = nearby
    
    return poi_lookup


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dLon/2)**2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def generate_html(opportunities, pharmacies, poi_lookup):
    """Generate the full self-contained HTML dashboard."""
    
    opp_json = json.dumps(opportunities, ensure_ascii=False)
    pharm_json = json.dumps(pharmacies, ensure_ascii=False)
    poi_json = json.dumps(poi_lookup, ensure_ascii=False)
    
    avg_ratio = sum(o['ratio'] for o in opportunities) / len(opportunities) if opportunities else 0
    
    # Collect unique states and rules for filter generation
    states = sorted(set(o['state'] for o in opportunities))
    rules_set = set()
    for o in opportunities:
        for r in o['rules'].split(','):
            r = r.strip()
            if r:
                rules_set.add(r)
    rules_list = sorted(rules_set)
    
    max_pop = max((o['pop_10km'] for o in opportunities), default=100000)
    max_ratio = max((o['ratio'] for o in opportunities), default=50000)
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PharmacyFinder - Greenfield Opportunity Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f1117;color:#e2e8f0;overflow:hidden;height:100vh}}

/* === TOP BAR === */
.topbar{{
  background:linear-gradient(135deg,#0f1117 0%,#1a1d2e 50%,#1e2235 100%);
  border-bottom:1px solid #2a2f45;
  padding:0 20px;height:54px;display:flex;align-items:center;gap:24px;
  position:fixed;top:0;left:0;right:0;z-index:1000;
}}
.topbar .logo{{font-size:18px;font-weight:700;white-space:nowrap}}
.topbar .logo span{{color:#22c55e}}
.topbar .stats{{display:flex;gap:16px;font-size:12px;color:#94a3b8;flex-shrink:0}}
.topbar .stats b{{color:#22c55e;font-weight:600}}
.filters{{display:flex;gap:10px;align-items:center;flex:1;overflow-x:auto;padding:4px 0}}
.filter-group{{display:flex;align-items:center;gap:4px;flex-shrink:0}}
.filter-group label{{font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap}}
.filter-group select,.filter-group input[type=range]{{
  background:#1a1d2e;color:#e2e8f0;border:1px solid #2a2f45;border-radius:6px;
  font-size:11px;padding:4px 6px;outline:none;cursor:pointer;
}}
.filter-group select:focus,.filter-group input:focus{{border-color:#22c55e}}
.filter-group input[type=range]{{width:100px;height:4px;padding:0;-webkit-appearance:none;background:#2a2f45;border:none;border-radius:2px}}
.filter-group input[type=range]::-webkit-slider-thumb{{-webkit-appearance:none;width:12px;height:12px;border-radius:50%;background:#22c55e;cursor:pointer}}
.filter-val{{font-size:10px;color:#22c55e;min-width:35px;text-align:right}}
.rule-checks{{display:flex;gap:6px;flex-shrink:0}}
.rule-check{{display:flex;align-items:center;gap:3px;font-size:10px;color:#94a3b8;cursor:pointer}}
.rule-check input{{accent-color:#22c55e;width:12px;height:12px}}
.btn-reset{{
  background:#2a2f45;color:#94a3b8;border:1px solid #3a3f55;border-radius:6px;
  font-size:10px;padding:4px 10px;cursor:pointer;white-space:nowrap;
}}
.btn-reset:hover{{background:#3a3f55;color:#e2e8f0}}

/* === LAYOUT === */
.container{{display:flex;margin-top:54px;height:calc(100vh - 54px)}}

/* === LEFT PANEL === */
.left-panel{{
  width:310px;min-width:310px;background:#13151f;border-right:1px solid #2a2f45;
  display:flex;flex-direction:column;overflow:hidden;
}}
.list-header{{
  padding:10px 14px;border-bottom:1px solid #2a2f45;
  display:flex;justify-content:space-between;align-items:center;
  background:#181b28;flex-shrink:0;
}}
.list-header h3{{font-size:12px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px}}
.list-count{{font-size:11px;color:#22c55e;font-weight:600}}
.card-list{{flex:1;overflow-y:auto;padding:6px}}
.card-list::-webkit-scrollbar{{width:6px}}
.card-list::-webkit-scrollbar-track{{background:#13151f}}
.card-list::-webkit-scrollbar-thumb{{background:#2a2f45;border-radius:3px}}
.card-list::-webkit-scrollbar-thumb:hover{{background:#3a3f55}}

.opp-card{{
  background:#1a1d2e;border-radius:8px;padding:10px 12px;margin-bottom:6px;
  cursor:pointer;transition:all .15s;border-left:4px solid transparent;
  position:relative;
}}
.opp-card:hover{{background:#1e2235;transform:translateX(2px)}}
.opp-card.selected{{background:#1e2840;border-left-color:#22c55e;box-shadow:0 0 12px rgba(34,197,94,.15)}}
.opp-card.tier-red{{border-left-color:#ef4444}}
.opp-card.tier-orange{{border-left-color:#f97316}}
.opp-card.tier-yellow{{border-left-color:#eab308}}
.card-rank{{
  position:absolute;top:8px;right:10px;font-size:10px;color:#64748b;font-weight:700;
}}
.card-name{{font-size:13px;font-weight:600;color:#e2e8f0;margin-bottom:2px;padding-right:30px}}
.card-state{{font-size:10px;color:#94a3b8;margin-bottom:6px}}
.card-ratio{{font-size:22px;font-weight:800;margin-bottom:2px;line-height:1}}
.card-ratio.tier-red{{color:#ef4444}}
.card-ratio.tier-orange{{color:#f97316}}
.card-ratio.tier-yellow{{color:#eab308}}
.card-meta{{display:flex;gap:12px;font-size:10px;color:#64748b;margin-bottom:4px}}
.card-meta b{{color:#94a3b8}}
.card-rules{{display:flex;flex-wrap:wrap;gap:3px}}
.rule-tag{{
  font-size:9px;background:#2a2f45;color:#94a3b8;padding:2px 6px;border-radius:10px;
}}

/* === MAP === */
.map-area{{flex:1;position:relative}}
#map{{width:100%;height:100%;background:#0f1117}}
.leaflet-container{{background:#0f1117 !important}}

/* Pharmacy cluster styling */
.pharm-cluster{{
  background:rgba(120,120,140,.4);border-radius:50%;display:flex;
  align-items:center;justify-content:center;
}}
.pharm-cluster div{{
  background:rgba(120,120,140,.7);width:28px;height:28px;border-radius:50%;
  display:flex;align-items:center;justify-content:center;
  color:#e2e8f0;font-size:10px;font-weight:700;
}}

.layer-toggle{{
  position:absolute;top:10px;left:10px;background:#1a1d2e;border:1px solid #2a2f45;
  border-radius:8px;padding:8px 12px;z-index:800;font-size:11px;
}}
.layer-toggle label{{display:flex;align-items:center;gap:6px;cursor:pointer;color:#94a3b8;padding:2px 0}}
.layer-toggle label:hover{{color:#e2e8f0}}
.layer-toggle input{{accent-color:#22c55e}}

/* === RIGHT PANEL === */
.right-panel{{
  width:0;overflow:hidden;background:#13151f;border-left:1px solid #2a2f45;
  transition:width .25s ease;flex-shrink:0;
}}
.right-panel.open{{width:380px}}
.detail-inner{{width:380px;height:100%;overflow-y:auto;padding:0}}
.detail-inner::-webkit-scrollbar{{width:6px}}
.detail-inner::-webkit-scrollbar-track{{background:#13151f}}
.detail-inner::-webkit-scrollbar-thumb{{background:#2a2f45;border-radius:3px}}

.detail-header{{
  background:linear-gradient(135deg,#1a1d2e,#1e2235);
  padding:16px;border-bottom:1px solid #2a2f45;position:relative;
}}
.detail-close{{
  position:absolute;top:12px;right:12px;background:none;border:none;
  color:#64748b;font-size:20px;cursor:pointer;line-height:1;padding:4px;
}}
.detail-close:hover{{color:#e2e8f0}}
.detail-name{{font-size:16px;font-weight:700;color:#e2e8f0;margin-bottom:2px;padding-right:30px}}
.detail-addr{{font-size:11px;color:#64748b;margin-bottom:10px}}
.detail-ratio-big{{font-size:36px;font-weight:800;line-height:1;margin-bottom:4px}}
.detail-ratio-big.tier-red{{color:#ef4444}}
.detail-ratio-big.tier-orange{{color:#f97316}}
.detail-ratio-big.tier-yellow{{color:#eab308}}
.detail-ratio-label{{font-size:11px;color:#94a3b8}}

/* Ratio bar */
.ratio-bar-wrap{{margin:12px 0 8px;padding:0 16px}}
.ratio-bar-label{{display:flex;justify-content:space-between;font-size:10px;color:#64748b;margin-bottom:4px}}
.ratio-bar{{height:8px;background:#2a2f45;border-radius:4px;position:relative;overflow:visible}}
.ratio-bar-fill{{height:100%;border-radius:4px;transition:width .4s ease}}
.ratio-bar-avg{{
  position:absolute;top:-4px;height:16px;width:2px;background:#eab308;
}}
.ratio-bar-avg-label{{
  position:absolute;top:-18px;font-size:9px;color:#eab308;white-space:nowrap;
  transform:translateX(-50%);
}}

/* Detail sections */
.detail-section{{padding:12px 16px;border-bottom:1px solid #1e2235}}
.detail-section h4{{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;font-weight:600}}
.detail-grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}}
.detail-stat{{text-align:center}}
.detail-stat .val{{font-size:16px;font-weight:700;color:#e2e8f0}}
.detail-stat .lbl{{font-size:9px;color:#64748b;text-transform:uppercase}}

.nearest-ph{{
  background:#1a1d2e;border-radius:8px;padding:10px;margin-top:8px;
  display:flex;align-items:center;gap:10px;
}}
.nearest-ph .icon{{width:32px;height:32px;background:#2a2f45;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0}}
.nearest-ph .info{{flex:1}}
.nearest-ph .ph-name{{font-size:12px;font-weight:600;color:#e2e8f0}}
.nearest-ph .ph-dist{{font-size:11px;color:#94a3b8}}

.rule-item{{
  background:#1a1d2e;border-radius:8px;padding:10px;margin-bottom:6px;
  border-left:3px solid #22c55e;
}}
.rule-item .rule-name{{font-size:12px;font-weight:600;color:#22c55e;margin-bottom:3px}}
.rule-item .rule-evidence{{font-size:10px;color:#94a3b8;line-height:1.4}}

.poi-item{{
  display:flex;align-items:center;gap:8px;padding:5px 0;
  border-bottom:1px solid #1e2235;font-size:11px;
}}
.poi-item:last-child{{border-bottom:none}}
.poi-icon{{font-size:12px;flex-shrink:0}}
.poi-name{{color:#e2e8f0;flex:1}}
.poi-dist{{color:#64748b;font-size:10px;flex-shrink:0}}

.detail-actions{{padding:12px 16px;display:flex;flex-direction:column;gap:6px}}
.btn-action{{
  display:block;text-align:center;padding:8px;border-radius:8px;font-size:12px;
  font-weight:600;text-decoration:none;transition:background .15s;
}}
.btn-gmaps{{background:#1a1d2e;color:#4285f4;border:1px solid #2a2f45}}
.btn-gmaps:hover{{background:#1e2235}}
.btn-export{{background:#22c55e;color:#0f1117;border:none;cursor:pointer;width:100%}}
.btn-export:hover{{background:#16a34a}}

/* === BOTTOM EXPORT === */
.bottom-bar{{
  display:none; /* hidden if not needed; export is in detail or via button */
}}

/* Leaflet dark overrides */
.leaflet-tile-pane{{filter:brightness(0.7) contrast(1.1) saturate(0.3) hue-rotate(180deg) invert(1)}}
.leaflet-control-zoom a{{background:#1a1d2e !important;color:#e2e8f0 !important;border-color:#2a2f45 !important}}
.leaflet-control-zoom a:hover{{background:#2a2f45 !important}}
.leaflet-control-attribution{{background:rgba(15,17,23,.8) !important;color:#64748b !important;font-size:9px !important}}
.leaflet-control-attribution a{{color:#64748b !important}}
.leaflet-popup-content-wrapper{{background:#1a1d2e;color:#e2e8f0;border-radius:10px;box-shadow:0 4px 20px rgba(0,0,0,.4)}}
.leaflet-popup-tip{{background:#1a1d2e}}
.leaflet-popup-close-button{{color:#64748b !important}}
.leaflet-popup-close-button:hover{{color:#e2e8f0 !important}}
</style>
</head>
<body>

<!-- TOP BAR -->
<div class="topbar">
  <div class="logo">&#x1F48A; <span>Pharmacy</span>Finder</div>
  <div class="stats">
    <span>Opportunities: <b id="statTotal">{len(opportunities)}</b></span>
    <span>Avg Ratio: <b id="statAvg">{avg_ratio:,.0f}</b></span>
    <span>Showing: <b id="statShowing">{len(opportunities)}</b></span>
  </div>
  <div class="filters" id="filtersBar">
    <div class="filter-group">
      <label>State</label>
      <select id="filterState" multiple size="1" style="min-width:70px">
        <option value="ALL" selected>All</option>
        {"".join(f'<option value="{s}">{s}</option>' for s in states)}
      </select>
    </div>
    <div class="filter-group">
      <label>Min Pop</label>
      <input type="range" id="filterPop" min="0" max="{max_pop}" step="1000" value="0"/>
      <span class="filter-val" id="filterPopVal">0</span>
    </div>
    <div class="filter-group">
      <label>Min Ratio</label>
      <input type="range" id="filterRatio" min="0" max="{max_ratio}" step="500" value="0"/>
      <span class="filter-val" id="filterRatioVal">0</span>
    </div>
    <div class="filter-group rule-checks">
      <label style="margin-right:2px">Rules:</label>
      {"".join(f'<label class="rule-check"><input type="checkbox" class="ruleCb" value="{r}" checked/>{r.replace("Item ","")}</label>' for r in rules_list)}
    </div>
    <button class="btn-reset" onclick="resetFilters()">&#x1F504; Reset</button>
  </div>
</div>

<!-- MAIN LAYOUT -->
<div class="container">
  <!-- LEFT: RANKED LIST -->
  <div class="left-panel">
    <div class="list-header">
      <h3>Ranked Opportunities</h3>
      <span class="list-count" id="listCount">{len(opportunities)}</span>
    </div>
    <div class="card-list" id="cardList"></div>
    <div style="padding:8px;border-top:1px solid #2a2f45;flex-shrink:0">
      <button class="btn-export" onclick="exportCSV()">&#x1F4E5; Export Filtered CSV</button>
    </div>
  </div>

  <!-- CENTRE: MAP -->
  <div class="map-area">
    <div id="map"></div>
    <div class="layer-toggle">
      <label><input type="checkbox" id="togglePharmacies" checked/> Pharmacies (grey dots)</label>
    </div>
  </div>

  <!-- RIGHT: DETAIL PANEL -->
  <div class="right-panel" id="rightPanel">
    <div class="detail-inner" id="detailInner"></div>
  </div>
</div>

<script>
// ============================================================
// DATA
// ============================================================
const OPPS = {opp_json};
const PHARMS = {pharm_json};
const POI_LOOKUP = {poi_json};
const AUS_AVG = {AUS_AVG_RATIO};

// ============================================================
// STATE
// ============================================================
let map, pharmCluster, oppMarkers = {{}}, selectedId = null;
let filtered = [...OPPS];

// ============================================================
// HELPERS
// ============================================================
function tierColor(ratio) {{
  if (ratio > 20000) return '#ef4444';
  if (ratio > 10000) return '#f97316';
  return '#eab308';
}}
function tierClass(ratio) {{
  if (ratio > 20000) return 'tier-red';
  if (ratio > 10000) return 'tier-orange';
  return 'tier-yellow';
}}
function fmt(n) {{ return n.toLocaleString(); }}
function markerSize(score) {{
  return Math.max(8, Math.min(22, 6 + score * 0.16));
}}

// ============================================================
// MAP INIT
// ============================================================
function initMap() {{
  map = L.map('map', {{ zoomControl: true }}).setView([-25.5, 134], 5);
  L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}@2x.png', {{
    attribution: '&copy; OSM &copy; CARTO',
    maxZoom: 19
  }}).addTo(map);

  // Pharmacy cluster layer
  pharmCluster = L.markerClusterGroup({{
    maxClusterRadius: 50,
    showCoverageOnHover: false,
    zoomToBoundsOnClick: true,
    disableClusteringAtZoom: 14,
    iconCreateFunction: function(cluster) {{
      var c = cluster.getChildCount();
      var s = c > 100 ? 36 : c > 30 ? 30 : 24;
      return L.divIcon({{
        html: '<div style="width:'+s+'px;height:'+s+'px;line-height:'+s+'px;text-align:center;border-radius:50%;background:rgba(120,120,140,.6);color:#ccc;font-size:10px;font-weight:700">' + c + '</div>',
        className: '',
        iconSize: L.point(s+8, s+8)
      }});
    }}
  }});

  // Add pharmacy markers
  PHARMS.forEach(function(p) {{
    if (!p.lat || !p.lng) return;
    var m = L.circleMarker([p.lat, p.lng], {{
      radius: 3, fillColor: '#64748b', color: '#475569',
      weight: 0.5, opacity: 0.6, fillOpacity: 0.5
    }});
    m.bindPopup('<div style="font-size:12px"><b>' + (p.name||'Pharmacy') + '</b><br><span style="color:#94a3b8">' + (p.suburb||'') + ' ' + (p.state||'') + '</span></div>', {{maxWidth: 250}});
    pharmCluster.addLayer(m);
  }});
  map.addLayer(pharmCluster);

  // Toggle pharmacy layer
  document.getElementById('togglePharmacies').addEventListener('change', function() {{
    if (this.checked) map.addLayer(pharmCluster);
    else map.removeLayer(pharmCluster);
  }});
}}

// ============================================================
// OPPORTUNITY MARKERS
// ============================================================
function addOppMarkers() {{
  // Clear existing
  Object.values(oppMarkers).forEach(function(m) {{ map.removeLayer(m); }});
  oppMarkers = {{}};

  filtered.forEach(function(o) {{
    var color = tierColor(o.ratio);
    var size = markerSize(o.score);
    var m = L.circleMarker([o.lat, o.lng], {{
      radius: size, fillColor: color, color: '#fff',
      weight: 2, opacity: 0.9, fillOpacity: 0.85
    }});
    m.on('click', function() {{
      selectOpportunity(o.id);
    }});
    m.bindTooltip(o.name + ' — ' + fmt(o.ratio) + ':1', {{
      direction: 'top', className: '', offset: [0, -size]
    }});
    m.addTo(map);
    oppMarkers[o.id] = m;
  }});
}}

// ============================================================
// CARDS
// ============================================================
function renderCards() {{
  var container = document.getElementById('cardList');
  container.innerHTML = '';
  filtered.forEach(function(o, i) {{
    var tier = tierClass(o.ratio);
    var card = document.createElement('div');
    card.className = 'opp-card ' + tier + (o.id === selectedId ? ' selected' : '');
    card.setAttribute('data-id', o.id);
    card.onclick = function() {{ selectOpportunity(o.id); }};
    
    var rulesHtml = o.rules.split(',').map(function(r) {{
      return '<span class="rule-tag">' + r.trim() + '</span>';
    }}).join('');
    
    card.innerHTML =
      '<span class="card-rank">#' + (i+1) + '</span>' +
      '<div class="card-name">' + o.name + '</div>' +
      '<div class="card-state">' + o.state + (o.nearest_town ? ' \u2022 ' + o.nearest_town : '') + '</div>' +
      '<div class="card-ratio ' + tier + '">' + fmt(o.ratio) + ':1</div>' +
      '<div class="card-meta">' +
        '<span>Pop <b>' + fmt(o.pop_10km) + '</b></span>' +
        '<span>Ph <b>' + o.pharmacy_10km + '</b></span>' +
        '<span>Score <b>' + o.score + '</b></span>' +
      '</div>' +
      '<div class="card-rules">' + rulesHtml + '</div>';
    container.appendChild(card);
  }});
}}

// ============================================================
// SELECT / DETAIL
// ============================================================
function selectOpportunity(id) {{
  selectedId = id;
  var o = OPPS.find(function(x) {{ return x.id === id; }});
  if (!o) return;

  // Highlight card
  document.querySelectorAll('.opp-card').forEach(function(c) {{
    c.classList.toggle('selected', parseInt(c.getAttribute('data-id')) === id);
  }});
  // Scroll card into view
  var selCard = document.querySelector('.opp-card[data-id="' + id + '"]');
  if (selCard) selCard.scrollIntoView({{ behavior: 'smooth', block: 'nearest' }});

  // Fly map
  if (oppMarkers[id]) {{
    map.flyTo([o.lat, o.lng], 12, {{ duration: 0.8 }});
    oppMarkers[id].openTooltip();
  }}

  // Highlight marker
  Object.entries(oppMarkers).forEach(function(kv) {{
    var mid = parseInt(kv[0]);
    var mm = kv[1];
    if (mid === id) {{
      mm.setStyle({{ weight: 4, color: '#22c55e' }});
      mm.bringToFront();
    }} else {{
      mm.setStyle({{ weight: 2, color: '#fff' }});
    }}
  }});

  // Build detail panel
  showDetail(o);
}}

function showDetail(o) {{
  var panel = document.getElementById('rightPanel');
  var inner = document.getElementById('detailInner');
  panel.classList.add('open');

  var tier = tierClass(o.ratio);
  var color = tierColor(o.ratio);

  // Ratio bar: scale 0 to max(ratio, 50000)
  var barMax = Math.max(o.ratio * 1.2, 30000);
  var fillPct = Math.min(100, (o.ratio / barMax) * 100);
  var avgPct = Math.min(100, (AUS_AVG / barMax) * 100);

  // Rules breakdown
  var rulesHtml = o.rules.split(',').map(function(r) {{
    r = r.trim();
    var desc = {{
      'Item 130': 'New pharmacy 1.5km+ from nearest, near GP/supermarket',
      'Item 131': 'Rural/remote area, 10km+ from nearest pharmacy',
      'Item 132': 'In shopping centre with GLA 15,000+ sqm',
      'Item 133': 'Adjoining supermarket 1,000+ sqm',
      'Item 134': 'In shopping centre with GLA 5,000\u201315,000 sqm',
      'Item 136': 'In medical centre with 8+ FTE prescribers',
    }};
    return '<div class="rule-item"><div class="rule-name">' + r + '</div><div class="rule-evidence">' + (desc[r] || '') + '</div></div>';
  }}).join('');

  // Evidence
  var evidenceHtml = o.evidence
    ? '<div class="detail-section"><h4>Evidence</h4><div style="font-size:11px;color:#94a3b8;line-height:1.5">' + o.evidence.replace(/</g,'&lt;') + '</div></div>'
    : '';

  // Growth
  var growthHtml = o.growth
    ? '<div class="detail-section"><h4>Growth Indicator</h4><div style="font-size:11px;color:#22c55e">&#x1F4C8; ' + (o.growth_details || o.growth) + '</div></div>'
    : '';

  // POI data
  var pois = POI_LOOKUP[o.id] || {{}};
  var poiSections = '';
  
  var poiConfig = [
    ['medical_centres', 'Medical Centres', '\u2695\uFE0F', function(p) {{ return p.gps ? p.gps + ' GPs' : ''; }}],
    ['gps', 'GP Practices', '\uD83D\uDC68\u200D\u2695\uFE0F', function(p) {{ return p.fte ? p.fte + ' FTE' : ''; }}],
    ['shopping_centres', 'Shopping Centres', '\uD83C\uDFEC', function(p) {{ return p.gla ? fmt(Math.round(p.gla)) + ' sqm' : ''; }}],
    ['supermarkets', 'Supermarkets', '\uD83D\uDED2', function(p) {{ return p.brand || ''; }}],
    ['hospitals', 'Hospitals', '\uD83C\uDFE5', function(p) {{ return p.beds ? p.beds + ' beds' : ''; }}],
  ];

  poiConfig.forEach(function(cfg) {{
    var key = cfg[0], title = cfg[1], icon = cfg[2], detailFn = cfg[3];
    var items = pois[key] || [];
    if (items.length === 0) return;
    var itemsHtml = items.slice(0, 5).map(function(p) {{
      var detail = detailFn(p);
      return '<div class="poi-item">' +
        '<span class="poi-icon">' + icon + '</span>' +
        '<span class="poi-name">' + p.name + (detail ? ' <span style="color:#64748b">(' + detail + ')</span>' : '') + '</span>' +
        '<span class="poi-dist">' + p.dist_km + ' km</span>' +
      '</div>';
    }}).join('');
    poiSections += '<div class="detail-section"><h4>' + title + ' Nearby</h4>' + itemsHtml + '</div>';
  }});

  inner.innerHTML =
    '<div class="detail-header">' +
      '<button class="detail-close" onclick="closeDetail()">&times;</button>' +
      '<div class="detail-name">' + o.name + '</div>' +
      '<div class="detail-addr">' + (o.address || o.state) + '</div>' +
      '<div class="detail-ratio-big ' + tier + '">' + fmt(o.ratio) + '</div>' +
      '<div class="detail-ratio-label">people per pharmacy</div>' +
    '</div>' +
    '<div class="ratio-bar-wrap">' +
      '<div class="ratio-bar-label"><span>0</span><span>' + fmt(Math.round(barMax)) + '</span></div>' +
      '<div class="ratio-bar">' +
        '<div class="ratio-bar-fill" style="width:' + fillPct + '%;background:' + color + '"></div>' +
        '<div class="ratio-bar-avg" style="left:' + avgPct + '%">' +
          '<span class="ratio-bar-avg-label">Aus avg ' + fmt(AUS_AVG) + '</span>' +
        '</div>' +
      '</div>' +
    '</div>' +
    '<div class="detail-section"><h4>Population</h4>' +
      '<div class="detail-grid">' +
        '<div class="detail-stat"><div class="val">' + fmt(o.pop_5km) + '</div><div class="lbl">5 km</div></div>' +
        '<div class="detail-stat"><div class="val">' + fmt(o.pop_10km) + '</div><div class="lbl">10 km</div></div>' +
        '<div class="detail-stat"><div class="val">' + fmt(o.pop_15km) + '</div><div class="lbl">15 km</div></div>' +
      '</div>' +
    '</div>' +
    '<div class="detail-section"><h4>Pharmacy Count</h4>' +
      '<div class="detail-grid">' +
        '<div class="detail-stat"><div class="val">' + o.pharmacy_5km + '</div><div class="lbl">5 km</div></div>' +
        '<div class="detail-stat"><div class="val">' + o.pharmacy_10km + '</div><div class="lbl">10 km</div></div>' +
        '<div class="detail-stat"><div class="val">' + o.pharmacy_15km + '</div><div class="lbl">15 km</div></div>' +
      '</div>' +
      '<div class="nearest-ph">' +
        '<div class="icon">&#x1F3E5;</div>' +
        '<div class="info"><div class="ph-name">' + (o.nearest_pharmacy || 'Unknown') + '</div><div class="ph-dist">' + o.nearest_pharmacy_km + ' km away</div></div>' +
      '</div>' +
    '</div>' +
    '<div class="detail-section"><h4>Qualifying Rules</h4>' + rulesHtml + '</div>' +
    evidenceHtml +
    growthHtml +
    poiSections +
    '<div class="detail-actions">' +
      '<a class="btn-action btn-gmaps" href="https://www.google.com/maps?q=' + o.lat + ',' + o.lng + '" target="_blank" rel="noopener">&#x1F5FA; Open in Google Maps</a>' +
    '</div>';
}}

function closeDetail() {{
  document.getElementById('rightPanel').classList.remove('open');
  selectedId = null;
  document.querySelectorAll('.opp-card').forEach(function(c) {{ c.classList.remove('selected'); }});
  Object.values(oppMarkers).forEach(function(m) {{
    m.setStyle({{ weight: 2, color: '#fff' }});
  }});
}}

// ============================================================
// FILTERS
// ============================================================
function applyFilters() {{
  var stateSelect = document.getElementById('filterState');
  var selStates = Array.from(stateSelect.selectedOptions).map(function(o) {{ return o.value; }});
  var allStates = selStates.includes('ALL') || selStates.length === 0;

  var minPop = parseInt(document.getElementById('filterPop').value) || 0;
  var minRatio = parseInt(document.getElementById('filterRatio').value) || 0;

  var checkedRules = {{}};
  document.querySelectorAll('.ruleCb').forEach(function(c) {{
    if (c.checked) checkedRules[c.value] = true;
  }});

  filtered = OPPS.filter(function(o) {{
    if (!allStates && !selStates.includes(o.state)) return false;
    if (o.pop_10km < minPop) return false;
    if (o.ratio < minRatio) return false;
    var oRules = o.rules.split(',').map(function(r) {{ return r.trim(); }});
    if (!oRules.some(function(r) {{ return checkedRules[r]; }})) return false;
    return true;
  }});

  // Update UI
  addOppMarkers();
  renderCards();
  updateStats();
  
  // If selected opp is no longer in filtered, close detail
  if (selectedId && !filtered.find(function(o) {{ return o.id === selectedId; }})) {{
    closeDetail();
  }}
}}

function updateStats() {{
  document.getElementById('statShowing').textContent = filtered.length;
  document.getElementById('listCount').textContent = filtered.length;
  if (filtered.length > 0) {{
    var avg = filtered.reduce(function(s, o) {{ return s + o.ratio; }}, 0) / filtered.length;
    document.getElementById('statAvg').textContent = fmt(Math.round(avg));
  }}
}}

function resetFilters() {{
  document.getElementById('filterState').value = 'ALL';
  document.getElementById('filterPop').value = 0;
  document.getElementById('filterPopVal').textContent = '0';
  document.getElementById('filterRatio').value = 0;
  document.getElementById('filterRatioVal').textContent = '0';
  document.querySelectorAll('.ruleCb').forEach(function(c) {{ c.checked = true; }});
  applyFilters();
}}

// ============================================================
// EXPORT CSV
// ============================================================
function exportCSV() {{
  var headers = ['Rank','Name','State','Ratio','Score','Pop_10km','Pharmacies_10km','Nearest_Pharmacy','Nearest_Pharmacy_km','Rules','Lat','Lng','Address'];
  var rows = filtered.map(function(o, i) {{
    return [i+1, '"'+o.name+'"', o.state, o.ratio, o.score, o.pop_10km, o.pharmacy_10km,
      '"'+(o.nearest_pharmacy||'')+'"', o.nearest_pharmacy_km, '"'+o.rules+'"',
      o.lat, o.lng, '"'+(o.address||'')+'"'].join(',');
  }});
  var csv = headers.join(',') + '\\n' + rows.join('\\n');
  var blob = new Blob([csv], {{ type: 'text/csv' }});
  var url = URL.createObjectURL(blob);
  var a = document.createElement('a');
  a.href = url; a.download = 'pharmacyfinder_opportunities.csv';
  a.click(); URL.revokeObjectURL(url);
}}

// ============================================================
// INIT
// ============================================================
document.addEventListener('DOMContentLoaded', function() {{
  initMap();
  addOppMarkers();
  renderCards();

  // Filter event listeners
  document.getElementById('filterState').addEventListener('change', applyFilters);
  
  document.getElementById('filterPop').addEventListener('input', function() {{
    var v = parseInt(this.value);
    document.getElementById('filterPopVal').textContent = v >= 1000 ? Math.round(v/1000) + 'k' : v;
  }});
  document.getElementById('filterPop').addEventListener('change', applyFilters);
  
  document.getElementById('filterRatio').addEventListener('input', function() {{
    var v = parseInt(this.value);
    document.getElementById('filterRatioVal').textContent = v >= 1000 ? Math.round(v/1000) + 'k' : v;
  }});
  document.getElementById('filterRatio').addEventListener('change', applyFilters);
  
  document.querySelectorAll('.ruleCb').forEach(function(c) {{
    c.addEventListener('change', applyFilters);
  }});

  // Fit map to opportunities
  if (OPPS.length > 0) {{
    var bounds = L.latLngBounds(OPPS.map(function(o) {{ return [o.lat, o.lng]; }}));
    map.fitBounds(bounds.pad(0.1));
  }}
}});
</script>
</body>
</html>'''
    
    return html


def build():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("=" * 80)
    print("PharmacyFinder Dashboard v3 Builder")
    print("=" * 80)
    
    # 1. Score
    print("\n[1/5] Running scorer...")
    opportunities = load_scored_opportunities()
    print(f"  Scored {len(opportunities)} opportunities")
    
    # 2. Pharmacies
    print("\n[2/5] Loading pharmacies...")
    pharmacies = load_pharmacies()
    print(f"  Loaded {len(pharmacies)} pharmacies")
    
    # 3. POIs
    print("\n[3/5] Loading POI data...")
    opp_ids = [o['id'] for o in opportunities]
    poi_lookup = load_pois_for_opportunities(opp_ids)
    print(f"  Built POI lookup for {len(poi_lookup)} opportunities")
    
    # 4. Generate HTML
    print("\n[4/5] Generating dashboard HTML...")
    html = generate_html(opportunities, pharmacies, poi_lookup)
    
    # 5. Write
    print("\n[5/5] Writing dashboard...")
    # Clean surrogates from data
    html_clean = html.encode('utf-8', errors='replace').decode('utf-8')
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html_clean)
    
    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"\n{'=' * 80}")
    print(f"Dashboard written to: {OUTPUT_FILE}")
    print(f"File size: {size_kb:,.1f} KB")
    print(f"Opportunities: {len(opportunities)}")
    print(f"Pharmacies (background): {len(pharmacies)}")
    print(f"{'=' * 80}")


if __name__ == '__main__':
    build()
