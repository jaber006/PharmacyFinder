#!/usr/bin/env python3
"""Build the interactive PharmacyFinder dashboard from CSV data."""

import csv
import json
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']


def load_data():
    """Load all opportunity data from CSVs, preferring population_ranked > verified > raw."""
    all_data = []
    for state in STATES:
        # Try files in priority order
        for prefix in ['population_ranked_', 'verified_opportunities_', 'opportunity_zones_']:
            path = os.path.join(OUTPUT_DIR, f'{prefix}{state}.csv')
            if os.path.exists(path):
                break
        else:
            print(f"  No CSV found for {state}, skipping")
            continue

        print(f"  Loading {os.path.basename(path)}")
        with open(path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    rec = {
                        'lat': float(row['Latitude']),
                        'lng': float(row['Longitude']),
                        'addr': row.get('Address', ''),
                        'rules': row.get('Qualifying Rules', ''),
                        'evidence': row.get('Evidence', ''),
                        'confidence': row.get('Confidence', ''),
                        'nearPharmKm': float(row.get('Nearest Pharmacy (km)', 0) or 0),
                        'nearPharmName': row.get('Nearest Pharmacy Name', ''),
                        'poiName': row.get('POI Name', ''),
                        'poiType': row.get('POI Type', ''),
                        'state': state,
                        'verification': row.get('Verification', 'UNVERIFIED'),
                        'verificationNotes': row.get('Verification Notes', ''),
                        'pop5': int(row.get('Pop 5km', 0) or 0),
                        'pop10': int(row.get('Pop 10km', 0) or 0),
                        'pop15': int(row.get('Pop 15km', 0) or 0),
                        'nearTown': row.get('Nearest Town', ''),
                        'nearTownPop': int(row.get('Nearest Town Pop', 0) or 0),
                        'nearTownDist': float(row.get('Nearest Town Dist (km)', 0) or 0),
                        'score': float(row.get('Opportunity Score', 0) or 0),
                        'pharm5': int(row.get('Pharmacies 5km', 0) or 0),
                        'pharm10': int(row.get('Pharmacies 10km', 0) or 0),
                        'pharm15': int(row.get('Pharmacies 15km', 0) or 0),
                        'chains5': int(row.get('Chains 5km', 0) or 0),
                        'indep5': int(row.get('Independents 5km', 0) or 0),
                        'chainNames5': row.get('Chain Names 5km', ''),
                        'chainNames10': row.get('Chain Names 10km', ''),
                        'compScore': float(row.get('Competition Score', 0) or 0),
                        'popPerPharm5': float(row.get('Pop Per Pharmacy 5km', 0) or 0),
                    }
                    all_data.append(rec)
                except Exception as e:
                    pass
    return all_data


def build_html(data):
    data_json = json.dumps(data, separators=(',', ':'))
    total = len(data)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PharmacyFinder — Opportunity Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0}}
.header{{background:linear-gradient(135deg,#1e293b 0%,#0f172a 100%);padding:16px 24px;display:flex;align-items:center;gap:16px;border-bottom:1px solid #334155;flex-wrap:wrap}}
.header h1{{font-size:22px;font-weight:700;color:#f8fafc}}
.header h1 span{{color:#22d3ee}}
.header .stats{{margin-left:auto;display:flex;gap:20px;font-size:13px;color:#94a3b8}}
.header .stats .stat-val{{font-size:18px;font-weight:700;color:#22d3ee}}
.main{{display:flex;height:calc(100vh - 64px)}}
.sidebar{{width:320px;min-width:320px;background:#1e293b;border-right:1px solid #334155;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:12px}}
.sidebar h3{{font-size:13px;text-transform:uppercase;letter-spacing:1px;color:#64748b;margin-bottom:4px}}
.filter-group{{display:flex;flex-direction:column;gap:6px}}
.filter-group label{{font-size:12px;color:#94a3b8;display:flex;justify-content:space-between;align-items:center}}
.filter-group label span.val{{color:#22d3ee;font-weight:600}}
.filter-group input[type=range]{{width:100%;accent-color:#22d3ee;background:transparent}}
.filter-group select{{background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:6px;padding:6px 8px;font-size:13px;width:100%}}
.checkbox-grid{{display:grid;grid-template-columns:1fr 1fr;gap:4px}}
.checkbox-grid label{{font-size:12px;color:#cbd5e1;display:flex;align-items:center;gap:4px;cursor:pointer}}
.checkbox-grid input[type=checkbox]{{accent-color:#22d3ee}}
.btn{{padding:8px 16px;border:none;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;transition:all 0.2s}}
.btn-primary{{background:#22d3ee;color:#0f172a}}.btn-primary:hover{{background:#06b6d4}}
.btn-secondary{{background:#334155;color:#e2e8f0}}.btn-secondary:hover{{background:#475569}}
.btn-row{{display:flex;gap:8px}}
.content{{flex:1;display:flex;flex-direction:column;overflow:hidden}}
#map{{flex:1;min-height:40vh;background:#0f172a}}
.table-container{{height:38vh;overflow:auto;background:#1e293b;border-top:2px solid #334155}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
thead{{position:sticky;top:0;z-index:10}}
th{{background:#334155;color:#94a3b8;padding:8px 10px;text-align:left;cursor:pointer;user-select:none;white-space:nowrap;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:0.5px}}
th:hover{{background:#475569;color:#e2e8f0}}
th.sorted-asc::after{{content:' ▲';color:#22d3ee}}
th.sorted-desc::after{{content:' ▼';color:#22d3ee}}
td{{padding:6px 10px;border-bottom:1px solid #1e293b;white-space:nowrap;max-width:200px;overflow:hidden;text-overflow:ellipsis}}
tr{{background:#0f172a;cursor:pointer}}
tr:hover{{background:#1e293b}}
tr.selected{{background:#164e63!important}}
.badge{{display:inline-block;padding:2px 6px;border-radius:4px;font-size:10px;font-weight:700}}
.badge-verified{{background:#065f46;color:#6ee7b7}}
.badge-false{{background:#7f1d1d;color:#fca5a5}}
.badge-review{{background:#78350f;color:#fde68a}}
.badge-unverified{{background:#334155;color:#94a3b8}}
.state-dot{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px}}
.legend{{display:flex;flex-wrap:wrap;gap:8px;padding:4px 0}}
.legend-item{{display:flex;align-items:center;gap:4px;font-size:11px;color:#94a3b8}}
.legend-dot{{width:10px;height:10px;border-radius:50%;border:2px solid rgba(255,255,255,0.3)}}
.divider{{border:none;border-top:1px solid #334155;margin:4px 0}}
.resize-bar{{height:6px;background:#334155;cursor:row-resize;display:flex;align-items:center;justify-content:center}}
.resize-bar::after{{content:'';width:40px;height:2px;background:#64748b;border-radius:1px}}
.info-box{{background:#0f172a;border:1px solid #334155;border-radius:8px;padding:12px;font-size:12px;color:#94a3b8;line-height:1.6}}
.info-box strong{{color:#e2e8f0}}
.score-bar{{height:4px;border-radius:2px;background:#334155;width:80px;display:inline-block;vertical-align:middle;margin-left:4px}}
.score-fill{{height:100%;border-radius:2px}}
.pop-format{{font-variant-numeric:tabular-nums}}
.search-box{{background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:6px;padding:6px 8px;font-size:13px;width:100%}}
.search-box::placeholder{{color:#475569}}
@media(max-width:900px){{.sidebar{{display:none}}.main{{flex-direction:column}}}}
</style>
</head>
<body>
<div class="header">
  <h1>&#x1F3E5; Pharmacy<span>Finder</span></h1>
  <div class="stats">
    <div><div class="stat-val" id="statTotal">0</div>Total</div>
    <div><div class="stat-val" id="statShown">0</div>Showing</div>
    <div><div class="stat-val" id="statStates">0</div>States</div>
    <div><div class="stat-val" id="statVerified">0</div>Verified</div>
  </div>
</div>
<div class="main">
  <div class="sidebar">
    <h3>&#x1F50D; Filters</h3>
    <div class="filter-group">
      <input type="text" class="search-box" id="searchBox" placeholder="Search locations..." oninput="applyFilters()">
    </div>
    <hr class="divider">
    <div class="filter-group">
      <label>States</label>
      <div class="checkbox-grid" id="stateFilters"></div>
    </div>
    <hr class="divider">
    <div class="filter-group">
      <label>Verification</label>
      <div class="checkbox-grid" id="verifyFilters"></div>
    </div>
    <hr class="divider">
    <div class="filter-group">
      <label>Min Population (5km) <span class="val" id="popVal">0</span></label>
      <input type="range" id="popFilter" min="0" max="500000" step="1000" value="0" oninput="updateSliderLabels();applyFilters()">
    </div>
    <div class="filter-group">
      <label>Min Distance to Pharmacy (km) <span class="val" id="distVal">0</span></label>
      <input type="range" id="distFilter" min="0" max="100" step="0.5" value="0" oninput="updateSliderLabels();applyFilters()">
    </div>
    <div class="filter-group">
      <label>Min Opportunity Score <span class="val" id="scoreVal">0</span></label>
      <input type="range" id="scoreFilter" min="0" max="500000" step="1000" value="0" oninput="updateSliderLabels();applyFilters()">
    </div>
    <hr class="divider">
    <div class="filter-group">
      <label>Qualifying Rule</label>
      <select id="ruleFilter" onchange="applyFilters()"><option value="">All Rules</option></select>
    </div>
    <hr class="divider">
    <div class="btn-row">
      <button class="btn btn-primary" onclick="applyFilters()">Apply</button>
      <button class="btn btn-secondary" onclick="resetFilters()">Reset</button>
      <button class="btn btn-secondary" onclick="exportCSV()">&#x1F4BE; Export</button>
    </div>
    <hr class="divider">
    <h3>&#x1F5FA; Map Legend</h3>
    <div class="legend" id="legend"></div>
    <hr class="divider">
    <div class="info-box">
      <strong>PharmacyFinder Dashboard</strong><br>
      {total:,} greenfield pharmacy opportunity zones across Australia.
      Click markers for details. Click table rows to zoom.
      Data from OpenStreetMap, OSRM, ABS population estimates.
    </div>
  </div>
  <div class="content">
    <div id="map"></div>
    <div class="resize-bar" id="resizeBar"></div>
    <div class="table-container" id="tableContainer">
      <table>
        <thead><tr id="tableHead"></tr></thead>
        <tbody id="tableBody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
const RAW_DATA = {data_json};

const STATE_COLORS = {{
  NSW:'#ef4444',VIC:'#3b82f6',QLD:'#f59e0b',WA:'#10b981',SA:'#8b5cf6',TAS:'#ec4899',NT:'#f97316',ACT:'#06b6d4'
}};
const VERIFY_COLORS = {{VERIFIED:'#6ee7b7','FALSE POSITIVE':'#fca5a5','NEEDS REVIEW':'#fde68a',UNVERIFIED:'#94a3b8'}};

let map, markerCluster, filteredData = [], sortCol = 'score', sortDir = -1;

function initMap() {{
  map = L.map('map',{{zoomControl:true}}).setView([-25.5,134],4);
  L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png',{{
    attribution:'&copy; OSM &copy; CARTO',maxZoom:19,subdomains:'abcd'
  }}).addTo(map);
  markerCluster = L.markerClusterGroup({{
    maxClusterRadius:50,
    spiderfyOnMaxZoom:true,
    showCoverageOnHover:false,
    chunkedLoading:true,
    iconCreateFunction:function(cluster){{
      const count=cluster.getChildCount();
      let r=30;
      if(count>50) r=40;
      if(count>200) r=50;
      return L.divIcon({{html:'<div style="background:rgba(34,211,238,0.85);color:#0f172a;border-radius:50%;width:'+r+'px;height:'+r+'px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:12px;border:2px solid rgba(255,255,255,0.4);box-shadow:0 2px 8px rgba(0,0,0,0.3)">'+count+'</div>',className:'',iconSize:[r,r]}});
    }}
  }});
  map.addLayer(markerCluster);
}}

function createIcon(state, verification) {{
  const color = STATE_COLORS[state]||'#94a3b8';
  const bcolor = verification==='FALSE POSITIVE'?'#fca5a5':verification==='NEEDS REVIEW'?'#fde68a':'rgba(255,255,255,0.8)';
  const sz = verification==='FALSE POSITIVE'?8:12;
  return L.divIcon({{
    html:'<div style="width:'+sz+'px;height:'+sz+'px;background:'+color+';border-radius:50%;border:2px solid '+bcolor+';box-shadow:0 0 6px '+color+'80"></div>',
    className:'',iconSize:[sz+4,sz+4],iconAnchor:[(sz+4)/2,(sz+4)/2]
  }});
}}

function fmtPop(n){{if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=1e3)return Math.round(n/1e3)+'K';return ''+n}}
function fmtScore(n){{if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=1e3)return(n/1e3).toFixed(1)+'K';return n.toFixed(0)}}

function makePopup(d) {{
  const vBadge = d.verification==='VERIFIED'?'badge-verified':d.verification==='FALSE POSITIVE'?'badge-false':d.verification==='NEEDS REVIEW'?'badge-review':'badge-unverified';
  return '<div style="font-family:-apple-system,sans-serif;min-width:280px;color:#1e293b">'+
    '<div style="font-size:15px;font-weight:700;margin-bottom:6px">'+(d.poiName||'Unknown Location')+'</div>'+
    '<div style="font-size:11px;color:#64748b;margin-bottom:8px">'+(d.addr||d.nearTown+', '+d.state)+'</div>'+
    '<table style="font-size:12px;width:100%;border-collapse:collapse">'+
    '<tr><td style="padding:3px 0;color:#64748b">State</td><td style="padding:3px 0;font-weight:600"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:'+STATE_COLORS[d.state]+';margin-right:4px"></span>'+d.state+'</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Rules</td><td style="padding:3px 0;font-weight:600">'+d.rules+'</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Confidence</td><td style="padding:3px 0;font-weight:600">'+d.confidence+'</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Nearest Pharmacy</td><td style="padding:3px 0;font-weight:600">'+d.nearPharmKm.toFixed(2)+' km &mdash; '+d.nearPharmName+'</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Nearest Town</td><td style="padding:3px 0;font-weight:600">'+d.nearTown+' (pop '+fmtPop(d.nearTownPop)+', '+d.nearTownDist.toFixed(1)+' km)</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Pop 5/10/15km</td><td style="padding:3px 0;font-weight:600">'+fmtPop(d.pop5)+' / '+fmtPop(d.pop10)+' / '+fmtPop(d.pop15)+'</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Opportunity Score</td><td style="padding:3px 0;font-weight:700;color:#0891b2">'+fmtScore(d.score)+'</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Competition Score</td><td style="padding:3px 0;font-weight:700;color:'+(d.compScore>=70?'#6ee7b7':d.compScore>=40?'#fde68a':'#fca5a5')+'">'+d.compScore.toFixed(1)+'/100</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Pharmacies 5/10/15km</td><td style="padding:3px 0;font-weight:600">'+d.pharm5+' / '+d.pharm10+' / '+d.pharm15+'</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Chains vs Independent (5km)</td><td style="padding:3px 0;font-weight:600">'+d.chains5+' chains / '+d.indep5+' indep</td></tr>'+
    (d.chainNames5&&d.chainNames5!=='None'?'<tr><td style="padding:3px 0;color:#64748b">Chains (5km)</td><td style="padding:3px 0;font-size:11px">'+d.chainNames5+'</td></tr>':'')+
    '<tr><td style="padding:3px 0;color:#64748b">Pop/Pharmacy (5km)</td><td style="padding:3px 0;font-weight:600">'+(d.popPerPharm5>=999999?'No competition':fmtPop(d.popPerPharm5))+'</td></tr>'+
    '<tr><td style="padding:3px 0;color:#64748b">Verification</td><td style="padding:3px 0"><span class="badge '+vBadge+'">'+d.verification+'</span></td></tr>'+
    '</table>'+
    '<div style="margin-top:6px;font-size:11px;color:#64748b;max-width:300px;word-wrap:break-word">'+d.evidence+'</div>'+
    '</div>';
}}

function updateMarkers() {{
  markerCluster.clearLayers();
  const markers = [];
  filteredData.forEach((d,i) => {{
    const m = L.marker([d.lat,d.lng],{{icon:createIcon(d.state,d.verification)}});
    m.bindPopup(makePopup(d),{{maxWidth:350}});
    m.on('click',function(){{highlightRow(i)}});
    d._marker = m;
    markers.push(m);
  }});
  markerCluster.addLayers(markers);
}}

const COLUMNS = [
  {{key:'rank',label:'#',w:'40px',fmt:function(_,i){{return i+1}}}},
  {{key:'state',label:'State',w:'60px',fmt:function(d){{return '<span class="state-dot" style="background:'+STATE_COLORS[d.state]+'"></span>'+d.state}}}},
  {{key:'poiName',label:'Location',w:'180px',fmt:function(d){{return d.poiName||'-'}}}},
  {{key:'rules',label:'Rules',w:'110px'}},
  {{key:'confidence',label:'Conf',w:'50px'}},
  {{key:'nearPharmKm',label:'Pharm Dist',w:'85px',fmt:function(d){{return d.nearPharmKm.toFixed(2)+' km'}}}},
  {{key:'nearTown',label:'Town',w:'110px'}},
  {{key:'pop5',label:'Pop 5km',w:'70px',fmt:function(d){{return '<span class="pop-format">'+fmtPop(d.pop5)+'</span>'}}}},
  {{key:'pop10',label:'Pop 10km',w:'70px',fmt:function(d){{return '<span class="pop-format">'+fmtPop(d.pop10)+'</span>'}}}},
  {{key:'pharm5',label:'Ph 5km',w:'60px'}},
  {{key:'compScore',label:'Comp Score',w:'100px',fmt:function(d){{
    const pct=d.compScore;
    const col=pct>=70?'#6ee7b7':pct>=40?'#fde68a':'#fca5a5';
    return d.compScore.toFixed(0)+' <span class="score-bar"><span class="score-fill" style="width:'+pct+'%;background:'+col+'"></span></span>';
  }}}},
  {{key:'score',label:'Score',w:'110px',fmt:function(d){{
    const pct=Math.min(d.score/500000*100,100);
    const col=pct>66?'#22d3ee':pct>33?'#f59e0b':'#ef4444';
    return fmtScore(d.score)+' <span class="score-bar"><span class="score-fill" style="width:'+pct+'%;background:'+col+'"></span></span>';
  }}}},
  {{key:'verification',label:'Status',w:'100px',fmt:function(d){{
    const c=d.verification==='VERIFIED'?'badge-verified':d.verification==='FALSE POSITIVE'?'badge-false':d.verification==='NEEDS REVIEW'?'badge-review':'badge-unverified';
    return '<span class="badge '+c+'">'+d.verification+'</span>';
  }}}}
];

function buildHead() {{
  const tr = document.getElementById('tableHead');
  tr.innerHTML = '';
  COLUMNS.forEach(function(col) {{
    const th = document.createElement('th');
    th.textContent = col.label;
    th.style.width = col.w||'auto';
    th.dataset.key = col.key;
    if(col.key===sortCol) th.className = sortDir===1?'sorted-asc':'sorted-desc';
    th.onclick = function() {{
      if(sortCol===col.key) sortDir*=-1; else {{sortCol=col.key;sortDir=-1}}
      doSort();
    }};
    tr.appendChild(th);
  }});
}}

function doSort() {{
  filteredData.sort(function(a,b) {{
    let va=a[sortCol],vb=b[sortCol];
    if(typeof va==='string') return sortDir*va.localeCompare(vb);
    return sortDir*((va||0)-(vb||0));
  }});
  updateMarkers();
  renderTable();
  updateStats();
}}

function renderTable() {{
  const tbody = document.getElementById('tableBody');
  let html = '';
  const len = filteredData.length;
  for(let i=0;i<len;i++) {{
    const d = filteredData[i];
    html += '<tr data-idx="'+i+'" onclick="zoomTo('+i+')">';
    for(let j=0;j<COLUMNS.length;j++) {{
      const col = COLUMNS[j];
      const val = col.fmt ? col.fmt(d,i) : (d[col.key]||'');
      html += '<td>'+val+'</td>';
    }}
    html += '</tr>';
  }}
  tbody.innerHTML = html;
  buildHead();
}}

function highlightRow(idx) {{
  document.querySelectorAll('#tableBody tr').forEach(function(r){{r.classList.remove('selected')}});
  const row = document.querySelector('#tableBody tr[data-idx="'+idx+'"]');
  if(row){{row.classList.add('selected');row.scrollIntoView({{behavior:'smooth',block:'center'}})}}
}}

function zoomTo(idx) {{
  const d = filteredData[idx];
  if(!d) return;
  map.setView([d.lat,d.lng],13);
  highlightRow(idx);
  if(d._marker) {{
    markerCluster.zoomToShowLayer(d._marker, function(){{d._marker.openPopup()}});
  }}
}}

function applyFilters() {{
  const states = [];
  document.querySelectorAll('#stateFilters input:checked').forEach(function(cb){{states.push(cb.value)}});
  const verifs = [];
  document.querySelectorAll('#verifyFilters input:checked').forEach(function(cb){{verifs.push(cb.value)}});
  const minPop = parseInt(document.getElementById('popFilter').value);
  const minDist = parseFloat(document.getElementById('distFilter').value);
  const minScore = parseInt(document.getElementById('scoreFilter').value);
  const rule = document.getElementById('ruleFilter').value;
  const search = document.getElementById('searchBox').value.toLowerCase().trim();

  filteredData = RAW_DATA.filter(function(d) {{
    if(states.length && states.indexOf(d.state)===-1) return false;
    if(verifs.length && verifs.indexOf(d.verification)===-1) return false;
    if(d.pop5 < minPop) return false;
    if(d.nearPharmKm < minDist) return false;
    if(d.score < minScore) return false;
    if(rule && d.rules.indexOf(rule)===-1) return false;
    if(search && (d.poiName||'').toLowerCase().indexOf(search)===-1 
       && (d.nearTown||'').toLowerCase().indexOf(search)===-1
       && (d.addr||'').toLowerCase().indexOf(search)===-1
       && (d.nearPharmName||'').toLowerCase().indexOf(search)===-1) return false;
    return true;
  }});
  doSort();
}}

function resetFilters() {{
  document.querySelectorAll('#stateFilters input, #verifyFilters input').forEach(function(cb){{cb.checked=false}});
  document.getElementById('popFilter').value=0;
  document.getElementById('distFilter').value=0;
  document.getElementById('scoreFilter').value=0;
  document.getElementById('ruleFilter').value='';
  document.getElementById('searchBox').value='';
  updateSliderLabels();
  applyFilters();
}}

function updateSliderLabels() {{
  document.getElementById('popVal').textContent=fmtPop(parseInt(document.getElementById('popFilter').value));
  document.getElementById('distVal').textContent=parseFloat(document.getElementById('distFilter').value).toFixed(1);
  document.getElementById('scoreVal').textContent=fmtScore(parseInt(document.getElementById('scoreFilter').value));
}}

function updateStats() {{
  document.getElementById('statTotal').textContent=RAW_DATA.length.toLocaleString();
  document.getElementById('statShown').textContent=filteredData.length.toLocaleString();
  const ss=new Set(filteredData.map(function(d){{return d.state}}));
  document.getElementById('statStates').textContent=ss.size;
  document.getElementById('statVerified').textContent=filteredData.filter(function(d){{return d.verification==='VERIFIED'}}).length.toLocaleString();
}}

function exportCSV() {{
  let csv = 'Rank,State,Location,Rules,Confidence,Pharmacy Distance (km),Nearest Town,Pop 5km,Pop 10km,Pop 15km,Score,Verification,Latitude,Longitude\\n';
  filteredData.forEach(function(d,i) {{
    const row = [i+1,d.state,'"'+(d.poiName||'').replace(/"/g,'""')+'"','"'+d.rules+'"',d.confidence,d.nearPharmKm.toFixed(2),'"'+d.nearTown+'"',d.pop5,d.pop10,d.pop15,d.score.toFixed(1),d.verification,d.lat,d.lng];
    csv += row.join(',')+'\\n';
  }});
  const blob = new Blob([csv],{{type:'text/csv'}});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'pharmacy_opportunities_filtered.csv';
  a.click();
}}

function initFilters() {{
  const stateDiv = document.getElementById('stateFilters');
  ['ACT','NSW','NT','QLD','SA','TAS','VIC','WA'].forEach(function(s) {{
    const count = RAW_DATA.filter(function(d){{return d.state===s}}).length;
    if(count===0) return;
    const label = document.createElement('label');
    label.innerHTML = '<input type="checkbox" value="'+s+'" onchange="applyFilters()"><span class="state-dot" style="background:'+STATE_COLORS[s]+'"></span>'+s+' ('+count+')';
    stateDiv.appendChild(label);
  }});

  const verDiv = document.getElementById('verifyFilters');
  ['VERIFIED','NEEDS REVIEW','FALSE POSITIVE'].forEach(function(v) {{
    const count = RAW_DATA.filter(function(d){{return d.verification===v}}).length;
    const label = document.createElement('label');
    label.innerHTML = '<input type="checkbox" value="'+v+'" onchange="applyFilters()">'+v+' ('+count+')';
    verDiv.appendChild(label);
  }});

  const rules = {{}};
  RAW_DATA.forEach(function(d){{d.rules.split(', ').forEach(function(r){{if(r.trim()) rules[r.trim()]=true}})}});
  const ruleSelect = document.getElementById('ruleFilter');
  Object.keys(rules).sort().forEach(function(r) {{
    const opt = document.createElement('option');
    opt.value = r; opt.textContent = r;
    ruleSelect.appendChild(opt);
  }});

  const legend = document.getElementById('legend');
  Object.keys(STATE_COLORS).sort().forEach(function(s) {{
    legend.innerHTML += '<div class="legend-item"><div class="legend-dot" style="background:'+STATE_COLORS[s]+'"></div>'+s+'</div>';
  }});

  updateSliderLabels();
}}

function initResize() {{
  const bar = document.getElementById('resizeBar');
  const table = document.getElementById('tableContainer');
  let startY, startH;
  bar.addEventListener('mousedown', function(e) {{
    startY=e.clientY; startH=table.offsetHeight;
    function onMove(ev) {{
      const diff=startY-ev.clientY;
      const newH=Math.max(100,Math.min(startH+diff,window.innerHeight-200));
      table.style.height=newH+'px';
      map.invalidateSize();
    }}
    function onUp() {{document.removeEventListener('mousemove',onMove);document.removeEventListener('mouseup',onUp)}}
    document.addEventListener('mousemove',onMove);
    document.addEventListener('mouseup',onUp);
  }});
}}

window.addEventListener('DOMContentLoaded', function() {{
  initMap();
  initFilters();
  initResize();
  filteredData = RAW_DATA.slice();
  sortCol='score'; sortDir=-1;
  doSort();
}});
</script>
</body>
</html>'''


def main():
    print("Building PharmacyFinder Dashboard...")
    data = load_data()
    print(f"  Loaded {len(data)} opportunities from {len(set(d['state'] for d in data))} states")

    html = build_html(data)
    out_path = os.path.join(OUTPUT_DIR, 'dashboard.html')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)

    size_kb = os.path.getsize(out_path) / 1024
    print(f"  Dashboard written to {out_path} ({size_kb:.0f} KB)")
    print("  Done!")


if __name__ == '__main__':
    main()
