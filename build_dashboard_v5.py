#!/usr/bin/env python3
"""
PharmacyFinder Dashboard v5 — Dark theme, filtered, scored.
Filters out FAIL/NOT_VIABLE_REMOTE/DUPLICATE. Shows VERIFIED + LIKELY.
Sorts by composite_score v3. Color-coded markers. Dark Leaflet map.
"""

import sqlite3, os, json, html

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'dashboard.html')
os.makedirs(OUTPUT_DIR, exist_ok=True)

EXCLUDE = ['not_viable_remote','duplicate','fail_pharmacy_exists','false_positive',
           'false positive','pharmacy exists','below_threshold','invalid']

def query_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM opportunities ORDER BY composite_score DESC")
    all_opps = [dict(row) for row in cur.fetchall()]
    opps = []
    for o in all_opps:
        ver = (o.get('verification') or '').strip().lower()
        if any(ex in ver for ex in EXCLUDE):
            continue
        if (o.get('composite_score') or 0) <= 0:
            continue
        pop = o.get('pop_10km') or 0
        pharm = o.get('pharmacy_10km') or 0
        o['people_per_pharmacy'] = round(pop / pharm, 1) if pharm > 0 else pop
        o['town'] = o.get('nearest_town') or o.get('address') or 'Unknown'
        o['state'] = o.get('region') or ''
        o['score'] = round(o.get('composite_score') or 0, 1)
        v = (o.get('verification') or '').upper()
        o['status'] = 'VERIFIED' if ('VERIFIED' in v and 'UN' not in v) else 'LIKELY'
        opps.append(o)
    opps.sort(key=lambda x: -x['score'])

    cur.execute("SELECT id,name,latitude,longitude,state,suburb FROM pharmacies WHERE latitude IS NOT NULL")
    pharmacies = [dict(row) for row in cur.fetchall()]
    cur.execute("SELECT COUNT(*) as cnt FROM gps")
    gp_count = cur.fetchone()['cnt']
    conn.close()
    return opps, pharmacies, gp_count

def build_html(opps, pharmacies, gp_count):
    opp_json = []
    for i, o in enumerate(opps):
        opp_json.append({
            'rank': i+1, 'id': o.get('id'), 'town': o['town'], 'state': o['state'],
            'pop_5km': o.get('pop_5km') or 0, 'pop_10km': o.get('pop_10km') or 0,
            'pharmacy_10km': o.get('pharmacy_10km') or 0,
            'ppp': o['people_per_pharmacy'],
            'nearest_km': round(o.get('nearest_pharmacy_km') or 0, 1),
            'nearest_name': o.get('nearest_pharmacy_name') or '',
            'rules': o.get('qualifying_rules') or '',
            'status': o['status'], 'score': o['score'],
            'lat': o.get('latitude'), 'lng': o.get('longitude'),
            'poi': o.get('poi_name') or '',
            'evidence': (o.get('evidence') or '')[:200],
        })
    pharm_json = [{'name':p['name'],'lat':p['latitude'],'lng':p['longitude'],'state':p.get('state') or ''} for p in pharmacies]

    verified_count = sum(1 for o in opp_json if o['status']=='VERIFIED')
    likely_count = len(opp_json) - verified_count
    states = sorted(set(o['state'] for o in opp_json if o['state']))
    states_opts = ''.join(f'<option value="{html.escape(s)}">{html.escape(s)}</option>' for s in states)

    # Build HTML with JS as raw string (no f-string for JS block)
    css = """
* {margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0d1117;color:#c9d1d9;overflow:hidden}
.container{display:flex;height:100vh}
.sidebar{width:42%;min-width:500px;background:#161b22;display:flex;flex-direction:column;border-right:1px solid #30363d}
.header{padding:16px 20px 14px;border-bottom:1px solid #30363d;flex-shrink:0}
.header h1{font-size:18px;font-weight:700;color:#f0f6fc;margin-bottom:10px}
.header h1 .accent{color:#3fb950} .header h1 .v{color:#6e7681;font-size:13px;font-weight:400}
.stats{display:flex;gap:16px;margin-bottom:12px;flex-wrap:wrap}
.stat{text-align:center;min-width:70px}
.stat-val{font-size:20px;font-weight:700;color:#f0f6fc}
.stat-val.green{color:#3fb950} .stat-val.yellow{color:#d29922}
.stat-label{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:.5px}
.controls{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.controls input,.controls select{background:#0d1117;border:1px solid #30363d;color:#c9d1d9;padding:6px 10px;border-radius:6px;font-size:13px;outline:none}
.controls input:focus,.controls select:focus{border-color:#3fb950}
.controls input{flex:1;min-width:120px}
.btn{background:#238636;color:#fff;border:none;padding:6px 14px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer}
.btn:hover{background:#2ea043}
.table-wrap{flex:1;overflow-y:auto}
.table-wrap::-webkit-scrollbar{width:6px}
.table-wrap::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
table{width:100%;border-collapse:collapse;font-size:12px}
thead{position:sticky;top:0;z-index:2}
thead th{background:#0d1117;color:#8b949e;font-weight:600;text-transform:uppercase;font-size:10px;letter-spacing:.5px;padding:8px 6px;text-align:left;cursor:pointer;user-select:none;white-space:nowrap;border-bottom:2px solid #21262d}
thead th:hover{color:#3fb950}
thead th.sort-asc::after{content:" ▲";color:#3fb950}
thead th.sort-desc::after{content:" ▼";color:#3fb950}
tbody tr{border-bottom:1px solid #21262d;cursor:pointer;transition:background .1s}
tbody tr:hover{background:#1c2128}
tbody tr.active{background:#1f3320!important}
td{padding:5px 6px;white-space:nowrap}
td.rank{color:#484f58;font-size:11px;text-align:center;width:28px}
td.name{color:#f0f6fc;font-weight:500;max-width:170px;overflow:hidden;text-overflow:ellipsis}
td.score-cell{font-weight:700;font-size:13px}
.badge{display:inline-block;padding:2px 7px;border-radius:10px;font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.3px}
.badge-green{background:rgba(63,185,80,.15);color:#3fb950}
.badge-yellow{background:rgba(210,153,34,.15);color:#d29922}
.score-high{color:#3fb950} .score-mid{color:#d29922} .score-low{color:#f85149}
.map-wrap{width:58%;position:relative}
#map{width:100%;height:100%}
.leaflet-popup-content-wrapper{background:#161b22;color:#c9d1d9;border-radius:8px;border:1px solid #30363d}
.leaflet-popup-tip{background:#161b22}
.leaflet-popup-content{font-size:12px;line-height:1.5}
.leaflet-popup-content b{color:#3fb950}
.leaflet-popup-content .label{color:#8b949e;font-size:10px;text-transform:uppercase}
.marker-cluster-small{background-color:rgba(139,148,158,.2)}
.marker-cluster-small div{background-color:rgba(139,148,158,.5);color:#fff;font-size:12px}
.marker-cluster-medium{background-color:rgba(139,148,158,.25)}
.marker-cluster-medium div{background-color:rgba(139,148,158,.55);color:#fff}
.marker-cluster-large{background-color:rgba(139,148,158,.3)}
.marker-cluster-large div{background-color:rgba(139,148,158,.6);color:#fff}
.info-badge{position:absolute;bottom:8px;right:8px;background:rgba(0,0,0,.8);color:#8b949e;font-size:11px;padding:4px 10px;border-radius:6px;z-index:1000}
"""

    js = """
const OPPS = __OPPS__;
const PHARMS = __PHARMS__;

let filtered = [...OPPS];
let sortCol = 'score';
let sortDir = 'desc';
let activeId = null;
let markers = {};
let highlight = null;

const map = L.map('map',{zoomControl:true}).setView([-28,134],5);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',{
    attribution:'OSM/CARTO',maxZoom:19
}).addTo(map);

const pharmCluster = L.markerClusterGroup({
    maxClusterRadius:40,showCoverageOnHover:false,spiderfyOnMaxZoom:true,
    iconCreateFunction:function(c){return L.divIcon({
        html:'<div>'+c.getChildCount()+'</div>',
        className:'marker-cluster marker-cluster-'+(c.getChildCount()<10?'small':c.getChildCount()<50?'medium':'large'),
        iconSize:[30,30]
    })}
});
PHARMS.forEach(function(p){
    var m=L.circleMarker([p.lat,p.lng],{radius:3,color:'#484f58',fillColor:'#484f58',fillOpacity:0.5,weight:1});
    m.bindPopup('<b>'+p.name+'</b><br>'+p.state);
    pharmCluster.addLayer(m);
});
map.addLayer(pharmCluster);

function createOppMarker(o){
    var color=o.status==='VERIFIED'?'#3fb950':'#d29922';
    var m=L.circleMarker([o.lat,o.lng],{
        radius:Math.max(6,Math.min(12,o.score/8)),
        color:color,fillColor:color,fillOpacity:0.7,weight:2
    });
    m.bindPopup(
        '<b>'+(o.poi||o.town)+'</b><br>'+
        '<span class="label">Town:</span> '+o.town+' ('+o.state+')<br>'+
        '<span class="label">Score:</span> <b>'+o.score+'</b><br>'+
        '<span class="label">Pop 10km:</span> '+(o.pop_10km||0).toLocaleString()+'<br>'+
        '<span class="label">Nearest:</span> '+o.nearest_km+'km ('+o.nearest_name+')<br>'+
        '<span class="label">Rules:</span> '+o.rules+'<br>'+
        '<span class="label">Status:</span> '+o.status,
        {maxWidth:300}
    );
    m.on('click',function(){selectOpp(o.id)});
    return m;
}

var oppGroup=L.layerGroup().addTo(map);
function renderMarkers(){
    oppGroup.clearLayers();
    markers={};
    filtered.forEach(function(o){
        if(!o.lat||!o.lng)return;
        var m=createOppMarker(o);
        oppGroup.addLayer(m);
        markers[o.id]=m;
    });
}

function selectOpp(id){
    activeId=id;
    document.querySelectorAll('#tbody tr').forEach(function(tr){
        tr.classList.toggle('active',parseInt(tr.dataset.id)===id);
    });
    var o=OPPS.find(function(x){return x.id===id});
    if(o&&o.lat&&o.lng){
        map.setView([o.lat,o.lng],12);
        if(highlight)map.removeLayer(highlight);
        highlight=L.circle([o.lat,o.lng],{radius:10000,color:'#3fb950',fillColor:'#3fb950',fillOpacity:0.08,weight:1}).addTo(map);
        if(markers[id])markers[id].openPopup();
    }
}

function renderTable(){
    var tbody=document.getElementById('tbody');
    tbody.innerHTML='';
    filtered.forEach(function(o){
        var sc=o.score>=50?'score-high':o.score>=30?'score-mid':'score-low';
        var badge=o.status==='VERIFIED'?'<span class="badge badge-green">VERIFIED</span>':'<span class="badge badge-yellow">LIKELY</span>';
        var tr=document.createElement('tr');
        tr.dataset.id=o.id;
        if(o.id===activeId)tr.classList.add('active');
        tr.innerHTML=
            '<td class="rank">'+o.rank+'</td>'+
            '<td class="name" title="'+(o.poi||o.town)+'">'+(o.poi||o.town)+'</td>'+
            '<td>'+o.state+'</td>'+
            '<td class="score-cell '+sc+'">'+o.score+'</td>'+
            '<td>'+(o.pop_10km||0).toLocaleString()+'</td>'+
            '<td>'+o.nearest_km+'</td>'+
            '<td style="color:#8b949e;font-size:11px" title="'+o.rules+'">'+o.rules.substring(0,20)+'</td>'+
            '<td>'+badge+'</td>';
        tr.onclick=function(){selectOpp(o.id)};
        tbody.appendChild(tr);
    });
    document.getElementById('infoBadge').textContent=filtered.length+' opportunities shown';
}

document.querySelectorAll('thead th').forEach(function(th){
    th.addEventListener('click',function(){
        var col=th.dataset.col;
        var type=th.dataset.type;
        if(sortCol===col)sortDir=sortDir==='asc'?'desc':'asc';
        else{sortCol=col;sortDir=type==='num'?'desc':'asc';}
        document.querySelectorAll('thead th').forEach(function(t){t.classList.remove('sort-asc','sort-desc')});
        th.classList.add('sort-'+sortDir);
        applySort();renderTable();
    });
});

function applySort(){
    filtered.sort(function(a,b){
        var va=a[sortCol],vb=b[sortCol];
        if(sortCol==='name'){va=a.poi||a.town;vb=b.poi||b.town;}
        if(typeof va==='string'){va=va.toLowerCase();vb=(vb||'').toLowerCase();}
        if(va<vb)return sortDir==='asc'?-1:1;
        if(va>vb)return sortDir==='asc'?1:-1;
        return 0;
    });
    filtered.forEach(function(o,i){o.rank=i+1});
}

function applyFilters(){
    var q=document.getElementById('searchBox').value.toLowerCase();
    var st=document.getElementById('stateFilter').value;
    var status=document.getElementById('statusFilter').value;
    filtered=OPPS.filter(function(o){
        if(q&&!(o.town.toLowerCase().includes(q)||(o.poi||'').toLowerCase().includes(q)))return false;
        if(st&&o.state!==st)return false;
        if(status&&o.status!==status)return false;
        return true;
    });
    applySort();renderTable();renderMarkers();
}

document.getElementById('searchBox').addEventListener('input',applyFilters);
document.getElementById('stateFilter').addEventListener('change',applyFilters);
document.getElementById('statusFilter').addEventListener('change',applyFilters);

function exportCSV(){
    var csv='Rank,Location,Town,State,Score,Pop_10km,Pharmacies_10km,Nearest_km,Rules,Status\\n';
    filtered.forEach(function(o){
        csv+=[o.rank,'"'+(o.poi||o.town).replace(/"/g,'""')+'"','"'+o.town.replace(/"/g,'""')+'"',
              o.state,o.score,o.pop_10km,o.pharmacy_10km,o.nearest_km,
              '"'+o.rules.replace(/"/g,'""')+'"',o.status].join(',')+  '\\n';
    });
    var blob=new Blob([csv],{type:'text/csv'});
    var a=document.createElement('a');
    a.href=URL.createObjectURL(blob);
    a.download='pharmacy_opportunities.csv';
    a.click();
}

applySort();renderTable();renderMarkers();
document.querySelector('thead th[data-col="score"]').classList.add('sort-desc');
"""

    js = js.replace('__OPPS__', json.dumps(opp_json))
    js = js.replace('__PHARMS__', json.dumps(pharm_json))

    page = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PharmacyFinder Dashboard v5</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>
<style>{css}</style>
</head>
<body>
<div class="container">
  <div class="sidebar">
    <div class="header">
      <h1>🏥 PharmacyFinder <span class="accent">Dashboard</span> <span class="v">v5</span></h1>
      <div class="stats">
        <div class="stat"><div class="stat-val green">{verified_count}</div><div class="stat-label">Verified</div></div>
        <div class="stat"><div class="stat-val yellow">{likely_count}</div><div class="stat-label">Likely</div></div>
        <div class="stat"><div class="stat-val">{len(pharmacies):,}</div><div class="stat-label">Pharmacies</div></div>
        <div class="stat"><div class="stat-val">{gp_count:,}</div><div class="stat-label">GPs</div></div>
      </div>
      <div class="controls">
        <input type="text" id="searchBox" placeholder="Search town or POI...">
        <select id="stateFilter"><option value="">All States</option>{states_opts}</select>
        <select id="statusFilter"><option value="">All Status</option><option value="VERIFIED">Verified</option><option value="LIKELY">Likely</option></select>
        <button class="btn" onclick="exportCSV()">CSV</button>
      </div>
    </div>
    <div class="table-wrap" id="tableWrap">
      <table>
        <thead><tr>
          <th data-col="rank" data-type="num">#</th>
          <th data-col="name" data-type="str">Location</th>
          <th data-col="state" data-type="str">ST</th>
          <th data-col="score" data-type="num">Score</th>
          <th data-col="pop_10km" data-type="num">Pop 10km</th>
          <th data-col="nearest_km" data-type="num">Near km</th>
          <th data-col="rules" data-type="str">Rules</th>
          <th data-col="status" data-type="str">Status</th>
        </tr></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>
  <div class="map-wrap">
    <div id="map"></div>
    <div class="info-badge" id="infoBadge"></div>
  </div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<script>{js}</script>
</body>
</html>'''
    return page

def main():
    print("Building dashboard v5...")
    opps, pharmacies, gp_count = query_data()
    print(f"  {len(opps)} opportunities (filtered)")
    print(f"  {len(pharmacies)} pharmacies")
    print(f"  {gp_count} GPs")
    h = build_html(opps, pharmacies, gp_count)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(h)
    print(f"  Dashboard saved to {OUTPUT_FILE}")
    print(f"  File size: {os.path.getsize(OUTPUT_FILE)/1024:.0f} KB")

if __name__ == '__main__':
    main()
