#!/usr/bin/env python3
"""
PharmacyFinder Dashboard v3 — Full Rebuild

Generates output/dashboard.html showing:
- Map with 5,322 pharmacies (clustered) + opportunities colour-coded by verdict
- Left panel: sortable/filterable opportunity cards
- Right panel: detail view with ALL 7 rules (pass/fail/unverified per rule)
- Filter by rule, state, verdict
- Export to CSV
"""

import sqlite3, json, os, math, sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'output')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'dashboard.html')
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_pharmacies():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT name, latitude, longitude, state, suburb FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    rows = c.fetchall()
    conn.close()
    return [{'name': r[0], 'lat': r[1], 'lng': r[2], 'state': r[3] or '', 'suburb': r[4] or ''} for r in rows]


def generate_html(opportunities, pharmacies):
    """Generate complete self-contained HTML dashboard."""
    
    # Prepare JSON data
    opp_data = []
    for o in opportunities:
        rules_summary = {}
        rc = o.get('rules_checked', {})
        for rule_key, rule_data in rc.items():
            label = rule_key.replace('item_', 'Item ').replace('134a', '134A')
            rules_summary[rule_key] = {
                'label': label,
                'verdict': rule_data.get('verdict', 'FAIL'),
                'reason': rule_data.get('reason', ''),
                'checks': rule_data.get('checks', {})
            }
        
        opp_data.append({
            'id': o['id'],
            'name': o['name'],
            'state': o['state'],
            'lat': o['lat'],
            'lng': o['lng'],
            'address': o.get('address', ''),
            'poi_type': o.get('poi_type', ''),
            'pop_5km': o.get('pop_5km', 0),
            'pop_10km': o.get('pop_10km', 0),
            'pop_15km': o.get('pop_15km', 0),
            'pharmacy_5km': o.get('pharmacy_5km', 0),
            'pharmacy_10km': o.get('pharmacy_10km', 0),
            'pharmacy_15km': o.get('pharmacy_15km', 0),
            'nearest_pharmacy_km': o.get('nearest_pharmacy_km', 999),
            'nearest_pharmacy': o.get('nearest_pharmacy', ''),
            'nearest_town': o.get('nearest_town', ''),
            'evidence': o.get('evidence', ''),
            'growth_indicator': o.get('growth_indicator', ''),
            'growth_details': o.get('growth_details', ''),
            'geocoding_flag': o.get('geocoding_flag'),
            'verdict': o.get('verdict', 'FAIL'),
            'score': o.get('score', 0),
            'best_rule': o.get('best_rule_display', '') or o.get('best_rule', ''),
            'status_summary': o.get('status_summary', ''),
            'auto_pass': o.get('auto_pass', 0),
            'auto_fail': o.get('auto_fail', 0),
            'manual_checks': o.get('manual_checks', 0),
            'rules_summary': rules_summary,
            'original_rules': o.get('original_rules', ''),
            'ratio': o.get('ratio', 0),
        })
    
    opp_json = json.dumps(opp_data, ensure_ascii=False, default=str)
    pharm_data = [[p['lat'], p['lng'], p['name'][:30], p['state']] for p in pharmacies]
    pharm_json = json.dumps(pharm_data, ensure_ascii=False)
    
    states = sorted(set(o['state'] for o in opp_data if o['state']))
    n_pass = sum(1 for o in opp_data if o['verdict'] == 'PASS')
    n_likely = sum(1 for o in opp_data if o['verdict'] == 'LIKELY')
    n_fail = sum(1 for o in opp_data if o['verdict'] == 'FAIL')
    n_total = len(opp_data)
    
    states_options = ''.join('<option value="' + s + '">' + s + '</option>' for s in states)
    
    # Build HTML using string concatenation (NOT f-strings) to avoid JS brace issues
    parts = []
    
    # ---- HEAD ----
    parts.append('''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PharmacyFinder — Greenfield Opportunity Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f1117;color:#e2e8f0;overflow:hidden;height:100vh}
.topbar{background:linear-gradient(135deg,#0f1117,#1a1d2e,#1e2235);border-bottom:1px solid #2a2f45;padding:0 16px;height:50px;display:flex;align-items:center;gap:16px;position:fixed;top:0;left:0;right:0;z-index:1000}
.logo{font-size:17px;font-weight:700;white-space:nowrap}.logo span{color:#22c55e}
.stats{display:flex;gap:14px;font-size:11px;color:#94a3b8;flex-shrink:0}.stats b{font-weight:600}
.filters{display:flex;gap:8px;align-items:center;flex:1;overflow-x:auto;padding:4px 0}
.fg{display:flex;align-items:center;gap:3px;flex-shrink:0}
.fg label{font-size:9px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap}
.fg select{background:#1a1d2e;color:#e2e8f0;border:1px solid #2a2f45;border-radius:5px;font-size:11px;padding:3px 6px;outline:none}
.fg select:focus{border-color:#22c55e}
.fg input[type=text]{background:#1a1d2e;color:#e2e8f0;border:1px solid #2a2f45;border-radius:5px;font-size:11px;padding:3px 8px;width:120px;outline:none}
.btn-sm{background:#2a2f45;color:#94a3b8;border:1px solid #3a3f55;border-radius:5px;font-size:10px;padding:3px 8px;cursor:pointer;white-space:nowrap}
.btn-sm:hover{background:#3a3f55;color:#e2e8f0}
.container{display:flex;margin-top:50px;height:calc(100vh - 50px)}
.left-panel{width:300px;min-width:300px;background:#13151f;border-right:1px solid #2a2f45;display:flex;flex-direction:column;overflow:hidden}
.list-hdr{padding:8px 12px;border-bottom:1px solid #2a2f45;display:flex;justify-content:space-between;align-items:center;background:#181b28;flex-shrink:0}
.list-hdr h3{font-size:11px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px}
.list-cnt{font-size:11px;color:#22c55e;font-weight:600}
.card-list{flex:1;overflow-y:auto;padding:4px}
.card-list::-webkit-scrollbar{width:5px}.card-list::-webkit-scrollbar-track{background:#13151f}.card-list::-webkit-scrollbar-thumb{background:#2a2f45;border-radius:3px}
.opp-card{background:#1a1d2e;border-radius:7px;padding:8px 10px;margin-bottom:4px;cursor:pointer;transition:all .12s;border-left:3px solid transparent;position:relative}
.opp-card:hover{background:#1e2235;transform:translateX(2px)}
.opp-card.selected{background:#1e2840;border-left-color:#22c55e;box-shadow:0 0 10px rgba(34,197,94,.12)}
.opp-card.v-PASS{border-left-color:#22c55e}.opp-card.v-LIKELY{border-left-color:#3b82f6}.opp-card.v-FAIL{border-left-color:#ef4444}
.c-rank{position:absolute;top:6px;right:8px;font-size:9px;color:#475569;font-weight:700}
.c-name{font-size:12px;font-weight:600;color:#e2e8f0;margin-bottom:1px;padding-right:28px;line-height:1.3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.c-sub{font-size:9px;color:#64748b;margin-bottom:3px}
.c-badge{display:inline-block;font-size:8px;font-weight:700;padding:1px 6px;border-radius:8px;margin-right:4px}
.c-badge.v-PASS{background:rgba(34,197,94,.15);color:#22c55e}.c-badge.v-LIKELY{background:rgba(59,130,246,.15);color:#3b82f6}.c-badge.v-FAIL{background:rgba(239,68,68,.15);color:#ef4444}
.c-chk{font-size:9px;color:#64748b}.c-chk .p{color:#22c55e}.c-chk .f{color:#ef4444}.c-chk .u{color:#f59e0b}
.c-meta{display:flex;gap:10px;font-size:9px;color:#475569;margin-top:2px}.c-meta b{color:#94a3b8}
.c-rule{font-size:8px;background:#2a2f45;color:#94a3b8;padding:1px 5px;border-radius:8px;margin-top:2px;display:inline-block}
.list-foot{padding:6px 8px;border-top:1px solid #2a2f45;flex-shrink:0}
.btn-export{background:#22c55e;color:#0f1117;border:none;border-radius:6px;font-size:11px;font-weight:600;padding:7px;cursor:pointer;width:100%}.btn-export:hover{background:#16a34a}
.map-area{flex:1;position:relative}
#map{width:100%;height:100%}
.layer-ctl{position:absolute;top:10px;left:10px;background:#1a1d2eee;border:1px solid #2a2f45;border-radius:7px;padding:6px 10px;z-index:800;font-size:10px}
.layer-ctl label{display:flex;align-items:center;gap:5px;cursor:pointer;color:#94a3b8;padding:1px 0}.layer-ctl label:hover{color:#e2e8f0}
.layer-ctl input{accent-color:#22c55e}
.right-panel{width:0;overflow:hidden;background:#13151f;border-left:1px solid #2a2f45;transition:width .2s ease;flex-shrink:0}
.right-panel.open{width:400px}
.detail{width:400px;height:100%;overflow-y:auto;padding:0}
.detail::-webkit-scrollbar{width:5px}.detail::-webkit-scrollbar-track{background:#13151f}.detail::-webkit-scrollbar-thumb{background:#2a2f45;border-radius:3px}
.d-header{background:linear-gradient(135deg,#1a1d2e,#1e2235);padding:14px 16px;border-bottom:1px solid #2a2f45;position:relative}
.d-close{position:absolute;top:10px;right:12px;background:none;border:none;color:#64748b;font-size:18px;cursor:pointer;padding:4px}.d-close:hover{color:#e2e8f0}
.d-name{font-size:15px;font-weight:700;color:#e2e8f0;padding-right:28px;margin-bottom:2px}
.d-addr{font-size:10px;color:#64748b;margin-bottom:8px}
.d-verdict{font-size:28px;font-weight:800;line-height:1;margin-bottom:3px}
.d-verdict.v-PASS{color:#22c55e}.d-verdict.v-LIKELY{color:#3b82f6}.d-verdict.v-FAIL{color:#ef4444}
.d-sub{font-size:10px;color:#94a3b8}
.d-section{padding:10px 16px;border-bottom:1px solid #1e2235}
.d-section h4{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px;font-weight:600}
.d-grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px}
.d-stat{text-align:center}.d-stat .v{font-size:15px;font-weight:700;color:#e2e8f0}.d-stat .l{font-size:8px;color:#64748b;text-transform:uppercase}
.rule-card{background:#1a1d2e;border-radius:7px;margin-bottom:6px;overflow:hidden;border-left:3px solid #2a2f45}
.rule-card.rv-PASS{border-left-color:#22c55e}.rule-card.rv-UNVERIFIED{border-left-color:#f59e0b}.rule-card.rv-FAIL{border-left-color:#ef4444}
.rule-card-hdr{padding:8px 10px;display:flex;justify-content:space-between;align-items:center;cursor:pointer;user-select:none}
.rule-card-hdr:hover{background:#1e2235}
.rc-label{font-size:12px;font-weight:600;color:#e2e8f0}
.rc-verdict{font-size:10px;font-weight:700;padding:2px 8px;border-radius:8px}
.rc-verdict.rv-PASS{background:rgba(34,197,94,.15);color:#22c55e}.rc-verdict.rv-UNVERIFIED{background:rgba(245,158,11,.15);color:#f59e0b}.rc-verdict.rv-FAIL{background:rgba(239,68,68,.15);color:#ef4444}
.rule-card-body{padding:0 10px 8px;display:none}
.rule-card.open .rule-card-body{display:block}
.chk-row{padding:4px 0;border-bottom:1px solid rgba(42,47,69,.5);font-size:10px;display:flex;gap:6px;align-items:flex-start}
.chk-row:last-child{border-bottom:none}
.chk-icon{font-size:13px;flex-shrink:0;line-height:1.1}
.chk-text{flex:1;color:#94a3b8;line-height:1.4}.chk-text b{color:#e2e8f0;font-weight:600}
.nearest-box{background:#1a1d2e;border-radius:7px;padding:8px 10px;margin-top:6px;display:flex;align-items:center;gap:8px}
.nearest-box .ico{width:28px;height:28px;background:#2a2f45;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;flex-shrink:0}
.nearest-box .inf{flex:1}.nearest-box .inf .n{font-size:11px;font-weight:600;color:#e2e8f0}.nearest-box .inf .d{font-size:10px;color:#94a3b8}
.d-actions{padding:10px 16px}
.btn-gm{display:block;text-align:center;padding:7px;border-radius:7px;font-size:11px;font-weight:600;text-decoration:none;background:#1a1d2e;color:#4285f4;border:1px solid #2a2f45;margin-bottom:6px}.btn-gm:hover{background:#1e2235}
.geo-flag{background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);border-radius:6px;padding:6px 10px;font-size:10px;color:#f87171;margin-top:6px}
.leaflet-control-zoom a{background:#1a1d2e!important;color:#e2e8f0!important;border-color:#2a2f45!important}.leaflet-control-zoom a:hover{background:#2a2f45!important}
.leaflet-control-attribution{background:rgba(15,17,23,.8)!important;color:#64748b!important;font-size:8px!important}.leaflet-control-attribution a{color:#64748b!important}
.leaflet-popup-content-wrapper{background:#1a1d2e;color:#e2e8f0;border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,.5)}.leaflet-popup-tip{background:#1a1d2e}.leaflet-popup-close-button{color:#64748b!important}
.radius-label{background:rgba(26,29,46,.85)!important;border:1px solid #2a2f45!important;color:#e2e8f0!important;font-size:9px!important;font-weight:600!important;padding:2px 6px!important;border-radius:4px!important;box-shadow:0 2px 8px rgba(0,0,0,.4)!important;white-space:nowrap!important}
.radius-label::before{display:none!important}
</style>
</head>
<body>
''')
    
    # ---- TOPBAR (uses Python string formatting for dynamic values only) ----
    parts.append('<div class="topbar">')
    parts.append('<div class="logo">\U0001F48A <span>Pharmacy</span>Finder</div>')
    parts.append('<div class="stats">')
    parts.append('<span>Total: <b id="sT">' + str(n_total) + '</b></span>')
    parts.append('<span style="color:#22c55e">\u2705 <b id="sP">' + str(n_pass) + '</b></span>')
    parts.append('<span style="color:#3b82f6">\u2753 <b id="sL">' + str(n_likely) + '</b></span>')
    parts.append('<span style="color:#ef4444">\u274c <b id="sF">' + str(n_fail) + '</b></span>')
    parts.append('<span>Showing: <b id="sS">' + str(n_total) + '</b></span>')
    parts.append('</div>')
    parts.append('<div class="filters">')
    parts.append('<div class="fg"><label>Verdict</label><select id="fV"><option value="ALL">All</option><option value="PASS">PASS</option><option value="LIKELY">LIKELY</option><option value="FAIL">FAIL</option></select></div>')
    parts.append('<div class="fg"><label>State</label><select id="fSt"><option value="ALL">All</option>' + states_options + '</select></div>')
    parts.append('<div class="fg"><label>Best Rule</label><select id="fR"><option value="ALL">All</option><option value="item_131">131</option><option value="item_132">132</option><option value="item_133">133</option><option value="item_134">134</option><option value="item_134a">134A</option><option value="item_135">135</option><option value="item_136">136</option></select></div>')
    parts.append('<div class="fg"><label>Sort</label><select id="fSort"><option value="score">Score</option><option value="nearest">Nearest Ph</option><option value="pop">Population</option><option value="name">Name</option></select></div>')
    parts.append('<div class="fg"><label>Search</label><input type="text" id="fSearch" placeholder="Name or address..."/></div>')
    parts.append('<button class="btn-sm" onclick="resetFilters()">\U0001F504 Reset</button>')
    parts.append('</div></div>')
    
    # ---- MAIN LAYOUT ----
    parts.append('''
<div class="container">
  <div class="left-panel">
    <div class="list-hdr"><h3>Opportunities</h3><span class="list-cnt" id="lCnt">''' + str(n_total) + '''</span></div>
    <div class="card-list" id="cList"></div>
    <div class="list-foot"><button class="btn-export" onclick="exportCSV()">\U0001F4E5 Export CSV</button></div>
  </div>
  <div class="map-area">
    <div id="map"></div>
    <div class="layer-ctl">
      <label><input type="checkbox" id="togPh" checked/> Pharmacies</label>
      <label><input type="checkbox" id="togFail"/> Show FAIL</label>
    </div>
  </div>
  <div class="right-panel" id="rPanel">
    <div class="detail" id="detailInner"></div>
  </div>
</div>
''')
    
    # ---- SCRIPT: data injection ----
    parts.append('<script>\n')
    parts.append('var OPPS=' + opp_json + ';\n')
    parts.append('var PH=' + pharm_json + ';\n')
    
    # ---- SCRIPT: JS code (raw string, no f-string processing) ----
    parts.append(r'''
var RULE_NAMES={item_131:'Item 131 \u2014 10km Road',item_132:'Item 132 \u2014 Same Town',item_133:'Item 133 \u2014 Small Shopping Centre',item_134:'Item 134 \u2014 Large Shopping Centre',item_134a:'Item 134A \u2014 Large SC Additional',item_135:'Item 135 \u2014 Private Hospital',item_136:'Item 136 \u2014 Large Medical Centre'};
var map,phCluster,markers={},selId=null,filtered=OPPS.slice(),showFail=false,radiusCircles=[];

function vc(v){return v==='PASS'?'#22c55e':v==='LIKELY'?'#3b82f6':'#ef4444'}
function fmt(n){return n!=null?n.toLocaleString():'0'}
function ms(s){return Math.max(7,Math.min(18,5+s*0.13))}

function initMap(){
  map=L.map('map',{zoomControl:true}).setView([-25.5,134],5);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',{attribution:'&copy; OSM &copy; CARTO',maxZoom:19}).addTo(map);
  phCluster=L.markerClusterGroup({maxClusterRadius:50,disableClusteringAtZoom:14,showCoverageOnHover:false,
    iconCreateFunction:function(c){var n=c.getChildCount();var s=n>100?34:n>30?28:22;return L.divIcon({html:'<div style="width:'+s+'px;height:'+s+'px;line-height:'+s+'px;text-align:center;border-radius:50%;background:rgba(100,116,139,.5);color:#cbd5e1;font-size:9px;font-weight:700">'+n+'</div>',className:'',iconSize:L.point(s+6,s+6)});}
  });
  PH.forEach(function(p){if(!p[0]||!p[1])return;var m=L.circleMarker([p[0],p[1]],{radius:2.5,fillColor:'#64748b',color:'#475569',weight:0.4,opacity:0.5,fillOpacity:0.4});m.bindPopup('<b style="font-size:11px">'+p[2]+'</b><br><span style="color:#94a3b8;font-size:10px">'+p[3]+'</span>',{maxWidth:200});phCluster.addLayer(m)});
  map.addLayer(phCluster);
  document.getElementById('togPh').addEventListener('change',function(){if(this.checked)map.addLayer(phCluster);else map.removeLayer(phCluster)});
  document.getElementById('togFail').addEventListener('change',function(){showFail=this.checked;applyFilters()});
}

function addMarkers(){
  Object.values(markers).forEach(function(m){map.removeLayer(m)});markers={};
  filtered.forEach(function(o){
    if(!showFail&&o.verdict==='FAIL')return;
    var c=vc(o.verdict),s=ms(o.score);
    var m=L.circleMarker([o.lat,o.lng],{radius:s,fillColor:c,color:'#fff',weight:1.5,opacity:0.9,fillOpacity:0.8});
    m.on('click',function(){selectOpp(o.id)});
    m.bindTooltip(o.name+' ['+o.verdict+']',{direction:'top',offset:[0,-s]});
    m.addTo(map);markers[o.id]=m;
  });
}

function renderCards(){
  var el=document.getElementById('cList');el.innerHTML='';
  var list=showFail?filtered:filtered.filter(function(o){return o.verdict!=='FAIL'});
  list.forEach(function(o,i){
    var d=document.createElement('div');
    d.className='opp-card v-'+o.verdict+(o.id===selId?' selected':'');
    d.setAttribute('data-id',o.id);
    d.onclick=function(){selectOpp(o.id)};
    d.innerHTML='<span class="c-rank">#'+(i+1)+'</span>'+
      '<div class="c-name">'+o.name+'</div>'+
      '<div class="c-sub">'+o.state+(o.poi_type?' \u2022 '+o.poi_type:'')+' \u2022 Score: '+o.score+'</div>'+
      '<span class="c-badge v-'+o.verdict+'">'+o.verdict+'</span>'+
      '<span class="c-chk"><span class="p">\u2714'+o.auto_pass+'</span> <span class="f">\u2718'+o.auto_fail+'</span> <span class="u">\u26A0'+o.manual_checks+'</span></span>'+
      '<div class="c-meta"><span>Pop <b>'+fmt(o.pop_10km)+'</b></span><span>Ph <b>'+o.pharmacy_10km+'</b></span><span>Near <b>'+o.nearest_pharmacy_km+'km</b></span></div>'+
      '<span class="c-rule">'+o.best_rule+'</span>';
    el.appendChild(d);
  });
  document.getElementById('lCnt').textContent=list.length;
}

function clearRadii(){radiusCircles.forEach(function(c){map.removeLayer(c)});radiusCircles=[];}

function getRuleRadius(br){
  if(br==='item_131')return{r:10000,label:'10km (Item 131)'};
  if(br==='item_132')return{r:200,label:'200m (Item 132)'};
  if(br==='item_133')return{r:300,label:'300m (Item 133)'};
  if(br==='item_134'||br==='item_134a')return{r:300,label:'300m (Item 134)'};
  if(br==='item_135')return{r:300,label:'300m (Item 135)'};
  if(br==='item_136')return{r:300,label:'300m (Item 136)'};
  return{r:300,label:'300m'};
}

function findNearestPharmacy(lat,lng){
  var best=null,bestDist=Infinity;
  PH.forEach(function(p){
    if(!p[0]||!p[1])return;
    var dlat=(p[0]-lat)*111320;
    var dlng=(p[1]-lng)*111320*Math.cos(lat*Math.PI/180);
    var d=Math.sqrt(dlat*dlat+dlng*dlng);
    if(d<bestDist){bestDist=d;best={lat:p[0],lng:p[1],name:p[2],dist:d};}
  });
  return best;
}

function drawRadii(o){
  clearRadii();
  var br=(o.best_rule||'').toLowerCase().replace('item ','item_').replace(' ','');
  var rd=getRuleRadius(br);
  var c=L.circle([o.lat,o.lng],{radius:rd.r,color:'#f59e0b',weight:2,opacity:0.9,fillColor:'#f59e0b',fillOpacity:0.08,dashArray:'8 5'});
  c.bindTooltip(rd.label,{permanent:true,direction:'top',className:'radius-label'});
  c.addTo(map);radiusCircles.push(c);
  var np=findNearestPharmacy(o.lat,o.lng);
  if(np){
    var pm=L.circleMarker([np.lat,np.lng],{radius:10,fillColor:'#ec4899',color:'#fff',weight:2,opacity:1,fillOpacity:0.9});
    pm.bindTooltip(np.name+' ('+Math.round(np.dist)+'m)',{permanent:true,direction:'right',className:'radius-label',offset:[12,0]});
    pm.addTo(map);radiusCircles.push(pm);
  }
}

function selectOpp(id){
  selId=id;var o=OPPS.find(function(x){return x.id===id});if(!o)return;
  document.querySelectorAll('.opp-card').forEach(function(c){c.classList.toggle('selected',parseInt(c.getAttribute('data-id'))===id)});
  var sc=document.querySelector('.opp-card[data-id="'+id+'"]');if(sc)sc.scrollIntoView({behavior:'smooth',block:'nearest'});
  var br=(o.best_rule||'').toLowerCase().replace('item ','item_').replace(' ','');
  var zoomLevel=(br==='item_131')?10:15;
  if(markers[id]){map.flyTo([o.lat,o.lng],zoomLevel,{duration:0.6});markers[id].openTooltip()}
  Object.entries(markers).forEach(function(kv){var mid=parseInt(kv[0]);kv[1].setStyle(mid===id?{weight:3,color:'#22c55e'}:{weight:1.5,color:'#fff'})});
  drawRadii(o);
  showDetail(o);
}

function showDetail(o){
  document.getElementById('rPanel').classList.add('open');
  var el=document.getElementById('detailInner');
  var rulesHtml='';
  var ruleOrder=['item_131','item_132','item_133','item_134','item_134a','item_135','item_136'];
  ruleOrder.forEach(function(rk){
    var rd=o.rules_summary[rk];if(!rd)return;
    var isOpen=(rk===(o.best_rule||'').toLowerCase().replace('item ','item_').replace(' ',''));
    var checksHtml='';
    if(rd.checks){Object.keys(rd.checks).forEach(function(ck){
      var ch=rd.checks[ck];
      var icon=ch.status==='PASS'?'\u2705':ch.status==='FAIL'?'\u274C':'\u26A0\uFE0F';
      checksHtml+='<div class="chk-row"><span class="chk-icon">'+icon+'</span><span class="chk-text"><b>'+ck.replace(/_/g,' ')+'</b><br>'+(ch.detail||'').replace(/</g,'&lt;')+'</span></div>';
    })}
    rulesHtml+='<div class="rule-card rv-'+rd.verdict+(isOpen?' open':'')+'" onclick="this.classList.toggle(\'open\')">'+
      '<div class="rule-card-hdr"><span class="rc-label">'+(RULE_NAMES[rk]||rk)+'</span><span class="rc-verdict rv-'+rd.verdict+'">'+rd.verdict+'</span></div>'+
      '<div class="rule-card-body">'+checksHtml+'</div></div>';
  });
  var geoHtml=o.geocoding_flag?'<div class="geo-flag">\u26A0 Geocoding: '+o.geocoding_flag+'</div>':'';
  var evHtml=o.evidence?'<div class="d-section"><h4>Evidence / Notes</h4><div style="font-size:10px;color:#94a3b8;line-height:1.5;max-height:120px;overflow-y:auto">'+o.evidence.replace(/</g,'&lt;')+'</div></div>':'';
  var grHtml=o.growth_details?'<div class="d-section"><h4>Growth</h4><div style="font-size:10px;color:#22c55e">\uD83D\uDCC8 '+o.growth_details.replace(/</g,'&lt;')+'</div></div>':'';
  el.innerHTML=
    '<div class="d-header">'+
      '<button class="d-close" onclick="closeDetail()">&times;</button>'+
      '<div class="d-name">'+o.name+'</div>'+
      '<div class="d-addr">'+(o.address||o.state||'')+'</div>'+
      '<div class="d-verdict v-'+o.verdict+'">'+o.verdict+'</div>'+
      '<div class="d-sub">Score: '+o.score+' \u2022 '+o.status_summary+' \u2022 Best: '+o.best_rule+'</div>'+
      geoHtml+
    '</div>'+
    '<div class="d-section"><h4>Population &amp; Pharmacies</h4>'+
      '<div class="d-grid3">'+
        '<div class="d-stat"><div class="v">'+fmt(o.pop_5km)+'</div><div class="l">Pop 5km</div></div>'+
        '<div class="d-stat"><div class="v">'+fmt(o.pop_10km)+'</div><div class="l">Pop 10km</div></div>'+
        '<div class="d-stat"><div class="v">'+fmt(o.pop_15km)+'</div><div class="l">Pop 15km</div></div>'+
      '</div>'+
      '<div class="d-grid3" style="margin-top:6px">'+
        '<div class="d-stat"><div class="v">'+o.pharmacy_5km+'</div><div class="l">Ph 5km</div></div>'+
        '<div class="d-stat"><div class="v">'+o.pharmacy_10km+'</div><div class="l">Ph 10km</div></div>'+
        '<div class="d-stat"><div class="v">'+o.pharmacy_15km+'</div><div class="l">Ph 15km</div></div>'+
      '</div>'+
      '<div class="nearest-box"><div class="ico">\uD83C\uDFE5</div><div class="inf"><div class="n">'+(o.nearest_pharmacy||'Unknown')+'</div><div class="d">'+o.nearest_pharmacy_km+' km away</div></div></div>'+
    '</div>'+
    '<div class="d-section"><h4>Rule Compliance (All 7 Rules)</h4>'+rulesHtml+'</div>'+
    evHtml+grHtml+
    '<div class="d-actions"><a class="btn-gm" href="https://www.google.com/maps?q='+o.lat+','+o.lng+'" target="_blank">\uD83D\uDDFA Open in Google Maps</a></div>';
}

function closeDetail(){
  document.getElementById('rPanel').classList.remove('open');selId=null;
  document.querySelectorAll('.opp-card').forEach(function(c){c.classList.remove('selected')});
  Object.values(markers).forEach(function(m){m.setStyle({weight:1.5,color:'#fff'})});
  clearRadii();
}

function applyFilters(){
  var v=document.getElementById('fV').value;
  var st=document.getElementById('fSt').value;
  var r=document.getElementById('fR').value;
  var q=(document.getElementById('fSearch').value||'').toLowerCase();
  var sort=document.getElementById('fSort').value;
  filtered=OPPS.filter(function(o){
    if(v!=='ALL'&&o.verdict!==v)return false;
    if(st!=='ALL'&&o.state!==st)return false;
    if(r!=='ALL'){
      var br=(o.best_rule||'').toLowerCase().replace('item ','item_').replace(' ','');
      if(br!==r){
        var rd=o.rules_summary?o.rules_summary[r]:null;
        if(!rd||rd.verdict==='FAIL')return false;
      }
    }
    if(q&&o.name.toLowerCase().indexOf(q)===-1&&(o.address||'').toLowerCase().indexOf(q)===-1)return false;
    return true;
  });
  if(sort==='score')filtered.sort(function(a,b){return b.score-a.score});
  else if(sort==='nearest')filtered.sort(function(a,b){return b.nearest_pharmacy_km-a.nearest_pharmacy_km});
  else if(sort==='pop')filtered.sort(function(a,b){return b.pop_10km-a.pop_10km});
  else if(sort==='name')filtered.sort(function(a,b){return a.name.localeCompare(b.name)});
  addMarkers();renderCards();updateStats();
  if(selId&&!filtered.find(function(o){return o.id===selId}))closeDetail();
}

function updateStats(){
  var vis=showFail?filtered:filtered.filter(function(o){return o.verdict!=='FAIL'});
  document.getElementById('sS').textContent=vis.length;
  var p=0,l=0,f=0;
  filtered.forEach(function(o){if(o.verdict==='PASS')p++;else if(o.verdict==='LIKELY')l++;else f++});
  document.getElementById('sP').textContent=p;
  document.getElementById('sL').textContent=l;
  document.getElementById('sF').textContent=f;
}

function resetFilters(){
  document.getElementById('fV').value='ALL';
  document.getElementById('fSt').value='ALL';
  document.getElementById('fR').value='ALL';
  document.getElementById('fSort').value='score';
  document.getElementById('fSearch').value='';
  applyFilters();
}

function exportCSV(){
  var h=['Rank','Name','State','Verdict','Score','Best_Rule','Pop_10km','Pharmacies_10km','Nearest_Pharmacy','Nearest_km','Lat','Lng','Address'];
  var vis=showFail?filtered:filtered.filter(function(o){return o.verdict!=='FAIL'});
  var rows=vis.map(function(o,i){
    return [i+1,'"'+o.name.replace(/"/g,'""')+'"',o.state,o.verdict,o.score,'"'+o.best_rule+'"',o.pop_10km,o.pharmacy_10km,'"'+(o.nearest_pharmacy||'').replace(/"/g,'""')+'"',o.nearest_pharmacy_km,o.lat,o.lng,'"'+(o.address||'').replace(/"/g,'""')+'"'].join(',');
  });
  var csv=h.join(',')+'\n'+rows.join('\n');
  var b=new Blob([csv],{type:'text/csv'});var u=URL.createObjectURL(b);
  var a=document.createElement('a');a.href=u;a.download='pharmacyfinder_opportunities.csv';a.click();URL.revokeObjectURL(u);
}

document.addEventListener('DOMContentLoaded',function(){
  initMap();addMarkers();renderCards();
  document.getElementById('fV').addEventListener('change',applyFilters);
  document.getElementById('fSt').addEventListener('change',applyFilters);
  document.getElementById('fR').addEventListener('change',applyFilters);
  document.getElementById('fSort').addEventListener('change',applyFilters);
  document.getElementById('fSearch').addEventListener('input',applyFilters);
  if(OPPS.length>0){
    var nf=OPPS.filter(function(o){return o.verdict!=='FAIL'});
    var pts=nf.length>0?nf:OPPS;
    var bounds=L.latLngBounds(pts.map(function(o){return[o.lat,o.lng]}));
    map.fitBounds(bounds.pad(0.1));
  }
});
''')
    
    parts.append('</script>\n</body>\n</html>')
    
    return ''.join(parts)


def build():
    print("=" * 80)
    print("PharmacyFinder Dashboard v3 Builder")
    print("=" * 80)
    
    scored_path = os.path.join(OUTPUT_DIR, 'scored_v2.json')
    if os.path.exists(scored_path):
        print("\n[1/4] Loading pre-scored results from output/scored_v2.json...")
        with open(scored_path, 'r', encoding='utf-8') as f:
            scored = json.load(f)
        print(f"  Loaded {len(scored)} scored opportunities")
    else:
        print("\n[1/4] No scored data found — running scorer...")
        from score_v2 import score_all_opportunities, update_db
        scored = score_all_opportunities()
        update_db(scored)
        with open(scored_path, 'w', encoding='utf-8') as f:
            json.dump(scored, f, indent=2, ensure_ascii=False, default=str)
        print(f"  Scored {len(scored)} opportunities")
    
    print("\n[2/4] Loading pharmacies...")
    pharmacies = load_pharmacies()
    print(f"  {len(pharmacies)} pharmacies loaded")
    
    print("\n[3/4] Generating dashboard...")
    html = generate_html(scored, pharmacies)
    
    print("\n[4/4] Writing dashboard...")
    html_clean = html.encode('utf-8', errors='replace').decode('utf-8')
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html_clean)
    
    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    n_pass = sum(1 for o in scored if o.get('verdict') == 'PASS')
    n_likely = sum(1 for o in scored if o.get('verdict') == 'LIKELY')
    n_fail = sum(1 for o in scored if o.get('verdict') == 'FAIL')
    
    print(f"\n{'=' * 80}")
    print(f"DASHBOARD BUILT")
    print(f"  File: {OUTPUT_FILE}")
    print(f"  Size: {size_kb:,.1f} KB")
    print(f"  Opportunities: {len(scored)} (PASS: {n_pass}, LIKELY: {n_likely}, FAIL: {n_fail})")
    print(f"  Pharmacies: {len(pharmacies)}")
    print(f"{'=' * 80}")


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    build()
