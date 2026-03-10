#!/usr/bin/env python3
"""
PharmacyFinder Dashboard v6 — Compliance-first, rules engine results.

Only shows legally compliant sites from V2 rules engine.
Each site shows: which rules passed, WHY (reasons), confidence, evidence needed.
Color markers by primary rule item.
Sort by commercial score.
Dark theme, Leaflet map, detail panel.
"""
import sys, os, io, json, sqlite3
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_results():
    """Load V2 results from database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT * FROM v2_results
        WHERE passed_any = 1
        ORDER BY commercial_score DESC
    """)
    results = []
    for row in cur.fetchall():
        r = dict(row)
        try:
            r['rules'] = json.loads(r.get('rules_json', '[]'))
        except:
            r['rules'] = []
        try:
            r['all_rules'] = json.loads(r.get('all_rules_json', '[]'))
        except:
            r['all_rules'] = []
        results.append(r)
    
    conn.close()
    return results


def load_pharmacies():
    """Load pharmacy locations for reference layer."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT name, latitude, longitude, state FROM pharmacies WHERE latitude IS NOT NULL")
    pharmacies = [dict(r) for r in cur.fetchall()]
    conn.close()
    return pharmacies


def build_dashboard(results, pharmacies):
    """Generate HTML dashboard."""
    
    # Prepare data for JavaScript
    sites_js = []
    for r in results:
        passing_rules = []
        for rule in r.get('rules', []):
            passing_rules.append({
                'item': rule.get('item', ''),
                'confidence': rule.get('confidence', 0),
                'reasons': rule.get('reasons', []),
                'evidence_needed': rule.get('evidence_needed', []),
                'distances': rule.get('distances', {}),
            })
        
        sites_js.append({
            'id': r['id'],
            'name': r['name'],
            'address': r.get('address', ''),
            'lat': r['latitude'],
            'lon': r['longitude'],
            'state': r.get('state', ''),
            'source_type': r.get('source_type', ''),
            'primary_rule': r.get('primary_rule', ''),
            'commercial_score': r.get('commercial_score', 0),
            'best_confidence': r.get('best_confidence', 0),
            'rules': passing_rules,
        })
    
    # Pharmacy markers (simplified for map layer)
    pharm_js = [{'name': p['name'], 'lat': p['latitude'], 'lon': p['longitude'], 'state': p.get('state', '')}
                for p in pharmacies if p['latitude'] and p['longitude']]
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PharmacyFinder V2 — Rules Engine Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #e0e0e0; }}

#header {{ background: #16213e; padding: 12px 20px; display: flex; align-items: center; justify-content: space-between; border-bottom: 2px solid #0f3460; }}
#header h1 {{ font-size: 18px; color: #e94560; }}
#header .stats {{ font-size: 14px; color: #a0a0a0; }}

#container {{ display: flex; height: calc(100vh - 50px); }}
#sidebar {{ width: 400px; overflow-y: auto; background: #16213e; border-right: 1px solid #0f3460; }}
#map {{ flex: 1; }}

.filters {{ padding: 12px; border-bottom: 1px solid #0f3460; display: flex; flex-wrap: wrap; gap: 8px; }}
.filters select, .filters input {{ background: #1a1a2e; color: #e0e0e0; border: 1px solid #0f3460; padding: 6px 10px; border-radius: 4px; font-size: 13px; }}

.site-card {{ padding: 12px; border-bottom: 1px solid #0f3460; cursor: pointer; transition: background 0.2s; }}
.site-card:hover {{ background: #1a1a2e; }}
.site-card.active {{ background: #0f3460; border-left: 3px solid #e94560; }}
.site-name {{ font-weight: 600; font-size: 14px; color: #fff; }}
.site-meta {{ font-size: 12px; color: #a0a0a0; margin-top: 4px; }}
.site-score {{ font-size: 13px; color: #e94560; font-weight: 600; }}
.rule-badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; margin: 2px; }}

#detail-panel {{ display: none; position: absolute; right: 10px; top: 60px; width: 420px; max-height: calc(100vh - 80px); overflow-y: auto; background: #16213e; border: 1px solid #0f3460; border-radius: 8px; padding: 16px; z-index: 1000; box-shadow: 0 4px 20px rgba(0,0,0,0.5); }}
#detail-panel h2 {{ font-size: 16px; color: #e94560; margin-bottom: 10px; }}
#detail-panel .close-btn {{ position: absolute; top: 8px; right: 12px; cursor: pointer; font-size: 18px; color: #a0a0a0; }}
.detail-section {{ margin: 10px 0; padding: 8px; background: #1a1a2e; border-radius: 6px; }}
.detail-section h3 {{ font-size: 13px; color: #e94560; margin-bottom: 6px; }}
.reason {{ font-size: 12px; padding: 4px 0; border-bottom: 1px solid #0f3460; }}
.reason.pass {{ color: #4ecdc4; }}
.reason.fail {{ color: #ff6b6b; }}
.evidence {{ font-size: 12px; color: #ffa726; padding: 2px 0; }}
.confidence-bar {{ height: 6px; background: #0f3460; border-radius: 3px; margin-top: 4px; }}
.confidence-fill {{ height: 100%; border-radius: 3px; }}

.badge-130 {{ background: #2196F3; color: #fff; }}
.badge-131 {{ background: #4CAF50; color: #fff; }}
.badge-132 {{ background: #9C27B0; color: #fff; }}
.badge-133 {{ background: #FF9800; color: #fff; }}
.badge-135 {{ background: #f44336; color: #fff; }}
.badge-136 {{ background: #00897B; color: #fff; }}
</style>
</head>
<body>
<div id="header">
    <h1>PharmacyFinder V2 &mdash; Rules Engine</h1>
    <div class="stats">
        <span id="total-count">{len(results)}</span> compliant sites |
        <span id="filtered-count">{len(results)}</span> shown
    </div>
</div>
<div id="container">
    <div id="sidebar">
        <div class="filters">
            <select id="state-filter">
                <option value="">All States</option>
                <option value="NSW">NSW</option>
                <option value="VIC">VIC</option>
                <option value="QLD">QLD</option>
                <option value="WA">WA</option>
                <option value="SA">SA</option>
                <option value="TAS">TAS</option>
                <option value="NT">NT</option>
                <option value="ACT">ACT</option>
            </select>
            <select id="rule-filter">
                <option value="">All Rules</option>
                <option value="Item 130">Item 130 (1.5km)</option>
                <option value="Item 131">Item 131 (10km)</option>
                <option value="Item 132">Item 132 (Town)</option>
                <option value="Item 133">Item 133 (Shopping)</option>
                <option value="Item 135">Item 135 (Hospital)</option>
                <option value="Item 136">Item 136 (Medical)</option>
            </select>
            <select id="confidence-filter">
                <option value="0">Any Confidence</option>
                <option value="0.65">65%+</option>
                <option value="0.75">75%+</option>
                <option value="0.85">85%+</option>
                <option value="0.95">95%+</option>
            </select>
        </div>
        <div id="site-list"></div>
    </div>
    <div id="map"></div>
</div>
<div id="detail-panel">
    <span class="close-btn" onclick="closeDetail()">&times;</span>
    <div id="detail-content"></div>
</div>

<script>
const SITES = {json.dumps(sites_js)};
const PHARMACIES = {json.dumps(pharm_js)};

const RULE_COLORS = {{
    'Item 130': '#2196F3',
    'Item 131': '#4CAF50',
    'Item 132': '#9C27B0',
    'Item 133': '#FF9800',
    'Item 135': '#f44336',
    'Item 136': '#00897B',
}};

// Initialize map
const map = L.map('map').setView([-25.5, 134], 5);
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
    attribution: '&copy; CartoDB',
    maxZoom: 19,
}}).addTo(map);

// Pharmacy layer (toggleable)
const pharmLayer = L.layerGroup();
PHARMACIES.forEach(p => {{
    L.circleMarker([p.lat, p.lon], {{
        radius: 3, color: '#666', fillColor: '#888', fillOpacity: 0.4, weight: 1,
    }}).bindTooltip(p.name, {{direction: 'top'}}).addTo(pharmLayer);
}});
pharmLayer.addTo(map);

// Site markers
const siteMarkers = {{}};
const markerLayer = L.layerGroup().addTo(map);

function createMarker(site) {{
    const color = RULE_COLORS[site.primary_rule] || '#fff';
    const size = 6 + Math.round(site.commercial_score * 8);
    const marker = L.circleMarker([site.lat, site.lon], {{
        radius: size, color: color, fillColor: color, fillOpacity: 0.7, weight: 2,
    }});
    marker.bindTooltip(
        `<b>${{site.name}}</b><br>${{site.primary_rule}} | Score: ${{(site.commercial_score*100).toFixed(1)}}`,
        {{direction: 'top'}}
    );
    marker.on('click', () => showDetail(site));
    return marker;
}}

SITES.forEach(site => {{
    siteMarkers[site.id] = createMarker(site);
}});

// Layer control
L.control.layers(null, {{
    'Existing Pharmacies': pharmLayer,
    'Compliant Sites': markerLayer,
}}).addTo(map);

// Render
function render() {{
    const stateF = document.getElementById('state-filter').value;
    const ruleF = document.getElementById('rule-filter').value;
    const confF = parseFloat(document.getElementById('confidence-filter').value);
    
    markerLayer.clearLayers();
    const listEl = document.getElementById('site-list');
    listEl.innerHTML = '';
    let count = 0;
    
    SITES.forEach(site => {{
        // Filters
        if (stateF && site.state !== stateF) return;
        if (ruleF && !site.rules.some(r => r.item === ruleF)) return;
        if (site.best_confidence < confF) return;
        
        count++;
        markerLayer.addLayer(siteMarkers[site.id]);
        
        // Card
        const badges = site.rules.map(r => {{
            const cls = 'badge-' + r.item.replace('Item ', '');
            return `<span class="rule-badge ${{cls}}">${{r.item}}</span>`;
        }}).join('');
        
        const card = document.createElement('div');
        card.className = 'site-card';
        card.innerHTML = `
            <div class="site-name">${{site.name}}</div>
            <div class="site-meta">${{site.state}} | ${{site.source_type}} | ${{site.address.substring(0,50)}}</div>
            <div style="margin-top:4px">${{badges}}</div>
            <div class="site-score">Score: ${{(site.commercial_score*100).toFixed(1)}} | Confidence: ${{(site.best_confidence*100).toFixed(0)}}%</div>
        `;
        card.onclick = () => {{
            showDetail(site);
            map.setView([site.lat, site.lon], 13);
            document.querySelectorAll('.site-card').forEach(c => c.classList.remove('active'));
            card.classList.add('active');
        }};
        listEl.appendChild(card);
    }});
    
    document.getElementById('filtered-count').textContent = count;
}}

function showDetail(site) {{
    const panel = document.getElementById('detail-panel');
    const content = document.getElementById('detail-content');
    
    let html = `<h2>${{site.name}}</h2>`;
    html += `<p style="color:#a0a0a0;font-size:13px">${{site.address}}<br>${{site.state}} | ${{site.source_type}}</p>`;
    html += `<p class="site-score" style="margin:8px 0">Commercial Score: ${{(site.commercial_score*100).toFixed(1)}} / 100</p>`;
    
    site.rules.forEach(rule => {{
        const confPct = (rule.confidence * 100).toFixed(0);
        const confColor = rule.confidence >= 0.85 ? '#4ecdc4' : rule.confidence >= 0.65 ? '#ffa726' : '#ff6b6b';
        
        html += `<div class="detail-section">`;
        html += `<h3>${{rule.item}} — Confidence: ${{confPct}}%</h3>`;
        html += `<div class="confidence-bar"><div class="confidence-fill" style="width:${{confPct}}%;background:${{confColor}}"></div></div>`;
        
        // Reasons
        if (rule.reasons && rule.reasons.length) {{
            html += '<div style="margin-top:8px">';
            rule.reasons.forEach(r => {{
                const cls = r.startsWith('PASS') ? 'pass' : r.startsWith('FAIL') ? 'fail' : '';
                html += `<div class="reason ${{cls}}">${{r}}</div>`;
            }});
            html += '</div>';
        }}
        
        // Evidence needed
        if (rule.evidence_needed && rule.evidence_needed.length) {{
            html += '<div style="margin-top:8px"><b style="color:#ffa726;font-size:12px">Evidence Needed:</b>';
            rule.evidence_needed.forEach(e => {{
                html += `<div class="evidence">&#x26A0; ${{e}}</div>`;
            }});
            html += '</div>';
        }}
        
        // Distances
        if (rule.distances && Object.keys(rule.distances).length) {{
            html += '<div style="margin-top:8px"><b style="font-size:12px;color:#a0a0a0">Measurements:</b>';
            html += '<table style="font-size:11px;width:100%;margin-top:4px">';
            for (const [k, v] of Object.entries(rule.distances)) {{
                html += `<tr><td style="color:#888;padding:1px 4px">${{k}}</td><td>${{v}}</td></tr>`;
            }}
            html += '</table></div>';
        }}
        
        html += '</div>';
    }});
    
    content.innerHTML = html;
    panel.style.display = 'block';
}}

function closeDetail() {{
    document.getElementById('detail-panel').style.display = 'none';
}}

// Filter events
['state-filter', 'rule-filter', 'confidence-filter'].forEach(id => {{
    document.getElementById(id).addEventListener('change', render);
}});

// Initial render
render();
</script>
</body>
</html>"""
    
    return html


def main():
    print("Loading V2 results...")
    results = load_results()
    print(f"  {len(results)} compliant sites")
    
    print("Loading pharmacies...")
    pharmacies = load_pharmacies()
    print(f"  {len(pharmacies)} pharmacies")
    
    print("Building dashboard...")
    html = build_dashboard(results, pharmacies)
    
    outfile = os.path.join(OUTPUT_DIR, 'dashboard_v6.html')
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard saved to {outfile}")
    
    # Stats
    by_rule = {}
    by_state = {}
    for r in results:
        rule = r.get('primary_rule', 'Unknown')
        state = r.get('state', 'Unknown')
        by_rule[rule] = by_rule.get(rule, 0) + 1
        by_state[state] = by_state.get(state, 0) + 1
    
    print(f"\n--- By Rule ---")
    for rule, cnt in sorted(by_rule.items(), key=lambda x: -x[1]):
        print(f"  {rule}: {cnt}")
    print(f"\n--- By State ---")
    for state, cnt in sorted(by_state.items(), key=lambda x: -x[1]):
        print(f"  {state}: {cnt}")


if __name__ == '__main__':
    main()
