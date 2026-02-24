#!/usr/bin/env python3
"""
Full Coordinate Audit — Verify ALL data points via Nominatim.
Runs autonomously. Outputs results to output/coord_audit_full.json
"""

import sqlite3, json, os, math, time, urllib.request, sys

DB_PATH = 'pharmacy_finder.db'
OUTPUT_FILE = 'output/coord_audit_full.json'
PROGRESS_FILE = 'output/coord_audit_progress.json'

# Rate limit: 1 request per second
RATE_LIMIT = 1.1
# Only flag entries with shift > this many meters
FLAG_THRESHOLD = 200

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    p = math.pi / 180
    a = 0.5 - math.cos((lat2-lat1)*p)/2 + math.cos(lat1*p)*math.cos(lat2*p)*(1-math.cos((lon2-lon1)*p))/2
    return 2 * R * math.asin(math.sqrt(a))

def geocode(query):
    """Geocode via Nominatim. Returns (lat, lon, display_name) or None."""
    try:
        q = urllib.parse.quote(query)
        url = f'https://nominatim.openstreetmap.org/search?q={q}&format=json&limit=1&countrycodes=au'
        req = urllib.request.Request(url, headers={'User-Agent': 'PharmacyFinder-Audit/1.0'})
        r = urllib.request.urlopen(req, timeout=10)
        data = json.loads(r.read())
        if data:
            return float(data[0]['lat']), float(data[0]['lon']), data[0].get('display_name', '')
    except Exception as e:
        pass
    return None

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            p = json.load(f)
        # Ensure all keys exist
        if 'completed' not in p: p['completed'] = {}
        if 'flagged' not in p: p['flagged'] = []
        if 'stats' not in p: p['stats'] = {}
        return p
    return {'completed': {}, 'flagged': [], 'stats': {}}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2, default=str)

def audit_table(conn, table, name_col, progress):
    c = conn.cursor()
    c.execute(f"SELECT id, {name_col}, latitude, longitude, address FROM {table} WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    rows = c.fetchall()
    
    table_key = table
    if table_key not in progress['completed']:
        progress['completed'][table_key] = {}
    
    completed = progress['completed'][table_key]
    total = len(rows)
    skipped = 0
    checked = 0
    flagged_count = 0
    fixed_count = 0
    no_result = 0
    
    print(f"\n{'='*60}")
    print(f"Auditing {table} ({total} entries)")
    print(f"{'='*60}")
    
    for i, row in enumerate(rows):
        rid, name, lat, lng, address = row[0], row[1], row[2], row[3], row[4] if len(row) > 4 else ''
        
        # Skip if already checked
        str_id = str(rid)
        if str_id in completed:
            skipped += 1
            continue
        
        # Build search query
        if address and len(address) > 10:
            query = address
        else:
            query = f"{name} Australia"
        
        # Rate limit
        time.sleep(RATE_LIMIT)
        
        result = geocode(query)
        checked += 1
        
        if result is None:
            no_result += 1
            completed[str_id] = {'status': 'no_result', 'name': name}
            if checked % 50 == 0:
                save_progress(progress)
                print(f"  [{table}] {checked}/{total-skipped} checked, {flagged_count} flagged, {no_result} no result")
            continue
        
        new_lat, new_lng, display = result
        dist = haversine(lat, lng, new_lat, new_lng)
        
        entry = {
            'name': name,
            'old_lat': lat, 'old_lng': lng,
            'new_lat': new_lat, 'new_lng': new_lng,
            'distance_m': round(dist, 1),
            'nominatim_name': display[:100],
            'query': query[:80]
        }
        
        if dist > FLAG_THRESHOLD:
            # Check if it's a chain name false positive (different branch)
            name_lower = (name or '').lower()
            is_chain = any(c in name_lower for c in ['coles', 'woolworths', 'iga', 'foodworks', 'aldi', 
                'chemist warehouse', 'priceline', 'terrywhite', 'amcal', 'blooms', 'star discount'])
            
            if is_chain and dist > 5000:
                # Likely different branch — skip
                entry['status'] = 'chain_skip'
                completed[str_id] = entry
            elif dist > 50000:
                # > 50km shift — likely wrong result, skip auto-fix
                entry['status'] = 'extreme_shift'
                progress['flagged'].append({**entry, 'table': table, 'id': rid})
                flagged_count += 1
                completed[str_id] = entry
            else:
                entry['status'] = 'flagged'
                progress['flagged'].append({**entry, 'table': table, 'id': rid})
                flagged_count += 1
                
                # Auto-fix shifts between 200m and 5km if not a chain
                if dist < 5000 and not is_chain:
                    c2 = conn.cursor()
                    c2.execute(f"UPDATE {table} SET latitude=?, longitude=? WHERE id=?", (new_lat, new_lng, rid))
                    conn.commit()
                    entry['status'] = 'auto_fixed'
                    fixed_count += 1
                
                completed[str_id] = entry
        else:
            entry['status'] = 'ok'
            completed[str_id] = entry
        
        if checked % 50 == 0:
            save_progress(progress)
            print(f"  [{table}] {checked}/{total-skipped} checked, {flagged_count} flagged, {fixed_count} fixed, {no_result} no result")
    
    # Save after each table
    save_progress(progress)
    
    stats = {
        'total': total,
        'checked': checked,
        'skipped_existing': skipped,
        'flagged': flagged_count,
        'auto_fixed': fixed_count,
        'no_result': no_result
    }
    progress['stats'][table] = stats
    save_progress(progress)
    
    print(f"\n  {table} complete: {checked} checked, {flagged_count} flagged, {fixed_count} auto-fixed, {no_result} no result")
    return stats


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    print("=" * 60)
    print("FULL COORDINATE AUDIT")
    print("=" * 60)
    print(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = sqlite3.connect(DB_PATH)
    progress = load_progress()
    
    # Priority order: high-impact tables first
    tables = [
        ('medical_centres', 'name'),      # 171 — critical for Item 136
        ('hospitals', 'name'),             # 38 — critical for Item 135
        ('shopping_centres', 'name'),      # 154 — critical for Items 133/134
        ('gps', 'name'),                   # 89 — affects GP counts
        ('opportunities', 'poi_name'),     # 1,450 — the opportunity pins
        ('supermarkets', 'name'),          # 1,439 — affects Item 132
        ('pharmacies', 'name'),            # 5,592 — affects all distance calcs
    ]
    
    all_stats = {}
    for table, name_col in tables:
        stats = audit_table(conn, table, name_col, progress)
        all_stats[table] = stats
    
    conn.close()
    
    # Final report
    print("\n" + "=" * 60)
    print("AUDIT COMPLETE")
    print("=" * 60)
    
    total_checked = sum(s['checked'] for s in all_stats.values())
    total_flagged = sum(s['flagged'] for s in all_stats.values())
    total_fixed = sum(s['auto_fixed'] for s in all_stats.values())
    
    print(f"Total checked: {total_checked}")
    print(f"Total flagged (>200m): {total_flagged}")
    print(f"Total auto-fixed (<5km non-chain): {total_fixed}")
    print(f"Completed: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    for table, stats in all_stats.items():
        print(f"  {table:25s}: {stats['checked']:5d} checked, {stats['flagged']:4d} flagged, {stats['auto_fixed']:4d} fixed")
    
    # Save final results
    with open(OUTPUT_FILE, 'w') as f:
        json.dump({
            'summary': all_stats,
            'flagged': progress['flagged'],
            'total_checked': total_checked,
            'total_flagged': total_flagged,
            'total_fixed': total_fixed,
        }, f, indent=2, default=str)
    
    print(f"\nResults saved to {OUTPUT_FILE}")
    print(f"Progress saved to {PROGRESS_FILE} (resumable)")


if __name__ == '__main__':
    main()
