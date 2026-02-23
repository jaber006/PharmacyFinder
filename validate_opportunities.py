"""
Validate ALL opportunities by searching DuckDuckGo for "[town] pharmacy".
If a pharmacy exists in that town, mark as false positive.
Much more reliable than trying to scrape Google Maps results.
"""
import sqlite3, os, sys, io, re, time, json, urllib.request
from urllib.parse import quote

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

def search_ddg(query):
    """Search DuckDuckGo HTML and return result text."""
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode('utf-8', errors='replace')
    except:
        return ""

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("""SELECT id, poi_name, latitude, longitude, nearest_pharmacy_km, 
             nearest_pharmacy_name, region, nearest_town, composite_score, pop_5km
             FROM opportunities 
             WHERE latitude IS NOT NULL
             ORDER BY composite_score DESC""")
opportunities = c.fetchall()

print(f"VALIDATING {len(opportunities)} OPPORTUNITIES")
print(f"Searching DuckDuckGo for pharmacies in each town")
print(f"{'='*70}\n")

false_positives = []
confirmed = []
needs_review = []
total = 0

# Track towns we've already checked to avoid redundant searches
town_cache = {}  # town+state -> has_pharmacy (bool)

for i, (id, name, lat, lng, db_nearest_km, db_nearest_name, state, town, score, pop) in enumerate(opportunities, 1):
    total += 1
    
    # Cache key: town + state
    cache_key = f"{town}_{state}" if town else f"{lat:.2f}_{lng:.2f}"
    
    if cache_key in town_cache:
        result = town_cache[cache_key]
        if result['has_pharmacy']:
            print(f"[{i}/{len(opportunities)}] {name} ({town}, {state}) - CACHED FALSE POSITIVE")
            false_positives.append({
                'id': id, 'name': name, 'town': town, 'state': state, 
                'score': score, 'pop': pop, 'db_nearest_km': db_nearest_km,
                'evidence': result['evidence']
            })
            c.execute("UPDATE opportunities SET verification = 'FALSE_POSITIVE' WHERE id = ?", (id,))
        else:
            print(f"[{i}/{len(opportunities)}] {name} ({town}, {state}) - CACHED CONFIRMED")
            confirmed.append({'id': id, 'name': name, 'town': town, 'state': state, 'score': score})
            c.execute("UPDATE opportunities SET verification = 'VERIFIED' WHERE id = ?", (id,))
        continue
    
    print(f"[{i}/{len(opportunities)}] {name} ({town}, {state}) Score:{score:.0f}")
    print(f"  DB nearest: {db_nearest_name} ({db_nearest_km:.1f}km) | Pop: {pop}")
    
    # Search for pharmacy in this town
    search_town = town if town and len(town) > 2 else name
    query = f'"{search_town}" pharmacy chemist {state} Australia'
    
    html = search_ddg(query)
    time.sleep(1.5)  # Rate limit
    
    # Strip HTML tags to get just the text content of search results
    # Remove <title>, <meta>, <script>, <style> tags and their content first
    clean = re.sub(r'<(title|meta|script|style|link)[^>]*>.*?</(title|meta|script|style|link)>', '', html, flags=re.DOTALL|re.IGNORECASE)
    clean = re.sub(r'<(meta|link|input)[^>]*/?>', '', clean, flags=re.IGNORECASE)
    # Remove all remaining HTML tags
    clean = re.sub(r'<[^>]+>', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean).lower()
    
    town_lower = search_town.lower() if search_town else ""
    
    # Strong evidence patterns: "[Town] Pharmacy" as a business name
    has_pharmacy_evidence = False
    evidence = ""
    
    # Look for specific business name patterns in the cleaned text
    strong_patterns = [
        f'{town_lower} pharmacy',
        f'{town_lower} chemist',
        f'{town_lower} amcal',
        f'{town_lower} chemmart',
        f'{town_lower} priceline',
        f'{town_lower} discount drug',
        f'pharmacy {town_lower}',
        f'chemist {town_lower}',
        f'pharmacy in {town_lower}',
        f'pharmacist in {town_lower}',
        f'chemmart {town_lower}',
        f'priceline {town_lower}',
    ]
    
    for pat in strong_patterns:
        m = re.search(re.escape(pat) if '(' not in pat else pat, clean)
        if m:
            start = max(0, m.start() - 30)
            end = min(len(clean), m.end() + 80)
            snippet = clean[start:end].strip()
            
            # Filter out negatives
            if any(neg in snippet for neg in ['no pharmacy', 'nearest pharmacy', 'closest pharmacy', 
                                               'without a pharmacy', 'need a pharmacy', 'duckduckgo',
                                               'search for', 'looking for']):
                continue
            
            # Must have some real content, not just search page boilerplate
            if len(snippet) > 20:
                has_pharmacy_evidence = True
                evidence = snippet[:120]
                break
    
    if has_pharmacy_evidence and db_nearest_km > 3:
        print(f"  FALSE POSITIVE: Evidence of pharmacy in {search_town}")
        print(f"    Evidence: {evidence}")
        false_positives.append({
            'id': id, 'name': name, 'town': town, 'state': state,
            'score': score, 'pop': pop, 'db_nearest_km': db_nearest_km,
            'evidence': evidence
        })
        c.execute("UPDATE opportunities SET verification = 'FALSE_POSITIVE' WHERE id = ?", (id,))
        town_cache[cache_key] = {'has_pharmacy': True, 'evidence': evidence}
    elif has_pharmacy_evidence:
        # We found evidence but nearest pharmacy is already <3km - probably already in DB
        print(f"  CONFIRMED (pharmacy evidence but nearest already {db_nearest_km:.1f}km)")
        confirmed.append({'id': id, 'name': name, 'town': town, 'state': state, 'score': score})
        c.execute("UPDATE opportunities SET verification = 'VERIFIED' WHERE id = ?", (id,))
        town_cache[cache_key] = {'has_pharmacy': False, 'evidence': ''}
    else:
        print(f"  CONFIRMED: No pharmacy found in {search_town}")
        confirmed.append({'id': id, 'name': name, 'town': town, 'state': state, 'score': score})
        c.execute("UPDATE opportunities SET verification = 'VERIFIED' WHERE id = ?", (id,))
        town_cache[cache_key] = {'has_pharmacy': False, 'evidence': ''}
    
    if total % 50 == 0:
        conn.commit()
        print(f"\n  --- Progress: {total}/{len(opportunities)} | FP:{len(false_positives)} Confirmed:{len(confirmed)} ---\n")

conn.commit()

print(f"\n{'='*70}")
print(f"VALIDATION COMPLETE")
print(f"{'='*70}")
print(f"Total checked:   {total}")
print(f"FALSE POSITIVES: {len(false_positives)}")
print(f"CONFIRMED:       {len(confirmed)}")

if false_positives:
    print(f"\nFALSE POSITIVES (pharmacy exists in town but missing from DB):")
    for fp in sorted(false_positives, key=lambda x: -x['score'])[:30]:
        print(f"  {fp['name']} ({fp['town']}, {fp['state']}) Score:{fp['score']:.0f}")
        print(f"    DB said nearest: {fp['db_nearest_km']:.1f}km | Evidence: {fp['evidence'][:80]}")

# Save
results = {
    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
    'total': total,
    'false_positives': len(false_positives),
    'confirmed': len(confirmed),
    'fp_list': false_positives,
}
with open(os.path.join(OUTPUT_DIR, 'validation_results.json'), 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

# Rebuild dashboard
print(f"\nRebuilding dashboard...")
import subprocess
subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'build_dashboard.py')],
               capture_output=True, timeout=30)
print("Dashboard rebuilt!")

conn.close()
print("\nDone!")
