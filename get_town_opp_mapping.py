import sqlite3, json

conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()

# Get all unique nearest_town + region combinations and their pharmacy distances
c.execute("""
    SELECT nearest_town, region, COUNT(*) as opp_count, 
           MAX(nearest_pharmacy_km) as max_pharm_dist,
           GROUP_CONCAT(id) as opp_ids
    FROM opportunities 
    WHERE nearest_town != ''
    GROUP BY nearest_town, region
    ORDER BY opp_count DESC
""")

town_opp_map = {}
for row in c.fetchall():
    key = f"{row[0]}_{row[1]}"
    town_opp_map[key] = {
        'town': row[0],
        'state': row[1],
        'opp_count': row[2],
        'max_pharm_dist': row[3],
        'opp_ids': [int(x) for x in row[4].split(',')]
    }

print(f"Total town-state combos: {len(town_opp_map)}")
print(f"Total opps covered: {sum(v['opp_count'] for v in town_opp_map.values())}")

# Load the town batches to see what we need to validate
with open('output/town_batches.json') as f:
    batches = json.load(f)

batch_towns = set()
for batch in batches['batches']:
    for t in batch:
        if not t.startswith('Unknown'):
            # Convert "Town, ST" to "Town_ST"
            parts = t.rsplit(', ', 1)
            key = f"{parts[0]}_{parts[1]}"
            batch_towns.add(key)

print(f"\nBatch towns (non-Unknown): {len(batch_towns)}")

# Check overlap
in_both = batch_towns & set(town_opp_map.keys())
in_batch_only = batch_towns - set(town_opp_map.keys())
print(f"In both: {len(in_both)}")
print(f"In batch only: {len(in_batch_only)}")
if in_batch_only:
    print(f"Batch-only towns: {sorted(in_batch_only)[:10]}")

conn.close()
