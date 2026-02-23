"""Get unique towns from opportunities for batch pharmacy validation."""
import sqlite3, sys, io, json

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
DB_PATH = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Get unique town+state combos with their opportunity count and max score
c.execute("""SELECT nearest_town, region, COUNT(*) as opp_count, 
             MAX(composite_score) as max_score, MAX(nearest_pharmacy_km) as max_dist,
             MAX(pop_5km) as max_pop
             FROM opportunities 
             WHERE nearest_town IS NOT NULL AND nearest_town != ''
             GROUP BY nearest_town, region 
             ORDER BY max_score DESC""")

towns = c.fetchall()
print(f"Unique towns: {len(towns)}")
print(f"(These cover all 1,450 opportunities)\n")

# Group into batches of 25 for Perplexity queries
batches = []
batch = []
for town, state, count, score, dist, pop in towns:
    batch.append(f"{town}, {state}")
    if len(batch) == 25:
        batches.append(batch)
        batch = []
if batch:
    batches.append(batch)

print(f"Batches needed: {len(batches)}\n")

# Print first few batches
for i, b in enumerate(batches[:3], 1):
    print(f"Batch {i}:")
    print(f"  {', '.join(b)}")
    print()

# Save all batches
with open(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\town_batches.json', 'w') as f:
    json.dump({'batches': batches, 'total_towns': len(towns)}, f, indent=2)

# Also save town details
town_details = {}
for town, state, count, score, dist, pop in towns:
    key = f"{town}_{state}"
    town_details[key] = {
        'town': town, 'state': state, 'opp_count': count,
        'max_score': score, 'max_pharmacy_dist': dist, 'max_pop': pop
    }

with open(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\town_details.json', 'w') as f:
    json.dump(town_details, f, indent=2)

print(f"Saved to output/town_batches.json and output/town_details.json")

# Stats
towns_with_far_pharmacy = sum(1 for t in towns if t[4] > 5)
print(f"\nTowns where DB says nearest pharmacy > 5km: {towns_with_far_pharmacy}")
print(f"Towns where DB says nearest pharmacy > 20km: {sum(1 for t in towns if t[4] > 20)}")
print(f"Towns where DB says nearest pharmacy > 100km: {sum(1 for t in towns if t[4] > 100)}")

conn.close()
