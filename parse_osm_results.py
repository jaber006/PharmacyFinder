"""Parse the OSM scraper log output and summarize findings"""
import re

log = open("osm_log.txt").read()

# Parse all OK results
ok_pattern = r'\[(\d+)/118\] (.+?) \((\w+)\) @ .+?\n\s+Current: est_gla=(\d+\.?\d*).+?\n\s+\[OK\] OSM: (\d+) sqm \(tagged: (.+?), dist: (\d+)m, supermarket_tag: (\w+)\)'
matches = re.findall(ok_pattern, log)

print(f"Successfully measured: {len(matches)}/118\n")

# Categorize
significant_changes = []
threshold_impacts = []

for m in matches:
    idx, name, brand, old_gla, new_gla, osm_name, dist, is_super = m
    old = float(old_gla)
    new = int(new_gla)
    change_pct = ((new - old) / old) * 100 if old > 0 else 0
    
    if abs(change_pct) > 20:
        significant_changes.append((name.strip(), brand, old, new, change_pct, osm_name, is_super))
    
    # Check threshold crossings
    thresholds = [(500, "Item 134"), (1000, "Item 132/134A"), (2500, "Item 133")]
    for thresh, rule in thresholds:
        if old >= thresh and new < thresh:
            threshold_impacts.append(("LOST", name.strip(), brand, old, new, rule))
        elif old < thresh and new >= thresh:
            threshold_impacts.append(("GAINED", name.strip(), brand, old, new, rule))

print("=== SIGNIFICANT CHANGES (>20% difference) ===")
for name, brand, old, new, pct, osm_name, is_super in sorted(significant_changes, key=lambda x: x[4]):
    tag = "[TAGGED]" if is_super == "True" else ""
    print(f"  {brand:12s} {name:40s}: {old:,.0f} -> {new:,} sqm ({pct:+.0f}%) {tag}")

print(f"\n=== RULE THRESHOLD IMPACTS ===")
for action, name, brand, old, new, rule in threshold_impacts:
    symbol = "[!!]" if action == "LOST" else "[++]"
    print(f"  {symbol} {action}: {name:40s} ({brand:12s}) {old:,.0f} -> {new:,} sqm -- {rule}")

# Summary stats
print(f"\n=== SUMMARY ===")
tagged = [m for m in matches if m[7] == "True"]
print(f"Total measured: {len(matches)}")
print(f"OSM supermarket-tagged (high confidence): {len(tagged)}")
print(f"Untagged buildings (medium confidence): {len(matches) - len(tagged)}")
print(f"Failed/rate-limited: {118 - len(matches) - 11}")  # rough estimate

# Brand averages
brands = {}
for m in matches:
    brand = m[2]
    new = int(m[4])
    if brand not in brands:
        brands[brand] = []
    brands[brand].append(new)

print(f"\n=== ACTUAL AVERAGE GLA BY BRAND (OSM measured) ===")
for brand, vals in sorted(brands.items()):
    avg = sum(vals) / len(vals)
    print(f"  {brand:15s}: avg {avg:,.0f} sqm (n={len(vals)}, range {min(vals):,}-{max(vals):,})")
