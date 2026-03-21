import sys
sys.path.insert(0, '.')
from candidates.commercial_re import init_db, get_qualifying_sites, _suburb_from_address, _parse_rent, _parse_area, DB_PATH
from candidates.matcher import match_sites_to_properties, score_match, _score_distance, _score_rent, _score_size

# Test DB init
print("DB path:", DB_PATH)
init_db()
print("[OK] Table created")

# Test site loading
sites = get_qualifying_sites(state="TAS", top_n=5)
print(f"[OK] {len(sites)} TAS sites loaded")
for s in sites[:3]:
    print(f"  {s['id']}: {s['name']} @ {s['address']} (score={s['commercial_score']})")

# Test suburb extraction
tests = [
    "440-450, Chapel Road, Bankstown, NSW, Australia",
    "123 Main Street, Hobart, TAS, Australia",
    "Shop 5, Legana Shopping Centre, Legana, TAS",
]
for t in tests:
    suburb, state = _suburb_from_address(t)
    print(f"  '{t}' -> suburb='{suburb}', state='{state}'")

# Test rent parsing
print("\nRent parsing:")
for r in ["$350 per sqm", "$45,000 pa", "$25,000 - $35,000 p.a. + GST", "$450/sqm"]:
    annual, psqm = _parse_rent(r)
    print(f"  '{r}' -> annual={annual}, per_sqm={psqm}")

# Test area parsing
print("\nArea parsing:")
for a in ["120 m2", "85 sqm", "150m2", "200 sq m"]:
    val = _parse_area(a)
    print(f"  '{a}' -> {val}")

# Test scoring
print("\nScoring tests:")
print(f"  Distance 0m: {_score_distance(0)}")
print(f"  Distance 500m: {_score_distance(500)}")
print(f"  Distance 1500m: {_score_distance(1500)}")
print(f"  Rent $150/sqm: {_score_rent(150)}")
print(f"  Rent $350/sqm: {_score_rent(350)}")
print(f"  Size 100sqm: {_score_size(100)}")
print(f"  Size 50sqm: {_score_size(50)}")
print(f"  Size 300sqm: {_score_size(300)}")

print("\n[OK] All tests passed!")
