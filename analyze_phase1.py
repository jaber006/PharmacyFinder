import json

with open('output/likely_verification.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print("=== FAILED (coords wrong) ===")
for item in data:
    if item['verified'] == False:
        print(f"  id={item['id']} {item['name']} ({item['poi_type']}, {item['state']})")
        print(f"    Issues: {item['issues']}")
        print(f"    Nominatim: {item['nominatim_address']}")
        print()

print("\n=== UNCERTAIN (POI not found on OSM but coords OK) ===")
for item in data:
    if item['verified'] is None:
        print(f"  id={item['id']} {item['name']} ({item['poi_type']}, {item['state']})")
        print(f"    Nominatim: {item['nominatim_address']}")
        print()

print("\n=== SHOPPING CENTRES (need tenant count + pharmacy check) ===")
for item in data:
    if item['poi_type'] == 'shopping_centre':
        print(f"  id={item['id']} {item['name']} ({item['state']}) verified={item['verified']}")
        print(f"    Nominatim: {item['nominatim_address']}")
        print()

print("\n=== HOSPITALS (need pharmacy-onsite check) ===")
for item in data:
    if item['poi_type'] == 'hospital':
        print(f"  id={item['id']} {item['name']} ({item['state']}) verified={item['verified']}")
        print(f"    Nominatim: {item['nominatim_address']}")
        print()

print("\n=== MEDICAL CENTRES (need pharmacy-nearby check) ===")
for item in data:
    if item['poi_type'] == 'medical_centre':
        print(f"  id={item['id']} {item['name']} ({item['state']}) verified={item['verified']}")
        print(f"    Nominatim: {item['nominatim_address']}")
        print()

print("\n=== DUPLICATE IDs (same name+coords appearing multiple times) ===")
seen = {}
for item in data:
    key = (item['name'], item['lat'], item['lng'])
    if key in seen:
        print(f"  DUPLICATE: id={item['id']} & id={seen[key]} -> {item['name']} ({item['lat']}, {item['lng']})")
    seen[key] = item['id']
