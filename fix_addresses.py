"""Fix 7 vague/incorrect addresses in scored_v2.json"""
import json
import os

path = os.path.join('output', 'scored_v2.json')
with open(path, encoding='utf-8') as f:
    data = json.load(f)

# Build lookup by id
by_id = {item['id']: item for item in data}

fixes = {
    # 1. Strathfield Medical Centre - KNOWN correct address
    8981: {
        'address': '3 Everton Road, Strathfield, Sydney, Strathfield Municipal Council, New South Wales, 2135, Australia',
        'lat': -33.8720,
        'lng': 151.0976,
    },
    # 2. SmartClinics Toowoomba - coords were near Mackay (-22.26,148.86), should be East Toowoomba
    #    Bell Street, East Toowoomba QLD 4350. No street number found but coords from Nominatim for Bell St.
    8705: {
        'address': 'Bell Street, East Toowoomba, Toowoomba, Toowoomba Regional, Queensland, 4350, Australia',
        'lat': -27.5602,
        'lng': 151.9569,
    },
    # 3. IGA Friendly Grocer - blank address. Reverse geocode confirms 40 Main Street, Derrinallum VIC 3325
    8505: {
        'address': '40 Main Street, Derrinallum, Shire of Corangamite, Victoria, 3325, Australia',
        'lat': -37.9478,
        'lng': 143.2228,
    },
    # 4. Marion Medical Centre - "Township Road, Marion" SA. The coords were slightly off from Township Rd.
    #    Update coords to centre of Township Road, Marion SA 5043.
    8103: {
        'address': 'Township Road, Marion, Adelaide, City of Marion, South Australia, 5043, Australia',
        'lat': -35.0117,
        'lng': 138.5553,
    },
    # 5. SmartClinics Adelaide - plotted to North Adelaide parklands (-34.9157, 138.6001).
    #    Address says "Stockley Alley, Adelaide" which is at -34.9265, 138.6041 in Adelaide CBD.
    #    SmartClinics doesn't operate in SA, so this is likely misnamed data.
    #    Fix coords to Stockley Alley location in Adelaide CBD.
    8105: {
        'address': 'Stockley Alley, Adelaide, Adelaide City Council, South Australia, 5000, Australia',
        'lat': -34.9265,
        'lng': 138.6041,
    },
    # 6. Canning Vale Medical Centre - was "Lydiard Retreat, Canning Vale".
    #    Healthpages.wiki confirms: Unit 10, 98 Waratah Boulevard, Canning Vale WA 6155
    #    OSM Nominatim has it on Woodhouse Circuit (nearby). Using Nominatim coords for the actual clinic.
    8212: {
        'address': 'Unit 10, 98 Waratah Boulevard, Canning Vale, City of Canning, Western Australia, 6155, Australia',
        'lat': -32.0739,
        'lng': 115.9132,
    },
    # 7. Lake Macquarie Medical Centre - was "Griffiths Street, Charlestown" NSW 2290.
    #    Healthpages.wiki confirms: Shop 15, 24-26 Brooks Parade, Belmont NSW 2280
    #    Nominatim coords for Brooks Parade, Belmont.
    8972: {
        'address': 'Shop 15, 24-26 Brooks Parade, Belmont, City of Lake Macquarie, New South Wales, 2280, Australia',
        'lat': -33.0342,
        'lng': 151.6569,
    },
}

print("Fixing entries:")
for entry_id, fix in fixes.items():
    if entry_id not in by_id:
        print(f"  WARNING: ID {entry_id} not found!")
        continue
    item = by_id[entry_id]
    old_addr = item.get('address', '')
    old_lat = item.get('lat')
    old_lng = item.get('lng')
    
    for key, val in fix.items():
        item[key] = val
    
    print(f"  ID {entry_id} ({item['name']}):")
    print(f"    Address: '{old_addr}' -> '{fix['address']}'")
    print(f"    Coords:  ({old_lat}, {old_lng}) -> ({fix['lat']}, {fix['lng']})")

# Save
with open(path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"\nSaved {path} with {len(fixes)} fixes applied.")
