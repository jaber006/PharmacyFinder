"""
Phase 2: Deep verification of LIKELY opportunities.
- Use Overpass API to find pharmacies near each location
- Add detailed info for shopping centres, hospitals, medical centres
"""
import json
import time
import urllib.request
import urllib.parse
import ssl
import sys

sys.stdout.reconfigure(line_buffering=True)

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def overpass_pharmacies_near(lat, lng, radius_m=2000):
    """Find pharmacies within radius_m of coordinates using Overpass API."""
    query = f"""
    [out:json][timeout:25];
    (
      node["amenity"="pharmacy"](around:{radius_m},{lat},{lng});
      way["amenity"="pharmacy"](around:{radius_m},{lat},{lng});
    );
    out center;
    """
    url = "https://overpass-api.de/api/interpreter"
    data = urllib.parse.urlencode({'data': query}).encode()
    req = urllib.request.Request(url, data=data, headers={'User-Agent': 'PharmacyFinder/1.0'})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            pharmacies = []
            for elem in result.get('elements', []):
                tags = elem.get('tags', {})
                name = tags.get('name', 'Unnamed pharmacy')
                plat = elem.get('lat') or elem.get('center', {}).get('lat')
                plng = elem.get('lon') or elem.get('center', {}).get('lon')
                pharmacies.append({'name': name, 'lat': plat, 'lng': plng})
            return pharmacies
    except Exception as e:
        return [{'error': str(e)}]

def overpass_poi_check(lat, lng, radius_m=500):
    """Check what POIs exist near the given coordinates."""
    query = f"""
    [out:json][timeout:25];
    (
      node["amenity"](around:{radius_m},{lat},{lng});
      way["amenity"](around:{radius_m},{lat},{lng});
      node["shop"](around:{radius_m},{lat},{lng});
      way["shop"](around:{radius_m},{lat},{lng});
      node["healthcare"](around:{radius_m},{lat},{lng});
      way["healthcare"](around:{radius_m},{lat},{lng});
    );
    out center;
    """
    url = "https://overpass-api.de/api/interpreter"
    data = urllib.parse.urlencode({'data': query}).encode()
    req = urllib.request.Request(url, data=data, headers={'User-Agent': 'PharmacyFinder/1.0'})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            pois = []
            for elem in result.get('elements', []):
                tags = elem.get('tags', {})
                name = tags.get('name', '')
                amenity = tags.get('amenity', '')
                shop = tags.get('shop', '')
                healthcare = tags.get('healthcare', '')
                pois.append({
                    'name': name, 
                    'amenity': amenity,
                    'shop': shop,
                    'healthcare': healthcare,
                })
            return pois
    except Exception as e:
        return [{'error': str(e)}]


# Known data from web research
KNOWN_DATA = {
    # Shopping centres
    7973: {  # Westfield Woden
        'tenant_count': 266,
        'pharmacy_inside': True,
        'pharmacy_names': ['PharmaSave Pharmacy'],
        'notes': 'Wikipedia confirms 266 stores+services, 4 anchors (David Jones, Big W, Coles, Woolworths). PharmaSave Pharmacy confirmed inside via Westfield website.'
    },
    8382: {  # Eastland Shopping Centre (TAS)
        'tenant_count': 95,
        'pharmacy_inside': True,
        'pharmacy_names': ['Chemist Warehouse Eastlands'],
        'notes': 'australia-shoppings.com lists 95 stores. Chemist Warehouse confirmed at Shop G017 via healthdirect and localsearch.'
    },
    9364: {  # Eastlands Shopping Centre (TAS) - duplicate
        'tenant_count': 95,
        'pharmacy_inside': True,
        'pharmacy_names': ['Chemist Warehouse Eastlands'],
        'notes': 'DUPLICATE of id=8382. Same Eastlands Shopping Centre, Rosny Park. 95 stores, Chemist Warehouse inside.'
    },
    9365: {  # Channel Court Shopping Centre
        'tenant_count': 80,
        'pharmacy_inside': None,  # Need to verify
        'notes': 'Wikipedia: sub-regional shopping centre, 25,415 sqm GLA. Anchored by Big W and Woolworths. Website says "over 80 stores".'
    },
    8383: {  # Liberty Tower
        'tenant_count': None,
        'pharmacy_inside': None,
        'notes': 'Liberty Tower is a residential/commercial tower at 620 Collins St Melbourne CBD. NOT a traditional shopping centre. Unlikely to have many retail tenants.'
    },
    8354: {  # Melo Velo
        'tenant_count': None,
        'pharmacy_inside': None,
        'notes': 'Melo Velo appears to be a cafe/bike shop in Nannup, WA (pop ~1,300). NOT a shopping centre. Misclassified POI type.'
    },

    # State mismatches
    8512: {  # Mallacoota Medical Centre
        'state_fix': 'VIC',
        'notes': 'State should be VIC not NSW. Mallacoota is in East Gippsland, Victoria (3892). Medical centre confirmed on OSM.'
    },
    8587: {  # IGA Country Grocers & Liquor
        'state_fix': 'VIC',
        'notes': 'State should be VIC not NSW. Located in Trentham, Shire of Hepburn, Victoria (3458). IGA confirmed on OSM.'
    },
    8654: {  # Foodworks
        'state_fix': 'VIC',
        'notes': 'State should be VIC not NSW. Located in Wedderburn, Shire of Loddon, Victoria (3518). Foodworks confirmed on OSM.'
    },

    # Duplicates
    8370: {'duplicate_of': 7971, 'notes': 'DUPLICATE of id=7971 (Garran Medical Centre, ACT). Same coords.'},
    8987: {'duplicate_of': 7971, 'notes': 'DUPLICATE of id=7971 (Garran Medical Centre, ACT). Same coords.'},
    9206: {'duplicate_of': 8584, 'notes': 'DUPLICATE of id=8584 (Pakenham Medical Centre, VIC). Same coords.'},
    9243: {'duplicate_of': 8598, 'notes': 'DUPLICATE of id=8598 (Ritchies Supa IGA, NSW). Same coords.'},

    # Remote locations where pharmacies are extremely unlikely
    8062: {'notes': 'Milikapiti Health Centre on Melville Island (Tiwi Islands), NT. Very remote Aboriginal community. No pharmacy expected.'},
    8063: {'notes': 'Pirlangimpi Health Centre on Melville Island (Tiwi Islands), NT. Very remote Aboriginal community. No pharmacy expected.'},
    8088: {'notes': 'Piliyamanyirra Supermarket on Tiwi Islands, NT. Remote community.'},
    8743: {'notes': 'Mornington Island Hospital, QLD. Remote island community. No pharmacy expected.'},
}


def main():
    with open('output/likely_verification.json', 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    print(f"Phase 2: Deep verification of {len(results)} LIKELY items")
    print("=" * 60)
    
    for i, entry in enumerate(results):
        item_id = entry['id']
        print(f"\n[{i+1}/{len(results)}] id={item_id} {entry['name']} ({entry['poi_type']}, {entry['state']})")
        
        # Apply known data
        if item_id in KNOWN_DATA:
            kd = KNOWN_DATA[item_id]
            if 'tenant_count' in kd and kd['tenant_count']:
                entry['tenant_count'] = kd['tenant_count']
            if 'pharmacy_inside' in kd and kd['pharmacy_inside'] is not None:
                entry['pharmacy_inside'] = kd['pharmacy_inside']
            if 'state_fix' in kd:
                entry['issues'].append(f"STATE FIX: Should be {kd['state_fix']} not {entry['state']}")
                entry['state'] = kd['state_fix']
                entry['coords_correct'] = True  # Coords are correct, state label was wrong
            if 'duplicate_of' in kd:
                entry['issues'].append(f"DUPLICATE of id={kd['duplicate_of']}")
            if 'notes' in kd:
                entry['notes'].append(kd['notes'])
            if 'pharmacy_names' in kd:
                entry['notes'].append(f"Pharmacies in centre: {', '.join(kd['pharmacy_names'])}")
            print(f"  Applied known data")
        
        # Search for nearby pharmacies via Overpass (2km radius)
        print(f"  Searching Overpass for pharmacies within 2km...")
        pharmacies = overpass_pharmacies_near(entry['lat'], entry['lng'], 2000)
        
        if pharmacies and 'error' not in pharmacies[0]:
            pharmacy_names = [p['name'] for p in pharmacies]
            entry['notes'].append(f"OSM pharmacies within 2km: {pharmacy_names}")
            if len(pharmacies) > 0:
                # Check if any pharmacy is INSIDE (within ~100m) for shopping centres/hospitals
                import math
                for p in pharmacies:
                    if p.get('lat') and p.get('lng'):
                        dlat = p['lat'] - entry['lat']
                        dlng = p['lng'] - entry['lng']
                        dist_m = math.sqrt(dlat**2 + dlng**2) * 111000
                        if dist_m < 200:  # within 200m - likely inside same complex
                            if entry['poi_type'] in ('shopping_centre', 'hospital'):
                                if entry['pharmacy_inside'] is None:
                                    entry['pharmacy_inside'] = True
                                    entry['notes'].append(f"Pharmacy '{p['name']}' found within {dist_m:.0f}m (likely inside)")
                
                # Record as potential missing pharmacies (for pharmacy near search)
                entry['missing_pharmacies'] = pharmacy_names
        elif pharmacies and 'error' in pharmacies[0]:
            entry['notes'].append(f"Overpass error: {pharmacies[0]['error']}")
        else:
            entry['notes'].append("No pharmacies found within 2km on OSM")
            entry['missing_pharmacies'] = []
        
        time.sleep(2)  # Rate limit Overpass
        
        # For uncertain POIs, check what's actually at the location
        if entry['verified'] is None and entry['poi_found_osm'] == False:
            print(f"  Checking POIs near location...")
            pois = overpass_poi_check(entry['lat'], entry['lng'], 500)
            if pois and 'error' not in pois[0]:
                relevant_pois = [p for p in pois if p.get('name')]
                if relevant_pois:
                    poi_list = [f"{p['name']} ({p.get('amenity') or p.get('shop') or p.get('healthcare')})" for p in relevant_pois[:10]]
                    entry['notes'].append(f"POIs within 500m: {poi_list}")
                    # Check if any match our expected POI type
                    for p in relevant_pois:
                        if entry['poi_type'] in ('medical_centre', 'gp') and p.get('amenity') in ('doctors', 'clinic', 'hospital'):
                            entry['verified'] = True
                            entry['poi_found_osm'] = True
                            entry['notes'].append(f"Found matching medical facility: {p['name']}")
                            break
                        if entry['poi_type'] == 'supermarket' and p.get('shop') == 'supermarket':
                            entry['verified'] = True
                            entry['poi_found_osm'] = True
                            entry['notes'].append(f"Found matching supermarket: {p['name']}")
                            break
                        if entry['poi_type'] == 'hospital' and p.get('amenity') == 'hospital':
                            entry['verified'] = True
                            entry['poi_found_osm'] = True
                            entry['notes'].append(f"Found matching hospital: {p['name']}")
                            break
                        if entry['poi_type'] == 'shopping_centre' and p.get('shop') in ('mall', 'supermarket', 'department_store'):
                            entry['verified'] = True
                            entry['poi_found_osm'] = True
                            entry['notes'].append(f"Found matching retail: {p['name']}")
                            break
            time.sleep(2)
        
        # Update verified status based on all findings
        if entry['verified'] is False and item_id in KNOWN_DATA and 'state_fix' in KNOWN_DATA[item_id]:
            # State was just mislabeled, POI itself is verified
            entry['verified'] = True
        
        # Save progress every 5 items
        if (i + 1) % 5 == 0 or i == len(results) - 1:
            with open('output/likely_verification.json', 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"  [Saved progress]")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"Total: {len(results)}")
    print(f"Verified: {sum(1 for r in results if r['verified'] == True)}")
    print(f"Failed: {sum(1 for r in results if r['verified'] == False)}")
    print(f"Uncertain: {sum(1 for r in results if r['verified'] is None)}")
    print(f"With pharmacy inside: {sum(1 for r in results if r['pharmacy_inside'] == True)}")
    print(f"Duplicates: {sum(1 for r in results if any('DUPLICATE' in i for i in r['issues']))}")
    print(f"State fixes: {sum(1 for r in results if any('STATE FIX' in i for i in r['issues']))}")

if __name__ == '__main__':
    main()
