#!/usr/bin/env python3
"""Use the Medmate API to fetch all stores for Sigma chains."""

import requests
import json
import os

DATA_DIR = "chain_data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Origin': 'https://www.discountdrugstores.com.au',
    'Referer': 'https://www.discountdrugstores.com.au/',
}

# Business IDs from the data:
# 2 = Discount Drug Stores
# 4 = Amcal
# Let's discover more

def fetch_locations(referer, business_id=None):
    """Fetch all locations from the Medmate API."""
    headers = HEADERS.copy()
    headers['Referer'] = referer
    headers['Origin'] = '/'.join(referer.split('/')[:3])
    
    # Try various POST payloads
    payloads = [
        {},
        {'businessid': business_id},
        {'state': 'all'},
        {'latitude': -25, 'longitude': 135, 'radius': 99999},
    ]
    
    for payload in payloads:
        try:
            r = requests.post(
                'https://app.medmate.com.au/connect/api/get_locations',
                headers=headers,
                json=payload,
                timeout=15
            )
            print(f"  Payload {payload}: {r.status_code} {len(r.text)} bytes")
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and len(data) > 0:
                    print(f"  Found {len(data)} locations!")
                    return data
                elif isinstance(data, dict):
                    print(f"  Keys: {list(data.keys())}")
                    if 'locations' in data:
                        return data['locations']
                    if 'data' in data:
                        return data['data']
        except Exception as e:
            print(f"  Error: {e}")
    
    return []

def main():
    # Test with DDS first
    chains = {
        'DDS': ('https://www.discountdrugstores.com.au/', 2, 'dds.json'),
        'Amcal': ('https://www.amcal.com.au/', 4, 'amcal_api.json'),
        'SoulPattinson': ('https://soulpattinson.com.au/', None, 'soulpattinson.json'),
        'Guardian': ('https://www.guardianpharmacies.com.au/', None, 'guardian.json'),
    }
    
    for name, (referer, bid, filename) in chains.items():
        print(f"\n=== {name} ===")
        data = fetch_locations(referer, bid)
        
        if data:
            # Convert to standard format
            stores = []
            for s in data:
                stores.append({
                    'n': s.get('locationname', ''),
                    'a': s.get('address', ''),
                    'lat': float(s.get('latitude', 0) or 0),
                    'lng': float(s.get('longitude', 0) or 0),
                    'sub': s.get('suburb', ''),
                    'st': s.get('state', ''),
                    'pc': s.get('postcode', '')
                })
            
            filepath = os.path.join(DATA_DIR, filename)
            with open(filepath, 'w') as f:
                json.dump(stores, f, indent=2)
            print(f"  Saved {len(stores)} to {filename}")
        else:
            print(f"  No data found")

if __name__ == "__main__":
    main()
