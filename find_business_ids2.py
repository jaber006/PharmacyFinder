#!/usr/bin/env python3
"""Find more Medmate business IDs."""

import requests
import json

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Origin': 'https://www.amcal.com.au',
    'Referer': 'https://www.amcal.com.au/',
}

for bid in range(30, 100):
    try:
        r = requests.post(
            'https://app.medmate.com.au/connect/api/get_locations',
            headers=HEADERS,
            json={'businessid': bid},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                first_name = data[0].get('locationname', 'unknown')
                print(f"  BID {bid:3d}: {len(data):4d} locations - e.g. {first_name}")
    except Exception as e:
        pass
