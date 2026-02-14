"""
Comprehensive pharmacy data supplementer.

Pulls pharmacy locations from multiple sources to build a near-complete
database of Australian pharmacies (~5,700+). Sources:

1. Funnelback API (findapharmacy.com.au) - full pagination, ~4,300 pharmacies
2. OpenStreetMap Overpass API - ~3,200 pharmacies (many overlap with #1)
3. Chemist Warehouse API - ~475 stores
4. TerryWhite Chemmart API - ~620 stores
5. Priceline Pharmacy API - ~520 stores
6. Manual additions (e.g., Evandale Pharmacy from healthdirect.gov.au)

Deduplication: pharmacies within 100m of an existing entry are skipped.

Usage:
    python supplement_pharmacies_all.py          # run all sources
    python supplement_pharmacies_all.py --source osm   # run only OSM
"""

import requests
import json
import time
import sqlite3
import math
import argparse
from datetime import datetime
from typing import List, Dict, Tuple, Set


DB_PATH = "pharmacy_finder.db"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

STATE_MAP = {
    'new south wales': 'NSW', 'victoria': 'VIC', 'queensland': 'QLD',
    'south australia': 'SA', 'western australia': 'WA', 'tasmania': 'TAS',
    'northern territory': 'NT', 'australian capital territory': 'ACT',
    'VICTORIA': 'VIC', 'NEW SOUTH WALES': 'NSW', 'QUEENSLAND': 'QLD',
    'SOUTH AUSTRALIA': 'SA', 'WESTERN AUSTRALIA': 'WA', 'TASMANIA': 'TAS',
    'NORTHERN TERRITORY': 'NT', 'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
    'VIC': 'VIC', 'NSW': 'NSW', 'QLD': 'QLD', 'SA': 'SA', 'WA': 'WA',
    'TAS': 'TAS', 'NT': 'NT', 'ACT': 'ACT',
}


def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km between two lat/lon points."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


class PharmacySupplementer:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.row_factory = sqlite3.Row
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.existing_coords: List[Tuple[float, float]] = []
        self.added = 0
        self.skipped = 0
        self._load_existing()

    def _load_existing(self):
        cur = self.conn.cursor()
        cur.execute("SELECT latitude, longitude FROM pharmacies")
        self.existing_coords = [(r[0], r[1]) for r in cur.fetchall()]

    def _is_duplicate(self, lat: float, lon: float, threshold_km: float = 0.1) -> bool:
        for elat, elon in self.existing_coords:
            if haversine_km(lat, lon, elat, elon) < threshold_km:
                return True
        return False

    def _insert(self, data: Dict) -> bool:
        lat = data.get('latitude')
        lon = data.get('longitude')
        if not lat or not lon:
            return False
        if not (-45 <= lat <= -10 and 110 <= lon <= 155):
            return False
        if self._is_duplicate(lat, lon):
            self.skipped += 1
            return False

        cur = self.conn.cursor()
        try:
            cur.execute("""
                INSERT OR IGNORE INTO pharmacies
                (name, address, latitude, longitude, source, date_scraped,
                 suburb, state, postcode, opening_hours)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.get('name', 'Unknown Pharmacy'),
                data.get('address', ''),
                lat, lon,
                data.get('source', ''),
                datetime.now().isoformat(),
                data.get('suburb', ''),
                data.get('state', ''),
                data.get('postcode', ''),
                data.get('opening_hours', ''),
            ))
            self.conn.commit()
            if cur.rowcount > 0:
                self.existing_coords.append((lat, lon))
                self.added += 1
                return True
        except Exception as e:
            pass
        return False

    def count(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM pharmacies")
        return cur.fetchone()[0]

    # ===== Source 1: Funnelback (findapharmacy.com.au) =====

    def source_funnelback(self):
        """Get all pharmacies from the Funnelback API via pagination."""
        print("\n=== Source 1: Funnelback API (findapharmacy.com.au) ===")
        before = self.count()

        url = "https://tpgoa-search.funnelback.squiz.cloud/s/search.html"
        seen_ids: Set[str] = set()
        all_results = []
        start = 1

        while True:
            params = {
                "collection": "tpgoa~sp-locations",
                "profile": "react-data",
                "query": "!null",
                "num_ranks": 1000,
                "sort": "prox",
                "serviceKeyword": "!null",
                "origin": "-25.27,133.77",
                "maxdist": "5000",
                "start_rank": start,
            }
            print(f"  Fetching ranks {start}-{start + 999}...")
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                results = resp.json().get("results", [])
                if not results:
                    break
                for r in results:
                    pid = r.get("id", "")
                    if pid not in seen_ids:
                        seen_ids.add(pid)
                        all_results.append(r)
                print(f"    {len(results)} results, {len(all_results)} unique total")
                if len(results) < 1000:
                    break
                start += 1000
                time.sleep(0.5)
            except Exception as e:
                print(f"    Error: {e}")
                break

        for r in all_results:
            coords = r.get("geometry", {}).get("coordinates", [])
            if len(coords) < 2:
                continue
            lon, lat = float(coords[0]), float(coords[1])
            name = r.get("name", "").strip()
            if not name:
                continue
            state_raw = r.get("state", "").strip().upper()
            state = STATE_MAP.get(state_raw, state_raw)
            city = r.get("city", "").strip()
            postcode = r.get("postcode", "").strip()
            addr_parts = [r.get(f, "").strip() for f in ("address", "address2", "address3")]
            addr_parts = [p for p in addr_parts if p]
            addr_parts.extend(filter(None, [city, state, postcode]))
            self._insert({
                'name': name, 'address': ', '.join(addr_parts),
                'latitude': lat, 'longitude': lon,
                'source': 'findapharmacy.com.au',
                'suburb': city, 'state': state, 'postcode': postcode,
            })

        after = self.count()
        print(f"  Added {after - before} new (total: {after})")

    # ===== Source 2: OpenStreetMap =====

    def source_osm(self):
        """Get all Australian pharmacies from OpenStreetMap."""
        print("\n=== Source 2: OpenStreetMap Overpass API ===")
        before = self.count()

        query = """
        [out:json][timeout:180];
        area["ISO3166-1"="AU"]->.au;
        (
          node["amenity"="pharmacy"](area.au);
          way["amenity"="pharmacy"](area.au);
          relation["amenity"="pharmacy"](area.au);
        );
        out center;
        """

        for api_url in [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
        ]:
            try:
                print(f"  Querying {api_url}...")
                resp = self.session.post(api_url, data={'data': query}, timeout=180)
                if resp.status_code != 200:
                    continue
                elements = resp.json().get('elements', [])
                print(f"  Got {len(elements)} elements")

                for el in elements:
                    tags = el.get('tags', {})
                    if el.get('type') == 'node':
                        lat, lon = el.get('lat'), el.get('lon')
                    else:
                        c = el.get('center', {})
                        lat, lon = c.get('lat'), c.get('lon')
                    if not lat or not lon:
                        continue
                    lat, lon = float(lat), float(lon)

                    name = tags.get('name', 'Unknown Pharmacy')
                    suburb = tags.get('addr:suburb', tags.get('addr:city', ''))
                    state = tags.get('addr:state', self._guess_state(lat, lon))
                    postcode = tags.get('addr:postcode', '')
                    addr_parts = []
                    if tags.get('addr:housenumber'):
                        addr_parts.append(tags['addr:housenumber'])
                    if tags.get('addr:street'):
                        addr_parts.append(tags['addr:street'])
                    if suburb:
                        addr_parts.append(suburb)
                    if state:
                        addr_parts.append(state)
                    if postcode:
                        addr_parts.append(postcode)

                    self._insert({
                        'name': name,
                        'address': ', '.join(addr_parts) if addr_parts else f"{name}, Australia",
                        'latitude': lat, 'longitude': lon,
                        'source': 'OpenStreetMap',
                        'suburb': suburb, 'state': state, 'postcode': postcode,
                    })
                break  # Success
            except Exception as e:
                print(f"  Error: {e}")

        after = self.count()
        print(f"  Added {after - before} new (total: {after})")

    def _guess_state(self, lat: float, lon: float) -> str:
        if -35.95 <= lat <= -35.1 and 148.7 <= lon <= 149.4:
            return 'ACT'
        bounds = {
            'NSW': ((-37.5, -28.0), (140.99, 153.64)),
            'VIC': ((-39.2, -34.0), (140.96, 150.03)),
            'QLD': ((-29.2, -10.0), (137.99, 153.55)),
            'SA':  ((-38.1, -26.0), (129.0, 141.01)),
            'WA':  ((-35.2, -13.5), (112.92, 129.01)),
            'TAS': ((-43.7, -39.5), (143.5, 148.5)),
            'NT':  ((-26.1, -10.9), (128.99, 138.01)),
        }
        for st, ((la, lb), (lo, lp)) in bounds.items():
            if la <= lat <= lb and lo <= lon <= lp:
                return st
        return ''

    # ===== Source 3: Chemist Warehouse =====

    def source_chemist_warehouse(self):
        """Get all Chemist Warehouse stores via their API."""
        print("\n=== Source 3: Chemist Warehouse API ===")
        before = self.count()

        url = 'https://api.chemistwarehouse.com.au/web/v1/channels/cwr-cw-au/en/radius'
        seen_keys: Set[str] = set()
        query_points = [
            (-33.87, 151.21), (-33.75, 150.69), (-33.43, 151.34), (-32.93, 151.78),
            (-34.42, 150.90), (-33.28, 149.10), (-32.25, 148.60), (-30.50, 153.00),
            (-34.75, 150.65), (-29.50, 153.25), (-31.90, 152.50), (-31.00, 150.80),
            (-35.30, 149.50), (-28.80, 153.30),
            (-37.81, 144.96), (-37.75, 145.15), (-37.95, 145.20), (-37.55, 144.85),
            (-38.15, 144.35), (-37.55, 143.85), (-36.76, 144.28), (-36.36, 145.39),
            (-38.10, 145.50), (-37.80, 147.00), (-36.00, 146.50),
            (-27.47, 153.03), (-27.95, 153.35), (-28.15, 153.48), (-26.55, 153.07),
            (-27.60, 152.70), (-27.50, 152.40), (-24.85, 152.35), (-23.38, 150.50),
            (-21.14, 149.19), (-19.26, 146.80), (-16.92, 145.77), (-20.00, 148.00),
            (-31.95, 115.86), (-31.75, 115.85), (-32.10, 115.85), (-33.30, 115.65),
            (-28.77, 114.62), (-21.00, 119.00), (-34.00, 117.50),
            (-34.93, 138.60), (-35.10, 138.55), (-34.70, 138.70), (-35.50, 138.50),
            (-33.50, 138.00),
            (-42.88, 147.33), (-41.44, 147.14), (-41.17, 146.37), (-41.05, 145.88),
            (-12.46, 130.84), (-23.70, 133.87),
            (-35.28, 149.13),
        ]

        self.session.headers['Referer'] = 'https://www.chemistwarehouse.com.au/store-locator'
        all_stores = []

        for lat, lon in query_points:
            params = {
                'channel-type': 'store',
                'latitude': str(lat), 'longitude': str(lon),
                'search-type': 'store-locator',
                'offset': '0', 'limit': '100',
            }
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 200:
                    for ch in resp.json().get('channels', []):
                        store = ch.get('channel', {})
                        key = store.get('key', '')
                        if key and key not in seen_keys:
                            seen_keys.add(key)
                            all_stores.append(store)
            except:
                pass
            time.sleep(0.3)

        print(f"  Found {len(all_stores)} unique CW stores")

        for store in all_stores:
            coords = store.get('coordinates', {})
            lat = coords.get('latitude')
            lon = coords.get('longitude')
            if not lat or not lon:
                continue
            addr = store.get('address', {})
            state_full = (addr.get('state', '') or '').lower()
            state = STATE_MAP.get(state_full, state_full.upper()[:3])
            self._insert({
                'name': store.get('name', 'Chemist Warehouse'),
                'address': ', '.join(filter(None, [
                    addr.get('streetNumber', ''), addr.get('city', ''),
                    state, addr.get('postalCode', '')
                ])),
                'latitude': float(lat), 'longitude': float(lon),
                'source': 'chemistwarehouse.com.au',
                'suburb': addr.get('city', ''),
                'state': state,
                'postcode': addr.get('postalCode', ''),
            })

        after = self.count()
        print(f"  Added {after - before} new (total: {after})")

    # ===== Source 4: TerryWhite Chemmart =====

    def source_terrywhite(self):
        """Get all TerryWhite Chemmart stores via their API."""
        print("\n=== Source 4: TerryWhite Chemmart API ===")
        before = self.count()

        self.session.headers.update({
            'Content-Type': 'application/json',
            'Referer': 'https://terrywhitechemmart.com.au/stores',
            'Origin': 'https://terrywhitechemmart.com.au',
        })
        try:
            resp = self.session.post(
                'https://terrywhitechemmart.com.au/store-api/get-stores-summary',
                json={}, timeout=30
            )
            if resp.status_code == 200:
                stores = resp.json().get('data', [])
                print(f"  Got {len(stores)} TWC stores")
                for s in stores:
                    lat, lng = s.get('lat'), s.get('lng')
                    if not lat or not lng:
                        continue
                    self._insert({
                        'name': s.get('storeName', 'TerryWhite Chemmart'),
                        'address': ', '.join(filter(None, [
                            s.get('addressLine1', ''), s.get('addressLine2', ''),
                            s.get('suburb', ''), s.get('state', ''), s.get('postcode', '')
                        ])),
                        'latitude': float(lat), 'longitude': float(lng),
                        'source': 'terrywhitechemmart.com.au',
                        'suburb': s.get('suburb', ''),
                        'state': s.get('state', ''),
                        'postcode': s.get('postcode', ''),
                    })
        except Exception as e:
            print(f"  Error: {e}")

        # Clean up headers
        self.session.headers.pop('Content-Type', None)
        self.session.headers.pop('Origin', None)

        after = self.count()
        print(f"  Added {after - before} new (total: {after})")

    # ===== Source 5: Priceline Pharmacy =====

    def source_priceline(self):
        """Get all Priceline Pharmacy stores via their SAP Hybris API."""
        print("\n=== Source 5: Priceline Pharmacy API ===")
        before = self.count()

        self.session.headers['Referer'] = 'https://www.priceline.com.au/pharmacy-finder'
        try:
            resp = self.session.get(
                'https://api.priceline.com.au/occ/v2/priceline/stores?pageSize=1000&fields=FULL',
                timeout=30
            )
            if resp.status_code == 200:
                stores = resp.json().get('stores', [])
                print(f"  Got {len(stores)} Priceline stores")
                for s in stores:
                    geo = s.get('geoPoint', {})
                    lat = geo.get('latitude')
                    lon = geo.get('longitude')
                    if not lat or not lon:
                        continue
                    addr = s.get('address', {})
                    region = addr.get('region', {})
                    region_name = region.get('name', '') if isinstance(region, dict) else str(region)
                    state = STATE_MAP.get(region_name.lower().strip(), region_name.upper()[:3])
                    town = addr.get('town', '')
                    postcode = addr.get('postalCode', '')
                    self._insert({
                        'name': s.get('displayName', s.get('name', 'Priceline Pharmacy')),
                        'address': ', '.join(filter(None, [
                            addr.get('line1', ''), addr.get('line2', ''),
                            town, state, postcode
                        ])),
                        'latitude': float(lat), 'longitude': float(lon),
                        'source': 'priceline.com.au',
                        'suburb': town, 'state': state, 'postcode': postcode,
                    })
        except Exception as e:
            print(f"  Error: {e}")

        after = self.count()
        print(f"  Added {after - before} new (total: {after})")

    # ===== Source 6: Manual additions =====

    def source_manual(self):
        """Add manually identified pharmacies missing from other sources."""
        print("\n=== Source 6: Manual additions ===")
        before = self.count()

        manual_pharmacies = [
            {
                'name': 'Evandale Pharmacy',
                'address': '8A High Street, EVANDALE, TAS, 7212',
                'latitude': -41.56942836,
                'longitude': 147.24636952,
                'source': 'healthdirect.gov.au',
                'suburb': 'EVANDALE', 'state': 'TAS', 'postcode': '7212',
            },
        ]

        for p in manual_pharmacies:
            self._insert(p)

        after = self.count()
        print(f"  Added {after - before} manual entries (total: {after})")

    # ===== Run all =====

    def run(self, sources=None):
        """Run all (or specified) sources."""
        print(f"Starting pharmacy supplement. Current count: {self.count()}")

        source_map = {
            'funnelback': self.source_funnelback,
            'osm': self.source_osm,
            'cw': self.source_chemist_warehouse,
            'twc': self.source_terrywhite,
            'priceline': self.source_priceline,
            'manual': self.source_manual,
        }

        if sources:
            for s in sources:
                if s in source_map:
                    source_map[s]()
                else:
                    print(f"Unknown source: {s}")
        else:
            for fn in source_map.values():
                fn()

        self._print_summary()
        self.conn.close()

    def _print_summary(self):
        total = self.count()
        cur = self.conn.cursor()

        print(f"\n{'=' * 60}")
        print(f"FINAL TOTAL: {total} pharmacies")
        print(f"Added: {self.added}, Duplicates skipped: {self.skipped}")

        cur.execute("SELECT source, COUNT(*) FROM pharmacies GROUP BY source ORDER BY COUNT(*) DESC")
        print("\nBy source:")
        for r in cur.fetchall():
            print(f"  {r[0]}: {r[1]}")

        cur.execute("SELECT state, COUNT(*) FROM pharmacies GROUP BY state ORDER BY COUNT(*) DESC")
        print("\nBy state:")
        for r in cur.fetchall():
            print(f"  {r[0]}: {r[1]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Supplement pharmacy database')
    parser.add_argument('--source', nargs='+',
                        choices=['funnelback', 'osm', 'cw', 'twc', 'priceline', 'manual'],
                        help='Specific source(s) to run')
    args = parser.parse_args()

    supplementer = PharmacySupplementer()
    supplementer.run(args.source)
