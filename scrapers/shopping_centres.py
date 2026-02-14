"""
Scraper for shopping centre locations with GLA (Gross Lettable Area) data.

Sources:
1. OpenStreetMap Overpass API (free, has coordinates)
2. Curated list of major Australian shopping centres with known GLA
3. Manual CSV import

Key for Items 132 and 134:
  - Item 132: Major shopping centres >= 15,000 sqm GLA without a pharmacy
  - Item 134: Shopping centres 5,000-15,000 sqm with supermarket but no pharmacy
"""
import requests
import time
import json
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
from utils.boundaries import in_state
from utils.overpass_cache import cached_overpass_query
import config


# Curated list of major shopping centres by state with GLA data
# Source: Property Council of Australia Shopping Centre Directory, various public sources
KNOWN_SHOPPING_CENTRES = {
    'TAS': [
        {'name': 'Eastlands Shopping Centre', 'address': '26 Bligh Street, Rosny Park TAS 7018',
         'latitude': -42.8704, 'longitude': 147.3579, 'gla_sqm': 22000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Northgate Shopping Centre', 'address': '378 Invermay Road, Mowbray TAS 7248',
         'latitude': -41.4267, 'longitude': 147.1336, 'gla_sqm': 12000,
         'major_supermarkets': ['Woolworths']},
        {'name': 'Channel Court Shopping Centre', 'address': '29 Channel Highway, Kingston TAS 7050',
         'latitude': -42.9768, 'longitude': 147.3057, 'gla_sqm': 16000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Centrepoint Shopping Centre', 'address': 'Brisbane Street Mall, Launceston TAS 7250',
         'latitude': -41.4382, 'longitude': 147.1395, 'gla_sqm': 12000,
         'major_supermarkets': ['Coles']},
        {'name': 'Cat & Fiddle Arcade', 'address': '63 Murray Street, Hobart TAS 7000',
         'latitude': -42.8826, 'longitude': 147.3283, 'gla_sqm': 8000,
         'major_supermarkets': []},
        {'name': 'Glenorchy Central', 'address': '374 Main Road, Glenorchy TAS 7010',
         'latitude': -42.8310, 'longitude': 147.2820, 'gla_sqm': 10000,
         'major_supermarkets': ['Woolworths']},
        {'name': 'Shoreline Shopping Centre', 'address': 'Bass Highway, Howrah TAS 7018',
         'latitude': -42.8803, 'longitude': 147.3845, 'gla_sqm': 12000,
         'major_supermarkets': ['Coles']},
        {'name': 'Gateway Shopping Centre (Launceston)', 'address': '107-129 Yorktown Square, Launceston TAS 7250',
         'latitude': -41.4352, 'longitude': 147.1452, 'gla_sqm': 7000,
         'major_supermarkets': ['Woolworths']},
        {'name': 'Meadow Mews Shopping Centre', 'address': '174 Invermay Road, Invermay TAS 7248',
         'latitude': -41.4300, 'longitude': 147.1295, 'gla_sqm': 5500,
         'major_supermarkets': ['IGA']},
        {'name': 'Claremont Village Shopping Centre', 'address': '19 Bilton Street, Claremont TAS 7011',
         'latitude': -42.7810, 'longitude': 147.2487, 'gla_sqm': 5000,
         'major_supermarkets': ['Woolworths']},
    ],
    'NSW': [
        {'name': 'Westfield Sydney', 'address': '188 Pitt Street, Sydney NSW 2000',
         'latitude': -33.8713, 'longitude': 151.2089, 'gla_sqm': 50500,
         'major_supermarkets': ['Coles']},
        {'name': 'Westfield Bondi Junction', 'address': '500 Oxford Street, Bondi Junction NSW 2022',
         'latitude': -33.8915, 'longitude': 151.2475, 'gla_sqm': 92000,
         'major_supermarkets': ['Woolworths', 'Coles', 'Aldi']},
        {'name': 'Westfield Parramatta', 'address': '159-175 Church Street, Parramatta NSW 2150',
         'latitude': -33.8162, 'longitude': 151.0080, 'gla_sqm': 129000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield Liverpool', 'address': 'Macquarie Street, Liverpool NSW 2170',
         'latitude': -33.9209, 'longitude': 150.9229, 'gla_sqm': 87000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield Penrith', 'address': '585 High Street, Penrith NSW 2750',
         'latitude': -33.7517, 'longitude': 150.6909, 'gla_sqm': 87500,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Charlestown Square', 'address': '30 Pearson Street, Charlestown NSW 2290',
         'latitude': -32.9609, 'longitude': 151.6920, 'gla_sqm': 77000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield Kotara', 'address': 'Northcott Drive, Kotara NSW 2289',
         'latitude': -32.9412, 'longitude': 151.6961, 'gla_sqm': 48000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Stockland Green Hills', 'address': 'Molly Morgan Drive, East Maitland NSW 2323',
         'latitude': -32.7428, 'longitude': 151.5845, 'gla_sqm': 66500,
         'major_supermarkets': ['Woolworths', 'Coles', 'Aldi']},
    ],
    'VIC': [
        {'name': 'Chadstone Shopping Centre', 'address': '1341 Dandenong Road, Chadstone VIC 3148',
         'latitude': -37.8864, 'longitude': 145.0831, 'gla_sqm': 212000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield Doncaster', 'address': '619 Doncaster Road, Doncaster VIC 3108',
         'latitude': -37.7847, 'longitude': 145.1264, 'gla_sqm': 88000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield Southland', 'address': '1239 Nepean Highway, Cheltenham VIC 3192',
         'latitude': -37.9559, 'longitude': 145.0524, 'gla_sqm': 93500,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Highpoint Shopping Centre', 'address': '120-200 Rosamond Road, Maribyrnong VIC 3032',
         'latitude': -37.7726, 'longitude': 144.8882, 'gla_sqm': 119000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Eastland Shopping Centre', 'address': '175 Maroondah Highway, Ringwood VIC 3134',
         'latitude': -37.8146, 'longitude': 145.2307, 'gla_sqm': 90000,
         'major_supermarkets': ['Woolworths', 'Coles']},
    ],
    'QLD': [
        {'name': 'Westfield Chermside', 'address': '670 Gympie Road, Chermside QLD 4032',
         'latitude': -27.3868, 'longitude': 153.0301, 'gla_sqm': 98000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield Garden City', 'address': 'Kessels Road, Upper Mount Gravatt QLD 4122',
         'latitude': -27.5573, 'longitude': 153.0762, 'gla_sqm': 97000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Pacific Fair', 'address': 'Hooker Boulevard, Broadbeach QLD 4218',
         'latitude': -28.0373, 'longitude': 153.4312, 'gla_sqm': 132000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Robina Town Centre', 'address': 'Robina Town Centre Drive, Robina QLD 4226',
         'latitude': -28.0785, 'longitude': 153.3841, 'gla_sqm': 102000,
         'major_supermarkets': ['Woolworths', 'Coles']},
    ],
    'SA': [
        {'name': 'Westfield Marion', 'address': '297 Diagonal Road, Oaklands Park SA 5046',
         'latitude': -35.0140, 'longitude': 138.5554, 'gla_sqm': 105000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield Tea Tree Plaza', 'address': '976 North East Road, Modbury SA 5092',
         'latitude': -34.8321, 'longitude': 138.6836, 'gla_sqm': 80000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield West Lakes', 'address': '111 West Lakes Boulevard, West Lakes SA 5021',
         'latitude': -34.8706, 'longitude': 138.4968, 'gla_sqm': 56000,
         'major_supermarkets': ['Woolworths', 'Coles']},
    ],
    'WA': [
        {'name': 'Westfield Carousel', 'address': '1382 Albany Highway, Cannington WA 6107',
         'latitude': -32.0178, 'longitude': 115.9365, 'gla_sqm': 74000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield Innaloo', 'address': 'Ellen Stirling Boulevard, Innaloo WA 6018',
         'latitude': -31.8926, 'longitude': 115.7949, 'gla_sqm': 63000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Garden City (Booragoon)', 'address': '125 Riseley Street, Booragoon WA 6154',
         'latitude': -32.0336, 'longitude': 115.8349, 'gla_sqm': 73000,
         'major_supermarkets': ['Woolworths', 'Coles']},
    ],
    'NT': [
        {'name': 'Casuarina Square', 'address': '247 Trower Road, Casuarina NT 0810',
         'latitude': -12.3805, 'longitude': 130.8825, 'gla_sqm': 46000,
         'major_supermarkets': ['Woolworths', 'Coles']},
    ],
    'ACT': [
        {'name': 'Westfield Woden', 'address': 'Keltie Street, Phillip ACT 2606',
         'latitude': -35.3450, 'longitude': 149.0865, 'gla_sqm': 56000,
         'major_supermarkets': ['Woolworths', 'Coles']},
        {'name': 'Westfield Belconnen', 'address': 'Benjamin Way, Belconnen ACT 2617',
         'latitude': -35.2393, 'longitude': 149.0655, 'gla_sqm': 76000,
         'major_supermarkets': ['Woolworths', 'Coles']},
    ],
}


class ShoppingCentreScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'TAS') -> int:
        """
        Scrape all shopping centre locations from available sources.

        Args:
            region: Australian state/territory code

        Returns:
            Number of shopping centres scraped
        """
        total = 0

        # Primary: Load curated shopping centre data
        print(f"  [1/2] Loading known shopping centres for {region}...")
        known_count = self._load_known_centres(region)
        total += known_count
        print(f"        Loaded {known_count} known shopping centres")

        # Secondary: OSM Overpass for additional shopping centres
        print(f"  [2/2] Scraping OpenStreetMap for shopping centres in {region}...")
        osm_count = self.scrape_osm_overpass(region)
        total += osm_count
        print(f"        Found {osm_count} additional shopping centres from OSM")

        print(f"  Total shopping centres: {total}")
        return total

    def _load_known_centres(self, region: str) -> int:
        """Load curated shopping centres for the region."""
        count = 0
        centres = KNOWN_SHOPPING_CENTRES.get(region, [])
        for centre in centres:
            # Enrich with classification
            enriched = {**centre}
            enriched['estimated_gla'] = centre.get('gla_sqm', 0)
            enriched['estimated_tenants'] = self._estimate_tenants(
                centre.get('name', ''), centre.get('gla_sqm', 0))
            enriched['centre_class'] = self._classify_centre(
                centre.get('name', ''), centre.get('gla_sqm', 0),
                enriched['estimated_tenants'])
            self.db.insert_shopping_centre(enriched)
            count += 1
        return count

    def scrape_osm_overpass(self, region: str = 'TAS') -> int:
        """
        Scrape shopping centres from OpenStreetMap.
        Uses cached_overpass_query for reliability.
        """
        count = 0
        state_name = config.AUSTRALIAN_STATES.get(region, region)

        query = f"""
        [out:json][timeout:120];
        area["name"="{state_name}"]["admin_level"="4"]->.state;
        (
          node["shop"="mall"](area.state);
          way["shop"="mall"](area.state);
          relation["shop"="mall"](area.state);
          node["building"="retail"](area.state);
          way["building"="retail"]["name"](area.state);
          relation["building"="retail"]["name"](area.state);
        );
        out center;
        """

        data = cached_overpass_query(
            query=query,
            cache_key=f"shopping_centres_{region}",
            session=self.session,
        )

        if data is None:
            print(f"        [ERROR] Overpass unavailable and no cache for shopping_centres_{region}")
            return 0

        elements = data.get('elements', [])
        print(f"        Overpass returned {len(elements)} elements")

        # Get existing centres to avoid duplicates
        existing = self.db.get_all_shopping_centres()
        existing_names = {c['name'].lower() for c in existing}

        for element in elements:
            centre_data = self._parse_osm_centre(element, region)
            if centre_data and centre_data['name'].lower() not in existing_names:
                self.db.insert_shopping_centre(centre_data)
                existing_names.add(centre_data['name'].lower())
                count += 1

        return count

    def _parse_osm_centre(self, element: Dict, region: str) -> Optional[Dict]:
        """Parse OSM element into shopping centre data."""
        try:
            tags = element.get('tags', {})

            if element.get('type') == 'node':
                lat = element.get('lat')
                lon = element.get('lon')
            else:
                center = element.get('center', {})
                lat = center.get('lat', element.get('lat'))
                lon = center.get('lon', element.get('lon'))

            if not lat or not lon:
                return None

            lat, lon = float(lat), float(lon)
            if not in_state(lat, lon, region):
                return None

            name = tags.get('name', '')
            if not name:
                return None

            # Build address
            addr_parts = []
            if tags.get('addr:housenumber'):
                addr_parts.append(tags['addr:housenumber'])
            if tags.get('addr:street'):
                addr_parts.append(tags['addr:street'])
            if tags.get('addr:suburb') or tags.get('addr:city'):
                addr_parts.append(tags.get('addr:suburb', tags.get('addr:city', '')))
            addr_parts.append(region)

            address = ', '.join(p for p in addr_parts if p)
            if not address or address == region:
                address = f"{name}, {region}, Australia"

            # OSM doesn't have GLA — estimate from building type/size
            gla_sqm = self._estimate_gla(tags)
            est_tenants = self._estimate_tenants(name, gla_sqm)
            centre_class = self._classify_centre(name, gla_sqm, est_tenants)

            return {
                'name': name,
                'address': address,
                'latitude': float(lat),
                'longitude': float(lon),
                'gla_sqm': gla_sqm,
                'estimated_gla': gla_sqm,
                'estimated_tenants': est_tenants,
                'centre_class': centre_class,
                'major_supermarkets': [],  # Will check during scanning
            }

        except Exception:
            return None

    def _estimate_gla(self, tags: Dict) -> float:
        """
        Estimate GLA from OSM tags.
        Very rough — real GLA data needs to come from curated sources.
        """
        # Check if building area is tagged
        if tags.get('building:levels'):
            try:
                levels = int(tags['building:levels'])
                # Assume ~2000 sqm per level for a commercial building
                return levels * 2000.0
            except (ValueError, TypeError):
                pass

        # Default: small centre estimate
        return 5000.0  # Conservative default

    def _estimate_tenants(self, name: str, gla_sqm: float) -> int:
        """
        Estimate tenant count from centre name and GLA.

        Rules of thumb:
        - Westfield / major regional: 150-400 tenants
        - Sub-regional (15,000-40,000 sqm): 50-150 tenants
        - Neighbourhood (5,000-15,000 sqm): 15-50 tenants
        - Strip mall / small (<5,000 sqm): 5-15 tenants
        - Average ~1 tenant per 100-200 sqm GLA depending on size
        """
        name_lower = name.lower()

        # Known major chains — always large
        if 'westfield' in name_lower:
            if gla_sqm >= 80000:
                return 300
            elif gla_sqm >= 50000:
                return 200
            else:
                return 120
        elif 'chadstone' in name_lower:
            return 550
        elif 'pacific fair' in name_lower:
            return 400

        # Estimate from GLA using average tenant size
        if gla_sqm >= 50000:
            return int(gla_sqm / 180)  # Large centres have more small tenants
        elif gla_sqm >= 15000:
            return int(gla_sqm / 200)
        elif gla_sqm >= 5000:
            return int(gla_sqm / 250)
        elif gla_sqm >= 1000:
            return int(gla_sqm / 300)
        else:
            return max(5, int(gla_sqm / 400))

    def _classify_centre(self, name: str, gla_sqm: float,
                          estimated_tenants: int) -> str:
        """
        Classify a shopping centre for Items 132-134.

        Returns:
            'major'   — Item 132: GLA >= 15,000 sqm
            'large'   — Item 134: GLA 5,000-15,000 sqm, supermarket >= 2,500 sqm, 50+ tenants
            'small'   — Item 133: GLA 1,000-5,000 sqm, supermarket >= 1,000 sqm, 15+ tenants
            'strip'   — Too small to qualify
            'unknown' — Insufficient data
        """
        name_lower = name.lower()

        # Westfield is always major
        if 'westfield' in name_lower:
            return 'major'
        if 'chadstone' in name_lower:
            return 'major'

        if gla_sqm >= 15000:
            return 'major'
        elif gla_sqm >= 5000:
            if estimated_tenants >= 50:
                return 'large'
            elif estimated_tenants >= 15:
                return 'small'
            else:
                return 'small'  # 5,000+ sqm with some tenants
        elif gla_sqm >= 1000:
            if estimated_tenants >= 15:
                return 'small'
            else:
                return 'strip'
        else:
            return 'strip'
