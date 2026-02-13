"""
Scraper for commercial real estate listings (retail/medical tenancies for lease).

Strategy: Scrape realcommercial.com.au and commercialrealestate.com.au 
for retail/medical tenancies in the target region.

Also supports manual entry and CSV import.
"""
import requests
import time
import re
import json
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
import config


# Mapping of states to realcommercial.com.au region slugs
STATE_REGION_SLUGS = {
    'TAS': 'tas',
    'NSW': 'nsw',
    'VIC': 'vic',
    'QLD': 'qld',
    'SA': 'sa',
    'WA': 'wa',
    'NT': 'nt',
    'ACT': 'act',
}


class CommercialREScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-AU,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })

    def scrape_all(self, region: str = 'TAS', limit: int = 100) -> int:
        """
        Scrape commercial property listings from multiple sources.

        Args:
            region: Australian state/territory code
            limit: Maximum properties to scrape

        Returns:
            Number of properties scraped
        """
        total = 0

        # Try realcommercial.com.au
        print(f"  [1/2] Scraping realcommercial.com.au for {region}...")
        rc_count = self.scrape_realcommercial(region, limit)
        total += rc_count
        print(f"        Found {rc_count} properties")

        # Try commercialrealestate.com.au
        print(f"  [2/2] Scraping commercialrealestate.com.au for {region}...")
        cre_count = self.scrape_commercialrealestate(region, max(0, limit - total))
        total += cre_count
        print(f"        Found {cre_count} additional properties")

        if total == 0:
            print("  NOTE: Web scraping returned 0 results (sites may block scrapers).")
            print("        You can add properties manually or import from CSV.")
            print("        Use: python -c \"from scrapers.commercial_re import CommercialREScraper; ...\"")
            print("        Or generate sample data with --generate-samples")

        print(f"  Total properties: {total}")
        return total

    def scrape_realcommercial(self, region: str = 'TAS', limit: int = 50) -> int:
        """
        Scrape from realcommercial.com.au.
        
        Args:
            region: State code
            limit: Max properties
            
        Returns:
            Number of properties scraped
        """
        count = 0
        slug = STATE_REGION_SLUGS.get(region, region.lower())

        # realcommercial.com.au URL structure for retail/medical for lease
        urls = [
            f"https://www.realcommercial.com.au/for-lease/in-{slug}/list-1?activeSort=list-date&propertyTypes=retail",
            f"https://www.realcommercial.com.au/for-lease/in-{slug}/list-1?activeSort=list-date&propertyTypes=medical+%2F+consulting",
        ]

        for url in urls:
            if count >= limit:
                break

            try:
                time.sleep(config.SCRAPER_CONFIG['rate_limit_delay'])
                response = self.session.get(url, timeout=30)

                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Try multiple selector patterns
                listings = self._extract_listings_from_html(soup, 'realcommercial')
                
                for listing in listings[:limit - count]:
                    if listing.get('address'):
                        coords = self.geocoder.geocode(listing['address'])
                        if coords:
                            listing['latitude'] = coords[0]
                            listing['longitude'] = coords[1]
                            self.db.insert_property(listing)
                            count += 1
                        time.sleep(0.5)

            except Exception as e:
                print(f"        Error scraping realcommercial: {e}")

        return count

    def scrape_commercialrealestate(self, region: str = 'TAS', limit: int = 50) -> int:
        """
        Scrape from commercialrealestate.com.au.
        """
        count = 0
        slug = STATE_REGION_SLUGS.get(region, region.lower())

        urls = [
            f"https://www.commercialrealestate.com.au/for-lease/retail/in-{slug}/",
            f"https://www.commercialrealestate.com.au/for-lease/medical-consulting/in-{slug}/",
        ]

        for url in urls:
            if count >= limit:
                break

            try:
                time.sleep(config.SCRAPER_CONFIG['rate_limit_delay'])
                response = self.session.get(url, timeout=30)

                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                listings = self._extract_listings_from_html(soup, 'cre')

                for listing in listings[:limit - count]:
                    if listing.get('address'):
                        coords = self.geocoder.geocode(listing['address'])
                        if coords:
                            listing['latitude'] = coords[0]
                            listing['longitude'] = coords[1]
                            self.db.insert_property(listing)
                            count += 1
                        time.sleep(0.5)

            except Exception as e:
                print(f"        Error scraping commercialrealestate: {e}")

        return count

    def _extract_listings_from_html(self, soup: BeautifulSoup, source: str) -> List[Dict]:
        """
        Extract property listings from parsed HTML.
        Tries multiple CSS selector patterns to handle different site structures.
        """
        listings = []

        # Common selectors for commercial RE sites
        selectors = [
            # realcommercial patterns
            {'container': '[class*="listing"]', 'address': '[class*="address"]',
             'link': 'a[href*="/property/"]', 'type': '[class*="type"]'},
            # CRE patterns
            {'container': '[class*="PropertyCard"]', 'address': '[class*="address"]',
             'link': 'a[href*="/property/"]', 'type': '[class*="property-type"]'},
            # Generic patterns
            {'container': 'article', 'address': '[class*="address"]',
             'link': 'a[href]', 'type': '[class*="type"]'},
            {'container': '.search-result', 'address': '.address',
             'link': 'a[href]', 'type': '.property-type'},
        ]

        for sel in selectors:
            containers = soup.select(sel['container'])
            if not containers:
                continue

            for container in containers:
                listing = self._parse_container(container, sel, source)
                if listing and listing.get('address'):
                    listings.append(listing)

            if listings:
                break

        # Fallback: look for JSON-LD structured data
        if not listings:
            listings = self._extract_json_ld(soup, source)

        # Fallback: look for __NEXT_DATA__ or similar JSON payloads
        if not listings:
            listings = self._extract_json_payload(soup, source)

        return listings

    def _parse_container(self, container, selectors: Dict, source: str) -> Optional[Dict]:
        """Parse a listing container element."""
        try:
            # Extract address
            addr_elem = container.select_one(selectors['address'])
            address = addr_elem.get_text(strip=True) if addr_elem else None
            if not address:
                return None

            # Clean up address
            address = re.sub(r'\s+', ' ', address).strip()
            if len(address) < 5:
                return None

            # Extract listing URL
            link_elem = container.select_one(selectors['link'])
            listing_url = None
            if link_elem and link_elem.get('href'):
                href = link_elem['href']
                if href.startswith('/'):
                    if source == 'realcommercial':
                        listing_url = f"https://www.realcommercial.com.au{href}"
                    else:
                        listing_url = f"https://www.commercialrealestate.com.au{href}"
                elif href.startswith('http'):
                    listing_url = href

            # Extract property type
            type_elem = container.select_one(selectors['type'])
            property_type = type_elem.get_text(strip=True) if type_elem else 'Retail'

            # Extract size
            size_sqm = self._extract_size(container.get_text())

            # Extract agent info
            agent_name, agent_phone, agent_email = self._extract_agent(container)

            return {
                'address': address,
                'latitude': None,
                'longitude': None,
                'listing_url': listing_url,
                'property_type': property_type,
                'size_sqm': size_sqm,
                'agent_name': agent_name,
                'agent_phone': agent_phone,
                'agent_email': agent_email,
            }

        except Exception:
            return None

    def _extract_json_ld(self, soup: BeautifulSoup, source: str) -> List[Dict]:
        """Extract listings from JSON-LD structured data."""
        listings = []
        
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    for item in data:
                        listing = self._json_ld_to_listing(item, source)
                        if listing:
                            listings.append(listing)
                elif isinstance(data, dict):
                    listing = self._json_ld_to_listing(data, source)
                    if listing:
                        listings.append(listing)
            except (json.JSONDecodeError, TypeError):
                continue

        return listings

    def _json_ld_to_listing(self, data: Dict, source: str) -> Optional[Dict]:
        """Convert JSON-LD item to listing dict."""
        try:
            item_type = data.get('@type', '')
            if 'Product' not in item_type and 'Place' not in item_type and 'RealEstate' not in item_type:
                return None

            address = data.get('address', '')
            if isinstance(address, dict):
                parts = [address.get('streetAddress', ''), address.get('addressLocality', ''),
                         address.get('addressRegion', ''), address.get('postalCode', '')]
                address = ', '.join(p for p in parts if p)

            if not address:
                return None

            return {
                'address': address,
                'latitude': None,
                'longitude': None,
                'listing_url': data.get('url'),
                'property_type': 'Retail',
                'size_sqm': None,
                'agent_name': None,
                'agent_phone': None,
                'agent_email': None,
            }
        except Exception:
            return None

    def _extract_json_payload(self, soup: BeautifulSoup, source: str) -> List[Dict]:
        """Extract listings from embedded JSON data (Next.js, etc.)."""
        listings = []

        for script in soup.find_all('script'):
            if not script.string:
                continue
            
            # Look for common patterns
            text = script.string
            if '__NEXT_DATA__' in text or 'window.__data' in text or '"listings"' in text:
                try:
                    # Extract JSON from assignment
                    match = re.search(r'=\s*(\{.*\})\s*;?\s*$', text, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        # Recursively look for listing-like objects
                        self._find_listings_in_json(data, listings, source)
                except (json.JSONDecodeError, TypeError):
                    continue

        return listings

    def _find_listings_in_json(self, data, listings: List[Dict], source: str, depth: int = 0):
        """Recursively find listing objects in JSON data."""
        if depth > 5:
            return

        if isinstance(data, dict):
            # Check if this looks like a listing
            if 'address' in data and ('propertyType' in data or 'listing' in str(data.get('url', '')).lower()):
                addr = data.get('address', '')
                if isinstance(addr, dict):
                    parts = [addr.get('street', ''), addr.get('suburb', ''),
                             addr.get('state', ''), addr.get('postcode', '')]
                    addr = ', '.join(str(p) for p in parts if p)

                if addr and len(str(addr)) > 5:
                    listings.append({
                        'address': str(addr),
                        'latitude': data.get('latitude'),
                        'longitude': data.get('longitude'),
                        'listing_url': data.get('url'),
                        'property_type': data.get('propertyType', 'Retail'),
                        'size_sqm': data.get('size') or data.get('floorArea'),
                        'agent_name': data.get('agentName'),
                        'agent_phone': data.get('agentPhone'),
                        'agent_email': data.get('agentEmail'),
                    })
            else:
                for key, value in data.items():
                    self._find_listings_in_json(value, listings, source, depth + 1)

        elif isinstance(data, list):
            for item in data:
                self._find_listings_in_json(item, listings, source, depth + 1)

    def _extract_size(self, text: str) -> Optional[float]:
        """Extract size in sqm from text."""
        try:
            text = text.replace(',', '')
            # Match patterns like "150 sqm", "150m²", "150 m2"
            match = re.search(r'(\d+\.?\d*)\s*(?:sqm|m²|m2|square\s*m)', text, re.IGNORECASE)
            if match:
                return float(match.group(1))
        except Exception:
            pass
        return None

    def _extract_agent(self, container) -> tuple:
        """Extract agent information from listing container."""
        agent_name = None
        agent_phone = None
        agent_email = None

        try:
            # Look for agent elements
            for sel in ['.agent', '[class*="agent"]', '[class*="Agent"]']:
                agent_elem = container.select_one(sel)
                if agent_elem:
                    name_elem = agent_elem.select_one('[class*="name"]')
                    agent_name = name_elem.get_text(strip=True) if name_elem else None

                    phone_elem = agent_elem.select_one('a[href^="tel:"]')
                    if phone_elem:
                        agent_phone = phone_elem.get('href', '').replace('tel:', '')

                    email_elem = agent_elem.select_one('a[href^="mailto:"]')
                    if email_elem:
                        agent_email = email_elem.get('href', '').replace('mailto:', '')

                    break
        except Exception:
            pass

        return agent_name, agent_phone, agent_email

    def generate_sample_properties(self, region: str = 'TAS', count: int = 20) -> int:
        """
        Generate sample commercial properties for testing.
        Uses known commercial areas in the target region.
        
        Args:
            region: State code
            count: Number of sample properties to generate
            
        Returns:
            Number of properties generated
        """
        sample_locations = {
            'TAS': [
                ('Shop 1, 123 Elizabeth Street, Hobart TAS 7000', 'Retail', -42.8826, 147.3295),
                ('Suite 2, 45 Brisbane Street, Launceston TAS 7250', 'Medical', -41.4332, 147.1387),
                ('Shop 3, 78 Rooke Street, Devonport TAS 7310', 'Retail', -41.1796, 146.3518),
                ('Unit 4, 90 Wilson Street, Burnie TAS 7320', 'Retail', -41.0547, 145.9070),
                ('12 Channel Highway, Kingston TAS 7050', 'Retail', -42.9756, 147.2936),
                ('Shop 5, 330 Main Road, Glenorchy TAS 7010', 'Retail', -42.8317, 147.2791),
                ('Unit 1, 2 Bay Road, New Town TAS 7008', 'Medical', -42.8587, 147.3102),
                ('15 Main Road, Moonah TAS 7009', 'Retail', -42.8431, 147.2971),
                ('Shop 6, Shoreline Plaza, Howrah TAS 7018', 'Retail', -42.8744, 147.3847),
                ('3 Bayfield Street, Rosny Park TAS 7018', 'Retail', -42.8727, 147.3598),
                ('Unit 2, 10 Formby Road, Devonport TAS 7310', 'Medical', -41.1760, 146.3488),
                ('Shop 8, Wellington Street, Longford TAS 7301', 'Retail', -41.6018, 147.1181),
                ('45 High Street, Campbell Town TAS 7210', 'Retail', -41.9308, 147.4877),
                ('Shop 2, Main Road, Huonville TAS 7109', 'Retail', -43.0316, 147.0468),
                ('8 Circle Street, New Norfolk TAS 7140', 'Medical', -42.7826, 147.0571),
                ('Shop 1, 5 Emu Bay Road, Deloraine TAS 7304', 'Retail', -41.5286, 146.6559),
                ('Unit 3, George Town Road, Newnham TAS 7248', 'Retail', -41.4114, 147.1187),
                ('12 Cattley Street, Burnie TAS 7320', 'Medical', -41.0519, 145.9083),
                ('Shop 4, Bass Highway, Somerset TAS 7322', 'Retail', -41.0401, 145.8282),
                ('3 Main Road, Sheffield TAS 7306', 'Retail', -41.3989, 146.3393),
                ('Unit 5, Patrick Street, Hobart TAS 7000', 'Retail', -42.8838, 147.3280),
                ('Shop 7, 88 George Street, Launceston TAS 7250', 'Retail', -41.4389, 147.1395),
                ('2 Alexander Street, Sandy Bay TAS 7005', 'Medical', -42.8945, 147.3274),
                ('15 Bligh Street, Rosny Park TAS 7018', 'Retail', -42.8739, 147.3600),
                ('Shop 9, Stony Rise Road, Devonport TAS 7310', 'Retail', -41.1637, 146.3345),
            ],
            'NSW': [
                ('Shop 1, 200 George Street, Sydney NSW 2000', 'Retail', -33.8630, 151.2084),
                ('Suite 3, 100 Church Street, Parramatta NSW 2150', 'Medical', -33.8148, 151.0036),
            ],
            'VIC': [
                ('Shop 1, 300 Collins Street, Melbourne VIC 3000', 'Retail', -37.8162, 144.9640),
            ],
        }

        generated = 0
        locations = sample_locations.get(region, [])

        for addr, prop_type, lat, lon in locations[:count]:
            try:
                property_data = {
                    'address': addr,
                    'latitude': lat,
                    'longitude': lon,
                    'listing_url': None,
                    'property_type': prop_type,
                    'size_sqm': None,
                    'agent_name': None,
                    'agent_phone': None,
                    'agent_email': None,
                }
                self.db.insert_property(property_data)
                generated += 1
            except Exception:
                continue

        return generated

    def add_manual_property(self, address: str, listing_url: str = None,
                            property_type: str = None, size_sqm: float = None,
                            agent_name: str = None, agent_phone: str = None,
                            agent_email: str = None) -> bool:
        """Manually add a commercial property."""
        try:
            coords = self.geocoder.geocode(address)
            if not coords:
                print(f"Could not geocode: {address}")
                return False

            lat, lon = coords

            self.db.insert_property({
                'address': address,
                'latitude': lat,
                'longitude': lon,
                'listing_url': listing_url,
                'property_type': property_type,
                'size_sqm': size_sqm,
                'agent_name': agent_name,
                'agent_phone': agent_phone,
                'agent_email': agent_email,
            })
            print(f"  Added property: {address}")
            return True

        except Exception as e:
            print(f"Error adding property: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """Import properties from CSV."""
        import csv
        count = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    address = row.get('address', '')
                    if not address:
                        continue

                    lat = row.get('latitude')
                    lon = row.get('longitude')
                    if not lat or not lon:
                        coords = self.geocoder.geocode(address)
                        if coords:
                            lat, lon = coords
                        else:
                            continue
                    else:
                        lat, lon = float(lat), float(lon)

                    size_sqm = row.get('size_sqm')
                    if size_sqm:
                        try:
                            size_sqm = float(size_sqm)
                        except (ValueError, TypeError):
                            size_sqm = None

                    self.db.insert_property({
                        'address': address,
                        'latitude': lat,
                        'longitude': lon,
                        'listing_url': row.get('listing_url'),
                        'property_type': row.get('property_type'),
                        'size_sqm': size_sqm,
                        'agent_name': row.get('agent_name'),
                        'agent_phone': row.get('agent_phone'),
                        'agent_email': row.get('agent_email'),
                    })
                    count += 1

        except Exception as e:
            print(f"Error importing CSV: {e}")

        return count
