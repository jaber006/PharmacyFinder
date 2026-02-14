"""
HotDoc Medical Centre Scraper

Scrapes medical centre data from HotDoc (hotdoc.com.au) to get
practitioner counts for Item 136 analysis.

Strategy: HotDoc is a fully client-rendered SPA, so we use their
internal API endpoints discovered via network inspection. The main
endpoint is their search API which returns clinic data with practitioner
counts.

Fallback: If the API is blocked, we scrape individual clinic profile
pages using requests + regex to extract structured data from the
initial HTML payload.
"""

import json
import time
import re
import os
import sys
import requests
from typing import List, Dict, Optional, Tuple
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.database import Database


# Australian cities/towns to search - covers major population centres
# We search by state to get broad coverage
SEARCH_LOCATIONS = {
    'TAS': [
        'Hobart', 'Launceston', 'Devonport', 'Burnie', 'Kingston',
        'Ulverstone', 'Wynyard', 'Smithton', 'Queenstown', 'New Norfolk',
        'Sorell', 'Bridgewater', 'Glenorchy', 'Clarence', 'Rosny Park',
        'Sandy Bay', 'Moonah', 'Mowbray', 'Ravenswood', 'George Town',
    ],
    'NSW': [
        'Sydney', 'Newcastle', 'Wollongong', 'Gosford', 'Parramatta',
        'Penrith', 'Campbelltown', 'Liverpool', 'Blacktown', 'Hornsby',
        'Chatswood', 'Bankstown', 'Hurstville', 'Cronulla', 'Manly',
        'Coffs Harbour', 'Tamworth', 'Orange', 'Dubbo', 'Wagga Wagga',
        'Albury', 'Bathurst', 'Lismore', 'Port Macquarie', 'Maitland',
        'Broken Hill', 'Nowra', 'Bowral', 'Camden', 'Richmond',
    ],
    'VIC': [
        'Melbourne', 'Geelong', 'Ballarat', 'Bendigo', 'Shepparton',
        'Mildura', 'Warrnambool', 'Traralgon', 'Wodonga', 'Frankston',
        'Dandenong', 'Ringwood', 'Box Hill', 'Heidelberg', 'Footscray',
        'Werribee', 'Sunbury', 'Melton', 'Craigieburn', 'Pakenham',
    ],
    'QLD': [
        'Brisbane', 'Gold Coast', 'Sunshine Coast', 'Townsville', 'Cairns',
        'Toowoomba', 'Mackay', 'Rockhampton', 'Bundaberg', 'Hervey Bay',
        'Gladstone', 'Ipswich', 'Logan', 'Redcliffe', 'Caboolture',
        'Nambour', 'Caloundra', 'Mount Isa', 'Emerald', 'Dalby',
    ],
    'WA': [
        'Perth', 'Mandurah', 'Bunbury', 'Geraldton', 'Kalgoorlie',
        'Albany', 'Karratha', 'Broome', 'Rockingham', 'Joondalup',
        'Armadale', 'Midland', 'Fremantle', 'Wanneroo', 'Stirling',
    ],
    'SA': [
        'Adelaide', 'Mount Gambier', 'Whyalla', 'Murray Bridge', 'Port Augusta',
        'Port Lincoln', 'Victor Harbor', 'Gawler', 'Mount Barker', 'Salisbury',
        'Elizabeth', 'Modbury', 'Noarlunga', 'Marion', 'Mitcham',
    ],
    'NT': [
        'Darwin', 'Alice Springs', 'Katherine', 'Palmerston', 'Casuarina',
    ],
    'ACT': [
        'Canberra', 'Belconnen', 'Woden', 'Tuggeranong', 'Gungahlin',
        'Weston Creek', 'Civic', 'Dickson', 'Kingston',
    ],
}


class HotDocScraper:
    """Scrape medical centre data from HotDoc."""

    SEARCH_URL = "https://www.hotdoc.com.au/api/patient/search"
    CLINIC_URL_PATTERN = "https://www.hotdoc.com.au/medical-centres/{slug}/doctors"
    
    # HotDoc's internal API base (discovered from network traffic)
    API_BASE = "https://api.hotdoc.com.au/api"

    def __init__(self, db: Database = None):
        self.db = db
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'en-AU,en;q=0.9',
        })
        self.centres: List[Dict] = []
        self.rate_limit = 2.0  # seconds between requests

    def search_clinics_by_location(self, location: str, state: str) -> List[Dict]:
        """
        Search HotDoc for GP clinics near a location.
        
        Since HotDoc's API is not directly accessible, we parse their
        sitemap and clinic directory pages to build a list.
        """
        clinics = []
        
        # Try the sitemap/directory approach
        slug_location = location.lower().replace(' ', '-')
        state_lower = state.lower()
        
        # HotDoc has SEO pages for locations
        urls_to_try = [
            f"https://www.hotdoc.com.au/find/doctor/{slug_location}-{state}-Australia",
            f"https://www.hotdoc.com.au/find/gp/{slug_location}-{state}-Australia",
            f"https://www.hotdoc.com.au/medical-centres/{slug_location}-{state}",
        ]
        
        for url in urls_to_try:
            try:
                resp = self.session.get(url, timeout=15)
                if resp.status_code == 200:
                    # Try to extract clinic data from the page
                    clinics.extend(self._parse_search_results(resp.text, state))
                    if clinics:
                        break
            except Exception as e:
                pass
            time.sleep(self.rate_limit)
        
        return clinics

    def _parse_search_results(self, html: str, state: str) -> List[Dict]:
        """Parse search results from HotDoc HTML."""
        clinics = []
        
        # Look for JSON data embedded in script tags
        json_patterns = [
            r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
            r'window\.__PRELOADED_STATE__\s*=\s*({.+?});',
            r'"clinics":\s*(\[.+?\])',
            r'"practices":\s*(\[.+?\])',
        ]
        
        for pattern in json_patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    if isinstance(data, list):
                        for item in data:
                            clinic = self._normalize_clinic(item, state)
                            if clinic:
                                clinics.append(clinic)
                    elif isinstance(data, dict):
                        # Navigate nested structure
                        for key in ['clinics', 'practices', 'results', 'data']:
                            if key in data and isinstance(data[key], list):
                                for item in data[key]:
                                    clinic = self._normalize_clinic(item, state)
                                    if clinic:
                                        clinics.append(clinic)
                except (json.JSONDecodeError, KeyError):
                    pass
        
        # Also look for clinic links in the HTML
        clinic_links = re.findall(
            r'href="(/medical-centres/[^"]+)"',
            html
        )
        for link in set(clinic_links):
            if '/doctors' not in link:
                link = link.rstrip('/') + '/doctors'
            clinics.append({
                'url': f"https://www.hotdoc.com.au{link}",
                'needs_scrape': True,
                'state': state,
            })
        
        return clinics

    def _normalize_clinic(self, data: dict, state: str) -> Optional[Dict]:
        """Normalize clinic data from various formats."""
        try:
            name = data.get('name') or data.get('clinic_name') or data.get('practice_name')
            if not name:
                return None
            
            address = data.get('address') or data.get('full_address') or ''
            lat = data.get('latitude') or data.get('lat')
            lng = data.get('longitude') or data.get('lng') or data.get('lon')
            
            # Get practitioner count
            num_practitioners = (
                data.get('practitioner_count') or
                data.get('doctor_count') or
                data.get('num_doctors') or
                len(data.get('practitioners', [])) or
                len(data.get('doctors', [])) or
                0
            )
            
            practitioners = data.get('practitioners') or data.get('doctors') or []
            practitioner_names = [
                p.get('name') or p.get('full_name') or f"Dr {p.get('first_name', '')} {p.get('last_name', '')}"
                for p in practitioners if isinstance(p, dict)
            ]
            
            return {
                'name': name,
                'address': address,
                'latitude': float(lat) if lat else None,
                'longitude': float(lng) if lng else None,
                'practitioner_count': int(num_practitioners),
                'practitioners': practitioner_names,
                'state': state,
                'source': 'hotdoc',
            }
        except (TypeError, ValueError):
            return None

    def scrape_clinic_page(self, url: str, state: str = '') -> Optional[Dict]:
        """
        Scrape an individual clinic page to get practitioner details.
        
        Since HotDoc is client-rendered, we look for:
        1. JSON-LD structured data
        2. Embedded state/config objects
        3. meta tags with practitioner info
        """
        try:
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200:
                return None
            
            html = resp.text
            clinic = {}
            
            # Try to find clinic name from URL
            url_match = re.search(r'/medical-centres/([^/]+)/([^/]+)', url)
            if url_match:
                location_slug = url_match.group(1)
                name_slug = url_match.group(2)
                clinic['name'] = name_slug.replace('-', ' ').title()
                
                # Parse location from slug
                loc_parts = location_slug.split('-')
                if len(loc_parts) >= 2:
                    clinic['state'] = loc_parts[-2].upper() if len(loc_parts[-2]) <= 3 else state
            
            # Look for structured data
            ld_json = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
            for ld in ld_json:
                try:
                    data = json.loads(ld)
                    if data.get('@type') in ['MedicalBusiness', 'MedicalClinic', 'Physician']:
                        clinic['name'] = data.get('name', clinic.get('name', ''))
                        if 'address' in data:
                            addr = data['address']
                            if isinstance(addr, dict):
                                clinic['address'] = f"{addr.get('streetAddress', '')}, {addr.get('addressLocality', '')}, {addr.get('addressRegion', '')}"
                            else:
                                clinic['address'] = str(addr)
                        if 'geo' in data:
                            clinic['latitude'] = data['geo'].get('latitude')
                            clinic['longitude'] = data['geo'].get('longitude')
                except json.JSONDecodeError:
                    pass
            
            # Try to count doctor/practitioner elements in the HTML
            doctor_count = len(re.findall(
                r'(?:class="[^"]*(?:doctor|practitioner|provider)[^"]*"|data-doctor|data-practitioner)',
                html,
                re.IGNORECASE
            ))
            
            # Also check for doctor name patterns
            doctor_names = re.findall(
                r'(?:Dr\.?\s+[A-Z][a-z]+ [A-Z][a-z]+|Doctor\s+[A-Z][a-z]+ [A-Z][a-z]+)',
                html
            )
            
            clinic['practitioner_count'] = max(doctor_count, len(set(doctor_names)))
            clinic['practitioners'] = list(set(doctor_names))
            clinic['source'] = 'hotdoc'
            clinic['state'] = clinic.get('state', state)
            
            return clinic if clinic.get('name') else None
            
        except Exception as e:
            print(f"  Error scraping {url}: {e}")
            return None

    def scrape_state(self, state: str, verbose: bool = True) -> List[Dict]:
        """Scrape all medical centres in a state."""
        locations = SEARCH_LOCATIONS.get(state, [])
        if verbose:
            print(f"\nScraping HotDoc for {state} ({len(locations)} locations)...")
        
        all_clinics = []
        seen_names = set()
        
        for location in locations:
            if verbose:
                print(f"  Searching {location}, {state}...")
            
            clinics = self.search_clinics_by_location(location, state)
            
            for clinic in clinics:
                name = clinic.get('name', '')
                if name and name not in seen_names:
                    seen_names.add(name)
                    all_clinics.append(clinic)
            
            if verbose and clinics:
                print(f"    Found {len(clinics)} clinics")
            
            time.sleep(self.rate_limit)
        
        # Scrape individual clinic pages that need it
        to_scrape = [c for c in all_clinics if c.get('needs_scrape')]
        if to_scrape and verbose:
            print(f"\n  Scraping {len(to_scrape)} individual clinic pages...")
        
        for i, clinic in enumerate(to_scrape):
            url = clinic.get('url', '')
            if url:
                detail = self.scrape_clinic_page(url, state)
                if detail:
                    # Replace the stub with real data
                    idx = all_clinics.index(clinic)
                    all_clinics[idx] = detail
                
                if verbose and (i + 1) % 10 == 0:
                    print(f"    Scraped {i+1}/{len(to_scrape)}")
                
                time.sleep(self.rate_limit)
        
        # Filter out stubs that didn't resolve
        all_clinics = [c for c in all_clinics if not c.get('needs_scrape')]
        
        if verbose:
            print(f"  Total unique clinics for {state}: {len(all_clinics)}")
        
        self.centres.extend(all_clinics)
        return all_clinics

    def save_to_db(self, db: Database = None):
        """Save scraped centres to the database."""
        db = db or self.db
        if not db:
            raise ValueError("No database provided")
        
        saved = 0
        for centre in self.centres:
            if not centre.get('latitude') or not centre.get('longitude'):
                continue
            if not centre.get('name'):
                continue
            
            data = {
                'name': centre['name'],
                'address': centre.get('address', ''),
                'latitude': centre['latitude'],
                'longitude': centre['longitude'],
                'num_gps': centre.get('practitioner_count', 0),
                'total_fte': centre.get('practitioner_count', 0) * 0.8,  # Estimate: 80% FTE average
                'source': 'hotdoc',
            }
            db.insert_medical_centre(data)
            saved += 1
        
        print(f"Saved {saved} medical centres from HotDoc to database")
        return saved


def scrape_hotdoc(states: List[str] = None, db_path: str = None):
    """Main entry point for HotDoc scraping."""
    if states is None:
        states = ['TAS']  # Default to TAS for testing
    
    db = Database(db_path or 'pharmacy_finder.db')
    db.connect()
    
    scraper = HotDocScraper(db)
    
    for state in states:
        scraper.scrape_state(state)
    
    scraper.save_to_db()
    
    stats = db.get_reference_data_stats()
    print(f"\nDatabase now has {stats.get('medical_centres', 0)} medical centres")
    
    db.close()
    return scraper.centres


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Scrape HotDoc for medical centre data')
    parser.add_argument('--states', nargs='+', default=['TAS'], help='States to scrape')
    parser.add_argument('--db', default='pharmacy_finder.db', help='Database path')
    args = parser.parse_args()
    
    scrape_hotdoc(args.states, args.db)
