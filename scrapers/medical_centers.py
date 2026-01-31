"""
Medical Center Scanner

Finds medical centers across Australia and checks if they have on-site pharmacies.

Key insight from MJ: Medical centers without pharmacies = greenfield opportunities.
When combined with growth signals (new developments nearby), these become priority targets.
"""

import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List, Optional, Tuple
import json
import time
import re


@dataclass
class MedicalCenter:
    """Represents a medical center."""
    name: str
    address: str
    suburb: str
    state: str
    postcode: str
    phone: Optional[str]
    website: Optional[str]
    num_gps: int  # Estimated number of GPs
    has_specialists: bool
    specialist_types: List[str]
    has_allied_health: bool
    allied_health_types: List[str]
    has_pharmacy: bool  # KEY FIELD - if False, this is an opportunity
    pharmacy_name: Optional[str]
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    source: str = ""
    notes: str = ""
    opportunity_score: int = 0


class MedicalCenterScanner:
    """Scans for medical centers and checks pharmacy co-location."""
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.centers: List[MedicalCenter] = []
    
    def search_healthdirect(self, suburb: str, state: str) -> List[MedicalCenter]:
        """Search Healthdirect for medical centers in an area."""
        # Healthdirect national health services directory
        url = f"https://www.healthdirect.gov.au/australian-health-services/search/{suburb.lower().replace(' ', '-')}-{state.lower()}/gp-general-practice"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Parse results
            results = soup.find_all('div', class_=re.compile(r'result|service|listing'))
            
            centers = []
            for result in results:
                name_elem = result.find(['h2', 'h3', 'a'], class_=re.compile(r'name|title'))
                if not name_elem:
                    continue
                
                name = name_elem.get_text(strip=True)
                
                # Look for address
                addr_elem = result.find(['p', 'span', 'div'], class_=re.compile(r'address|location'))
                address = addr_elem.get_text(strip=True) if addr_elem else ""
                
                center = MedicalCenter(
                    name=name,
                    address=address,
                    suburb=suburb,
                    state=state,
                    postcode="",
                    phone=None,
                    website=None,
                    num_gps=0,
                    has_specialists=False,
                    specialist_types=[],
                    has_allied_health=False,
                    allied_health_types=[],
                    has_pharmacy=False,  # Will check separately
                    pharmacy_name=None,
                    source="healthdirect"
                )
                centers.append(center)
            
            return centers
            
        except Exception as e:
            print(f"Error searching Healthdirect: {e}")
            return []
    
    def search_hotdoc(self, suburb: str, state: str) -> List[MedicalCenter]:
        """Search HotDoc for medical centers."""
        url = f"https://www.hotdoc.com.au/search?filters=reason%3Agp-consultation&in={suburb}%2C+{state}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            if response.status_code != 200:
                return []
            
            # HotDoc uses client-side rendering, so we'd need Selenium
            # For now, return empty - could implement with Playwright later
            return []
            
        except Exception as e:
            print(f"Error searching HotDoc: {e}")
            return []
    
    def check_has_pharmacy(self, center: MedicalCenter) -> Tuple[bool, Optional[str]]:
        """
        Check if a medical center has an on-site pharmacy.
        
        Methods:
        1. Check the medical center's website for pharmacy mentions
        2. Search for pharmacies at the same address
        3. Check Google Maps for pharmacy within building
        """
        has_pharmacy = False
        pharmacy_name = None
        
        # Method 1: Check website if available
        if center.website:
            try:
                response = requests.get(center.website, headers=self.headers, timeout=10)
                if response.status_code == 200:
                    text = response.text.lower()
                    if 'pharmacy' in text or 'chemist' in text or 'dispensary' in text:
                        has_pharmacy = True
                        # Try to extract pharmacy name
                        match = re.search(r'(\w+\s+pharmacy|\w+\s+chemist)', text, re.IGNORECASE)
                        if match:
                            pharmacy_name = match.group(1).title()
            except:
                pass
        
        # Method 2: Search Healthdirect for pharmacy at same suburb
        # (simplified - would need proper address matching)
        
        return has_pharmacy, pharmacy_name
    
    def scan_area(self, suburb: str, state: str) -> List[MedicalCenter]:
        """Scan an area for medical centers and check for pharmacies."""
        print(f"Scanning {suburb}, {state} for medical centers...")
        
        # Collect from multiple sources
        centers = []
        
        # Healthdirect
        hd_centers = self.search_healthdirect(suburb, state)
        centers.extend(hd_centers)
        print(f"  Found {len(hd_centers)} from Healthdirect")
        
        # HotDoc
        hotdoc_centers = self.search_hotdoc(suburb, state)
        centers.extend(hotdoc_centers)
        print(f"  Found {len(hotdoc_centers)} from HotDoc")
        
        # Check each for pharmacy
        for center in centers:
            has_pharm, pharm_name = self.check_has_pharmacy(center)
            center.has_pharmacy = has_pharm
            center.pharmacy_name = pharm_name
        
        # Store in instance
        self.centers.extend(centers)
        
        return centers
    
    def get_opportunities(self) -> List[MedicalCenter]:
        """Get medical centers WITHOUT on-site pharmacies (opportunities)."""
        return [c for c in self.centers if not c.has_pharmacy]
    
    def score_opportunities(self, priority_areas: List[dict] = None):
        """
        Score opportunities based on:
        1. Number of GPs (more = more scripts)
        2. Has specialists (= more complex scripts)
        3. Has allied health (= more foot traffic)
        4. In a priority area (development signals)
        """
        priority_suburbs = set()
        if priority_areas:
            for area in priority_areas:
                priority_suburbs.add(f"{area['location']}, {area['state']}")
        
        for center in self.centers:
            if center.has_pharmacy:
                center.opportunity_score = 0
                continue
            
            score = 50  # Base score for any opportunity
            
            # GP count
            score += center.num_gps * 10  # 10 points per GP
            
            # Specialists
            if center.has_specialists:
                score += 30
                score += len(center.specialist_types) * 5
            
            # Allied health
            if center.has_allied_health:
                score += 15
            
            # Priority area (development signals)
            key = f"{center.suburb}, {center.state}"
            if key in priority_suburbs:
                score += 50  # Big bonus for growth areas
            
            center.opportunity_score = score
    
    def export_opportunities(self, filepath: str):
        """Export opportunities to JSON."""
        opps = self.get_opportunities()
        opps.sort(key=lambda x: x.opportunity_score, reverse=True)
        
        data = [
            {
                'name': c.name,
                'address': c.address,
                'suburb': c.suburb,
                'state': c.state,
                'num_gps': c.num_gps,
                'has_specialists': c.has_specialists,
                'specialist_types': c.specialist_types,
                'has_allied_health': c.has_allied_health,
                'phone': c.phone,
                'website': c.website,
                'opportunity_score': c.opportunity_score,
                'notes': c.notes,
            }
            for c in opps
        ]
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Exported {len(data)} opportunities to {filepath}")


# Example usage
if __name__ == '__main__':
    scanner = MedicalCenterScanner()
    
    # Scan a test area
    centers = scanner.scan_area("South Burnie", "TAS")
    
    # Score opportunities
    scanner.score_opportunities()
    
    # Get opportunities
    opps = scanner.get_opportunities()
    
    print(f"\nFound {len(opps)} medical centers without pharmacies:")
    for opp in opps[:5]:
        print(f"  [{opp.opportunity_score}] {opp.name}")
        print(f"      {opp.address}")
        print()
